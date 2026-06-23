# Commit-ordered sequence (`commit_seq`) — SB-2140

## The bug

The consumer cursor was a scalar high-water mark over `sequence`. `sequence` is a
`BIGSERIAL` assigned by `nextval` at **INSERT**, inside the producer's
transaction — but rows only become visible at **COMMIT**. Under concurrency,
assignment order ≠ commit order:

```
Partition p, consumer offset at N-1:
1. Txn A (long, e.g. bulk import) INSERTs → sequence = N,   stays open.
2. Txn B (short)                  INSERTs → sequence = N+1, COMMITs.
3. Poller runs between B's and A's commit → sees only N+1 (sequence > N-1),
   processes it, advances offset to N+1.
4. Txn A COMMITs. N is now visible, but N > N+1 is false → never fetched.
   Silent loss — no error, no DLQ, no log.
```

This affected **every** consumer group. Confirmed in production twice (OrderItem
619817 on 2026-06-11; 459 `dim_products` SKUs on 2026-06-19).

## The fix

A second ordering token, `commit_seq`, assigned at the **COMMIT edge** (not at
INSERT), per `(topic, partition_key)`:

- `outbox_relay_partition_seq(topic, partition_id, next_seq)` — a transactional
  per-partition sequencer.
- A `DEFERRABLE INITIALLY DEFERRED`, statement-level, `AFTER INSERT` constraint
  trigger (`outbox_relay_commit_seq_trg`) using a `REFERENCING NEW TABLE`
  transition table. At commit it locks the partition's sequencer row, bumps it by
  the row count, and assigns `commit_seq` to the statement's rows in `sequence`
  order.

The guarantee: a PostgreSQL row lock is released only at commit, so the partition
sequencer serializes the **commit edge**. Therefore, per partition:

```
commit_seq assignment order == commit order == visibility order
```

A consumer reading `WHERE topic=? AND partition_key=? AND commit_seq > offset
ORDER BY commit_seq` can never see `commit_seq = K` without every `commit_seq < K`
in that partition already being visible. No skip.

`sequence` is kept unchanged — still unique, still used for the advisory-lock key
and as the intra-transaction tiebreaker (`row_number() OVER (ORDER BY sequence)`).

### Why DEFERRED matters

The sequencer lock is taken inside the trigger at the commit edge, **not** when
`publish()` runs. A 10-minute bulk-import transaction holds the lock for
milliseconds at its very end, not for the whole import. This is what makes the
fix indifferent to transaction size and avoids serializing a topic's throughput.

### Why per-partition (not a single global sequencer)

The serialization is per partition, so unrelated partitions commit concurrently.
Multi-partition transactions (e.g. the ChangeRelay drainer's 500-event batch)
acquire sequencer rows in a canonical order (`ORDER BY topic COLLATE "C",
partition_id`) so they can never deadlock.

### Why not a `NOT NULL` column for hardening

`commit_seq` is NULL during INSERT and only set by the deferred trigger at commit.
A column `NOT NULL` constraint is checked at insert time and would reject every
row. The fail-loud safety net (`harden_commit_seq`) is instead a second deferred
assert trigger that raises at commit if any row was left unassigned.

## Cutover: no offset-row migration

Existing offsets are global `sequence` values stored in
`consumer_offsets.last_consumed_sequence`. The Stage 1 migration backfills
historical rows with `commit_seq = sequence` and seeds every partition's sequencer
from the **global** sequence high-water (`last_value`). So every post-cutover
`commit_seq` exceeds every existing offset, and any pre-cutover offset value
remains a valid `commit_seq` threshold. The consumer flip (Stage 2) reuses the
same `last_consumed_sequence` column to store `commit_seq` — no data migration.

## Staged rollout

1. **Stage 1 — `rails generate outbox_relay:add_commit_seq`.** DB-only. Consumers
   still read `sequence`. Verify in prod: `SELECT count(*) FROM
   outbox_relay_outbox_events WHERE commit_seq IS NULL` is ~0 for committed rows.
2. **Stage 2 — gem release that flips consumers to `commit_seq`** (fetch, offset
   guard, `update_offset!`, `:latest` seed, all lag queries, cleanup gate).
3. **Stage 3 — `rails generate outbox_relay:harden_commit_seq`** once verified.

## Caveats

- **`session_replication_role = 'replica'`** (logical replication / DMS /
  blue-green apply) does **not** fire triggers. An app-written outbox is
  unaffected; any non-app insert path would leave `commit_seq` NULL.
- A bug in the assignment trigger fails publishes at COMMIT (fail-closed;
  at-least-once preserved). Keep the function trivial; verify on staging first.
- No cross-partition ordering guarantee — correct for a partitioned log. Do not
  build a cross-partition consumer on `commit_seq`.

## Manual verification (Postgres/Aurora — not in CI)

CI runs on in-memory SQLite (no triggers); it covers the Ruby layer with a
simulated `commit_seq`. Verify the trigger itself against a real instance:

1. **Lost-event race.** Open Txn A: `INSERT` one event (low `sequence`), keep
   open. Open Txn B: `INSERT` one event same partition (higher `sequence`),
   `COMMIT`. Poll as a consumer (advance offset past B). Now `COMMIT` A. Confirm A
   is still fetched (its `commit_seq` > B's). Pre-fix, A was skipped.
2. **Multi-partition batch.** `INSERT` 500 rows across all partitions in one
   statement; confirm correct per-partition `commit_seq` and no deadlock when two
   such transactions commit concurrently.
3. **Long transaction.** Confirm a long-open transaction does not hold the
   sequencer row lock until its commit edge (`pg_locks` inspection).
4. **`REFERENCING NEW TABLE` on a `CONSTRAINT TRIGGER`** is accepted on the target
   Aurora PG version. Fallback: a per-row deferred trigger filtered by
   `commit_seq IS NULL AND xmin = pg_current_xact_id()::xid`.
