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
- A `DEFERRABLE INITIALLY DEFERRED`, **per-row**, `AFTER INSERT` constraint trigger
  (`outbox_relay_commit_seq_trg`). At the commit edge each row takes a per-topic
  advisory lock, bumps its partition's sequencer row, and writes `commit_seq` via
  `UPDATE ... WHERE id = NEW.id`.

The guarantee: the per-topic lock is released only at commit, so it serializes the
**commit edge** per topic. Therefore, per `(topic, partition)`:

```
commit_seq assignment order == commit order == visibility order
```

A consumer reading `WHERE topic=? AND partition_key=? AND commit_seq > offset
ORDER BY commit_seq` can never see `commit_seq = K` without every `commit_seq < K`
in that partition already being visible. No skip.

`sequence` is kept unchanged — still unique, still used for the advisory-lock key
and as the intra-transaction tiebreaker (rows fire in insertion = `sequence` order).

### Why per-row, not statement-level with a transition table

The deferral is essential: it moves the lock to the COMMIT edge so a long
bulk-import transaction holds it for milliseconds at the very end, not for the
whole transaction. But **only a `CONSTRAINT TRIGGER` can be `DEFERRABLE`**, and a
constraint trigger **must be `FOR EACH ROW`** and **may not use `REFERENCING ...
TABLE`** (transition tables). PostgreSQL rejects the statement-level form with
`syntax error at or near "REFERENCING"`. So the trigger is per-row, operating on
`NEW` (an `AFTER` trigger can't mutate `NEW` in place, hence the
`UPDATE ... WHERE id = NEW.id`).

Trade-off vs a hypothetical statement-level trigger: a multi-row insert fires the
trigger N times instead of once. Correctness over micro-perf; the per-row work is
a tiny indexed upsert + one-row update.

### Deadlock-freedom: the per-topic advisory lock

A bare per-row trigger that only locked its own partition's sequencer row would
**deadlock**: deferred triggers fire at commit in row-insertion order, each holds
its partition row lock until commit, and two concurrent multi-partition
transactions on the same topic (exactly the ChangeRelay drainer workload) acquire
those locks in different orders. To prevent this, each row first takes a per-topic
advisory **transaction** lock:

```sql
PERFORM pg_advisory_xact_lock(hashtext('outbox_relay_commit_seq'), hashtext(NEW.topic));
```

All of a transaction's rows for one topic share this single re-entrant lock, so
commit-edge assignment serializes **per topic** (deadlock-free), while different
topics proceed in parallel. The two-int advisory form is a distinct lock space
from the consumer's one-arg bigint `pg_try_advisory_lock`, so they never collide.

Residual caveat: a single transaction that publishes to **multiple topics** in an
order that differs across concurrent transactions could still deadlock on the
topic advisory locks (PostgreSQL detects it and aborts one; the producer retries).
Not expected for the per-topic drainer workload; revisit (canonical topic-lock
ordering) only if it shows up.

### Why not a `NOT NULL` column for hardening

`commit_seq` is NULL during INSERT and only set by the deferred trigger at commit.
A column `NOT NULL` constraint is checked at insert time and would reject every
row. The fail-loud safety net (`harden_commit_seq`) is instead a second per-row
deferred assert trigger (named to fire after the assignment trigger) that
re-reads the row and raises at commit if `commit_seq` was left unassigned.

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

1. **Stage 1 — `rails generate outbox_relay:add_commit_seq` + `rake
   outbox_relay:backfill_commit_seq`.** The migration is schema-only (column,
   sequencer, trigger, `CONCURRENTLY` index — no row rewrite); run it in a quiet
   window or with a short `lock_timeout` since `CREATE TRIGGER` briefly locks the
   table. Then run the backfill task (batched by PK, idempotent, resumable,
   throttleable via `[batch_size,sleep_ms]`) to set `commit_seq = sequence` on
   historical rows. Consumers still read `sequence`. Verify before Stage 2:
   `SELECT count(*) FROM outbox_relay_outbox_events WHERE commit_seq IS NULL` is 0
   for committed rows.
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

## Verification

The Ruby consumer layer is covered on SQLite with a simulated `commit_seq`. The
trigger itself is PostgreSQL-only, so it is covered by a **Postgres integration
spec** (`spec/pg/commit_seq_trigger_spec.rb`) that runs the real generated DDL and
asserts assignment + commit-edge ordering + deadlock-freedom. CI runs it against a
Postgres service (`spec_pg` job); locally it runs when `OUTBOX_PG_URL` is set and
is skipped otherwise. This closes the gap that let the original
`CONSTRAINT TRIGGER ... REFERENCING ... FOR EACH STATEMENT` syntax error reach a
release.

What the PG spec checks:

1. **DDL applies** — the generated function + `CONSTRAINT TRIGGER ... FOR EACH ROW`
   create cleanly (no syntax error).
2. **Assignment** — committed rows get monotonic per-partition `commit_seq`.
3. **Lost-event race** — Txn A inserts (low `sequence`) and stays open; Txn B
   inserts the same partition (higher `sequence`) and commits; A then commits. A
   gets the higher `commit_seq` (commit order), and `WHERE commit_seq > offset`
   never skips it.
4. **Deadlock-freedom** — concurrent multi-partition transactions on one topic
   commit without deadlocking.

On real Aurora, additionally sanity-check with `pg_locks` that a long-open
transaction does not hold its locks until the commit edge.
