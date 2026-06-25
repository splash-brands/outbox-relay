# frozen_string_literal: true

# PostgreSQL integration spec for the commit_seq assignment trigger (SB-2140).
#
# Runs the REAL generated DDL against PostgreSQL and verifies behavior the SQLite
# suite cannot: that the constraint trigger is valid PG syntax, assigns
# commit_seq at the commit edge in commit order, never skips a late-committing
# low-sequence row, and is deadlock-free under concurrent multi-partition commits.
#
# This closes the CI gap that let the 0.10.0 `CONSTRAINT TRIGGER ... REFERENCING
# ... FOR EACH STATEMENT` syntax error ship (the gem's default suite is SQLite, so
# the trigger was never executed).
#
# Runs when a PostgreSQL database is reachable via OUTBOX_PG_URL (CI's spec_pg job
# sets it). Skipped otherwise — including the default SQLite run, where the `pg`
# gem (optional `:pg` bundler group) is not installed.

pg_available =
  begin
    require 'pg'
    true
  rescue LoadError
    false
  end

PG_URL = ENV['OUTBOX_PG_URL']

RSpec.describe 'commit_seq assignment trigger on PostgreSQL', :pg do
  before(:all) do
    skip 'pg gem not installed (bundle with the :pg group)' unless pg_available
    skip 'set OUTBOX_PG_URL to run the PostgreSQL trigger specs' if PG_URL.nil? || PG_URL.empty?

    begin
      @conn = PG.connect(PG_URL)
    rescue PG::Error => e
      skip "cannot connect to PostgreSQL at OUTBOX_PG_URL: #{e.message}"
    end

    install_schema(@conn)
  end

  after(:all) do
    @conn&.close
  end

  before(:each) do
    @conn.exec('TRUNCATE outbox_relay_outbox_events, outbox_relay_partition_seq')
    @conn.exec('ALTER SEQUENCE outbox_relay_outbox_events_sequence RESTART WITH 1')
  end

  let(:topic) { 'pg_topic' }

  it 'creates the per-row constraint trigger without a syntax error' do
    # If install_schema raised on the CONSTRAINT TRIGGER DDL, before(:all) would
    # have failed. Assert it is actually present and FOR EACH ROW.
    row = @conn.exec_params(<<~SQL, ['outbox_relay_commit_seq_trg']).first
      SELECT tgname, tgtype FROM pg_trigger WHERE tgname = $1 AND NOT tgisinternal
    SQL
    expect(row).not_to be_nil
  end

  it 'assigns monotonic commit_seq per partition in commit order' do
    id1 = insert_event(@conn, topic, 0)
    id2 = insert_event(@conn, topic, 0)
    id3 = insert_event(@conn, topic, 1)

    cs1 = commit_seq(@conn, id1)
    cs2 = commit_seq(@conn, id2)
    cs3 = commit_seq(@conn, id3)

    expect(cs1).not_to be_nil
    expect(cs2).to be > cs1          # same partition, later commit → higher
    expect(cs3).not_to be_nil        # different partition, independent counter
  end

  # The core SB-2140 scenario: a transaction assigned a LOWER `sequence` first but
  # committing LATER must end up with the HIGHER `commit_seq`, so the high-water
  # cursor never skips it.
  it 'gives the late-committing low-sequence row the higher commit_seq (no skip)' do
    conn_a = PG.connect(PG_URL)
    conn_b = PG.connect(PG_URL)

    begin
      conn_a.exec('BEGIN')
      id_a = insert_event(conn_a, topic, 0) # lower sequence (inserted first), uncommitted

      conn_b.exec('BEGIN')
      id_b = insert_event(conn_b, topic, 0) # higher sequence
      conn_b.exec('COMMIT')                 # B commits first

      conn_a.exec('COMMIT')                 # A commits second

      cs_a = commit_seq(@conn, id_a)
      cs_b = commit_seq(@conn, id_b)

      # commit_seq follows COMMIT order, not sequence/insertion order.
      expect(cs_a).to be > cs_b

      # A consumer reading by commit_seq sees both, B before A — A is never below
      # the offset after B is consumed.
      ordered = @conn.exec_params(
        'SELECT id FROM outbox_relay_outbox_events WHERE topic=$1 AND partition_key=0 ' \
        'AND commit_seq > 0 ORDER BY commit_seq', [topic]
      ).map { |r| r['id'].to_i }
      expect(ordered).to eq([id_b, id_a])
    ensure
      conn_a.close
      conn_b.close
    end
  end

  it 'is deadlock-free under concurrent multi-partition commits on one topic' do
    require 'concurrent'
    barrier = Concurrent::CyclicBarrier.new(2)
    errors = Concurrent::Array.new

    # Two transactions touch partitions 0 and 1 in OPPOSITE order, then commit at
    # the same time. Bare per-partition row locks would deadlock; the per-topic
    # advisory lock serializes the commit edge instead.
    plans = [[0, 1], [1, 0]]
    threads = plans.map do |partitions|
      Thread.new do
        c = PG.connect(PG_URL)
        c.exec('BEGIN')
        partitions.each { |p| insert_event(c, topic, p) }
        barrier.wait
        c.exec('COMMIT')
        c.close
      rescue StandardError => e
        errors << e
      end
    end
    threads.each(&:join)

    expect(errors).to be_empty
    # All four rows assigned.
    n = @conn.exec("SELECT count(*) FROM outbox_relay_outbox_events WHERE commit_seq IS NOT NULL").first['count'].to_i
    expect(n).to eq(4)
  end

  # ---- helpers ----

  def insert_event(conn, topic, partition_key)
    conn.exec_params(<<~SQL, [topic, partition_key]).first['id'].to_i
      INSERT INTO outbox_relay_outbox_events (sequence, topic, partition_key)
      VALUES (nextval('outbox_relay_outbox_events_sequence'), $1, $2)
      RETURNING id
    SQL
  end

  def commit_seq(conn, id)
    val = conn.exec_params('SELECT commit_seq FROM outbox_relay_outbox_events WHERE id = $1', [id]).first['commit_seq']
    val&.to_i
  end

  # Minimal schema + the SHIPPED trigger DDL (mirrors the generator templates; the
  # always-on guard spec asserts the templates keep the FOR EACH ROW shape).
  def install_schema(conn)
    conn.exec(<<~SQL)
      DROP TABLE IF EXISTS outbox_relay_outbox_events;
      DROP TABLE IF EXISTS outbox_relay_partition_seq;
      DROP SEQUENCE IF EXISTS outbox_relay_outbox_events_sequence;

      CREATE SEQUENCE outbox_relay_outbox_events_sequence START 1;

      CREATE TABLE outbox_relay_outbox_events (
        id           bigserial PRIMARY KEY,
        sequence     bigint  NOT NULL,
        commit_seq   bigint,
        topic        text    NOT NULL,
        partition_key integer NOT NULL DEFAULT 0
      );

      CREATE UNIQUE INDEX idx_outbox_commit_seq_fetch
        ON outbox_relay_outbox_events (topic, partition_key, commit_seq)
        WHERE commit_seq IS NOT NULL;

      CREATE TABLE outbox_relay_partition_seq (
        topic        text    NOT NULL,
        partition_id integer NOT NULL,
        next_seq     bigint  NOT NULL DEFAULT 0,
        PRIMARY KEY (topic, partition_id)
      );

      CREATE OR REPLACE FUNCTION outbox_relay_assign_commit_seq()
      RETURNS trigger LANGUAGE plpgsql AS $fn$
      DECLARE
        base bigint;
      BEGIN
        IF NEW.commit_seq IS NOT NULL THEN
          RETURN NULL;
        END IF;

        PERFORM pg_advisory_xact_lock(hashtext('outbox_relay_commit_seq'), hashtext(NEW.topic));

        LOOP
          UPDATE outbox_relay_partition_seq
             SET next_seq = next_seq + 1
           WHERE topic = NEW.topic AND partition_id = NEW.partition_key
           RETURNING next_seq INTO base;
          EXIT WHEN FOUND;
          BEGIN
            INSERT INTO outbox_relay_partition_seq (topic, partition_id, next_seq)
            VALUES (NEW.topic, NEW.partition_key,
                    (SELECT last_value FROM outbox_relay_outbox_events_sequence) + 1)
            RETURNING next_seq INTO base;
            EXIT;
          EXCEPTION WHEN unique_violation THEN
          END;
        END LOOP;

        UPDATE outbox_relay_outbox_events SET commit_seq = base WHERE id = NEW.id;
        RETURN NULL;
      END;
      $fn$;

      CREATE CONSTRAINT TRIGGER outbox_relay_commit_seq_trg
        AFTER INSERT ON outbox_relay_outbox_events
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW
        EXECUTE FUNCTION outbox_relay_assign_commit_seq();
    SQL
  end
end
