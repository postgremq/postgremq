import os
import sys
from pathlib import Path
import pytest
import psycopg2
import psycopg2.extras
from datetime import datetime, timedelta
from typing import Generator, Any
import uuid
import pytz
from testcontainers.postgres import PostgresContainer
import time
from datetime import timezone
import json

# Get the path to latest.sql relative to this test file
SQL_FILE = Path(__file__).parent.parent / 'sql' / 'latest.sql'

@pytest.fixture(scope="session")
def postgres_container() -> Generator[PostgresContainer, None, None]:
    """Create a PostgreSQL container for testing."""
    postgres = PostgresContainer(
        "postgres:15"
    )
    
    postgres.start()
    yield postgres
    postgres.stop()

@pytest.fixture(scope="session")
def db_config(postgres_container: PostgresContainer) -> dict[str, str]:
    """Get database configuration from the test container."""
    return {
        'dbname': postgres_container.dbname,
        'user': postgres_container.username,
        'password': postgres_container.password,
        'host': postgres_container.get_container_host_ip(),
        'port': postgres_container.get_exposed_port(5432),
    }

@pytest.fixture(scope="session")
def test_db_name() -> str:
    """Generate a unique test database name."""
    return f"mq_test_{uuid.uuid4().hex[:8]}"

@pytest.fixture(scope="session")
def admin_conn(db_config: dict[str, str]) -> Generator[psycopg2.extensions.connection, None, None]:
    """Create a connection with admin privileges for database creation/deletion."""
    conn = psycopg2.connect(**db_config)
    conn.autocommit = True
    yield conn
    conn.close()

@pytest.fixture(scope="session")
def test_db(admin_conn: psycopg2.extensions.connection, test_db_name: str) -> Generator[str, None, None]:
    """Create and drop the test database."""
    # Drop database if it exists
    cur = admin_conn.cursor()
    try:
        cur.execute(f"DROP DATABASE IF EXISTS {test_db_name} WITH (FORCE)")
        
        # Create fresh database
        cur.execute(f"CREATE DATABASE {test_db_name}")
        cur.close()
        
        yield test_db_name
        
    finally:
        # Cleanup - will run even if tests fail
        cur = admin_conn.cursor()
        cur.execute(f"DROP DATABASE IF EXISTS {test_db_name} WITH (FORCE)")
        cur.close()

@pytest.fixture(scope="function")
def conn(test_db: str, db_config: dict[str, str]) -> Generator[psycopg2.extensions.connection, None, None]:
    """Create a connection to the test database for each test."""
    config = db_config.copy()
    config['dbname'] = test_db
    conn = psycopg2.connect(**config)
    conn.autocommit = True
    
    # Initialize schema and functions
    with conn.cursor() as cur:
        # First drop existing objects if they exist
        cur.execute("""
            DROP SCHEMA IF EXISTS postgremq CASCADE;
        """)
        
        # Load MQ implementation
        with open(SQL_FILE, 'r') as f:
            cur.execute(f.read())
    
    yield conn
    conn.close()

@pytest.fixture(scope="function")
def cur(conn: psycopg2.extensions.connection) -> Generator[psycopg2.extensions.cursor, None, None]:
    """Create a cursor for each test."""
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    yield cur
    cur.close()

def test_topic_and_queue_creation(cur: psycopg2.extensions.cursor) -> None:
    """Test basic topic and queue creation functionality."""
    cur.execute("""
        SELECT postgremq.create_topic('TestTopic');
        SELECT postgremq.create_queue('TestQueue', 'TestTopic', 3, false);
        SELECT postgremq.create_queue('TestQueue_Ex', 'TestTopic', 2, true, interval '300 seconds');
    """)
    
    # Verify queues were created
    cur.execute("SELECT count(*) FROM postgremq.queues WHERE topic_name = 'TestTopic'")
    assert cur.fetchone()[0] == 2

    # Verify keep-alive for exclusive queue
    cur.execute("""
        SELECT keep_alive_until 
        FROM postgremq.queues
        WHERE name = 'TestQueue_Ex'
    """)
    keep_alive = cur.fetchone()[0]
    now = datetime.now(pytz.UTC)
    assert keep_alive > now + timedelta(minutes=4)
    assert keep_alive < now + timedelta(minutes=6)

    cur.execute("""
        SELECT * FROM postgremq.consume_message('TestQueue', 30)
    """)

def test_queue_messages_status_check_constraint(cur: psycopg2.extensions.cursor) -> None:
    """A typo'd status must be rejected by the CHECK constraint rather than
    silently stranding the row in a state no query matches."""
    cur.execute("SELECT postgremq.create_topic('TestTopic')")
    cur.execute("SELECT postgremq.create_queue('Q', 'TestTopic', 0, false)")
    cur.execute("SELECT postgremq.publish_message('TestTopic', '{}'::jsonb)")
    with pytest.raises(psycopg2.errors.CheckViolation):
        cur.execute("UPDATE postgremq.queue_messages SET status = 'Processing'")  # capital P typo

def test_fk_columns_not_null(cur: psycopg2.extensions.cursor) -> None:
    """queues.topic_name and messages.topic_name are NOT NULL (defense against
    direct-table writes that bypass the create_* functions)."""
    cur.execute("""
        SELECT is_nullable FROM information_schema.columns
        WHERE table_schema = 'postgremq' AND table_name = 'queues' AND column_name = 'topic_name'
    """)
    assert cur.fetchone()[0] == 'NO'
    cur.execute("""
        SELECT is_nullable FROM information_schema.columns
        WHERE table_schema = 'postgremq' AND table_name = 'messages' AND column_name = 'topic_name'
    """)
    assert cur.fetchone()[0] == 'NO'

def test_consumer_token_is_uuid(cur: psycopg2.extensions.cursor) -> None:
    """consume_message now mints consumer tokens with gen_random_uuid(); the
    token should parse as a UUID (no more timestamp+random()+txid hack)."""
    cur.execute("SELECT postgremq.create_topic('TestTopic')")
    cur.execute("SELECT postgremq.create_queue('Q', 'TestTopic', 0, false)")
    cur.execute("SELECT postgremq.publish_message('TestTopic', '{}'::jsonb)")
    cur.execute("SELECT consumer_token FROM postgremq.consume_message('Q', 30, 1)")
    token = cur.fetchone()[0]
    # Raises ValueError if not a valid UUID.
    uuid.UUID(token)

def test_consume_missing_queue_raises_pmq02(cur: psycopg2.extensions.cursor) -> None:
    """Consuming a queue that does not exist (deleted out-of-band, or never
    created) raises PMQ02 so the client can tear the consumer down — rather than
    silently returning zero rows forever. An existing but empty queue must still
    return zero rows with no error (the common idle case)."""
    # Missing queue → PMQ02.
    with pytest.raises(psycopg2.Error) as exc_info:
        cur.execute("SELECT * FROM postgremq.consume_message('NoSuchQueue', 30, 1)")
    assert exc_info.value.pgcode == 'PMQ02', \
        f"expected PMQ02, got {exc_info.value.pgcode}: {exc_info.value}"
    cur.connection.rollback()

    # Existing but empty queue → zero rows, no error.
    cur.execute("SELECT postgremq.create_topic('EmptyTopic')")
    cur.execute("SELECT postgremq.create_queue('EmptyQueue', 'EmptyTopic', 0, false)")
    cur.execute("SELECT * FROM postgremq.consume_message('EmptyQueue', 30, 1)")
    assert cur.fetchall() == []

def test_consume_skips_redundant_keep_alive_write(cur: psycopg2.extensions.cursor) -> None:
    """Back-to-back consumes on an exclusive queue should NOT rewrite
    keep_alive_until every time — only once the deadline drifts past the
    half-interval point. Avoids dead-tuple churn on the hot queues row."""
    cur.execute("SELECT postgremq.create_topic('TestTopic')")
    # Long interval so a second consume stays well inside the half-interval window.
    cur.execute("SELECT postgremq.create_queue('ExQ', 'TestTopic', 0, true, interval '1 hour')")
    cur.execute("SELECT postgremq.publish_message('TestTopic', '{}'::jsonb)")
    cur.execute("SELECT postgremq.publish_message('TestTopic', '{}'::jsonb)")

    cur.execute("SELECT postgremq.consume_message('ExQ', 30, 1)")
    cur.execute("SELECT keep_alive_until FROM postgremq.queues WHERE name = 'ExQ'")
    first = cur.fetchone()[0]

    cur.execute("SELECT postgremq.consume_message('ExQ', 30, 1)")
    cur.execute("SELECT keep_alive_until FROM postgremq.queues WHERE name = 'ExQ'")
    second = cur.fetchone()[0]

    assert first == second, "second consume should not have rewritten keep_alive_until"

def test_maintenance_reaps_expired_exclusive_queue_no_grace(cur: psycopg2.extensions.cursor) -> None:
    """pmq_maintenance_fast reaps any exclusive queue past its keep_alive_until —
    there is NO grace window (the reaper uses keep_alive_until > NOW()). A queue
    whose deadline is still in the future survives. (Commit 3b1788d removed the
    former 5s grace; clients must send keep-alive before expiry.)"""
    cur.execute("SELECT postgremq.create_topic('TestTopic')")
    cur.execute("SELECT postgremq.create_queue('JustExpired', 'TestTopic', 0, true, interval '60 seconds')")
    cur.execute("SELECT postgremq.create_queue('StillAlive', 'TestTopic', 0, true, interval '60 seconds')")

    # JustExpired: 2s past deadline (no grace → reaped). StillAlive: 60s out.
    cur.execute("UPDATE postgremq.queues SET keep_alive_until = NOW() - INTERVAL '2 seconds' WHERE name = 'JustExpired'")
    cur.execute("UPDATE postgremq.queues SET keep_alive_until = NOW() + INTERVAL '60 seconds' WHERE name = 'StillAlive'")

    cur.execute("SELECT postgremq.pmq_maintenance_fast()")

    cur.execute("SELECT count(*) FROM postgremq.queues WHERE name = 'JustExpired'")
    assert cur.fetchone()[0] == 0, "a queue past keep_alive_until must be reaped (no grace)"
    cur.execute("SELECT count(*) FROM postgremq.queues WHERE name = 'StillAlive'")
    assert cur.fetchone()[0] == 1, "a queue whose deadline is still in the future must survive"

def test_create_queue_idempotent_and_strict(cur: psycopg2.extensions.cursor) -> None:
    """create_queue follows RabbitMQ-style 'match-or-error' semantics:
    re-creating with identical parameters is a no-op success; re-creating
    with any parameter different raises PMQ03 so accidental config drift
    surfaces loudly."""
    cur.execute("SELECT postgremq.create_topic('TestTopic')")
    cur.execute("SELECT postgremq.create_topic('OtherTopic')")

    # 1. Identical exclusive params → both calls succeed (no error).
    cur.execute("SELECT postgremq.create_queue('ExQueue', 'TestTopic', 2, true, interval '300 seconds')")
    cur.execute("SELECT postgremq.create_queue('ExQueue', 'TestTopic', 2, true, interval '300 seconds')")

    # 2. Identical non-exclusive params → idempotent.
    cur.execute("SELECT postgremq.create_queue('NxQueue', 'TestTopic', 2, false)")
    cur.execute("SELECT postgremq.create_queue('NxQueue', 'TestTopic', 2, false)")

    # 3. Param mismatches each raise PMQ03 with the existing values surfaced.
    mismatches = [
        # Different topic
        "SELECT postgremq.create_queue('ExQueue', 'OtherTopic', 2, true, interval '300 seconds')",
        # Different max_delivery_attempts
        "SELECT postgremq.create_queue('ExQueue', 'TestTopic', 5, true, interval '300 seconds')",
        # Different exclusive flag
        "SELECT postgremq.create_queue('ExQueue', 'TestTopic', 2, false, interval '300 seconds')",
        # Different keep_alive_interval
        "SELECT postgremq.create_queue('ExQueue', 'TestTopic', 2, true, interval '60 seconds')",
    ]
    for stmt in mismatches:
        with pytest.raises(psycopg2.Error) as exc_info:
            cur.execute(stmt)
        assert exc_info.value.pgcode == 'PMQ03', \
            f"expected PMQ03 for {stmt!r}, got {exc_info.value.pgcode}: {exc_info.value}"
        assert "already exists with different parameters" in str(exc_info.value)
        cur.connection.rollback()

def test_expired_queue_cannot_be_revived(cur):
    cur.execute("SELECT postgremq.create_topic('TestTopic')")
    cur.execute("SELECT postgremq.create_queue('Revive', 'TestTopic', 0, true, interval '60 seconds')")
    cur.execute("UPDATE postgremq.queues SET keep_alive_until = clock_timestamp() - interval '1 hour' WHERE name = 'Revive'")
    for statement in ["SELECT postgremq.create_queue('Revive', 'TestTopic', 0, true, interval '60 seconds')", "SELECT postgremq.consume_message('Revive', 30, 1)"]:
        with pytest.raises(psycopg2.Error) as error:
            cur.execute(statement)
        assert error.value.pgcode == 'PMQ02'
    cur.execute("SELECT * FROM postgremq.extend_queue_keep_alive_multi(ARRAY['Revive']::varchar[], ARRAY[60000]::bigint[])")
    assert cur.fetchall() == []
    cur.execute("SELECT postgremq.delete_queue('Revive')")
    cur.execute("SELECT postgremq.create_queue('Revive', 'TestTopic', 0, true, interval '60 seconds')")
    cur.execute("SELECT keep_alive_until > clock_timestamp() FROM postgremq.queues WHERE name='Revive'")
    assert cur.fetchone()[0]

def test_create_queue_idempotent_does_not_touch_nonexclusive(cur: psycopg2.extensions.cursor) -> None:
    """The keep_alive_until refresh in the idempotent path is gated on
    p_exclusive; non-exclusive queues' keep_alive_until stays NULL across
    re-creates."""
    cur.execute("SELECT postgremq.create_topic('TestTopic')")
    cur.execute("SELECT postgremq.create_queue('Plain', 'TestTopic', 0, false)")
    cur.execute("SELECT postgremq.create_queue('Plain', 'TestTopic', 0, false)")
    cur.execute("SELECT keep_alive_until FROM postgremq.queues WHERE name = 'Plain'")
    assert cur.fetchone()[0] is None

def test_create_queue_rejects_negative_max_attempts(cur: psycopg2.extensions.cursor) -> None:
    """create_queue must reject p_max_attempts < 0 with PMQ03. A negative
    value silently breaks postgremq.consume_message (the filter
    qm.delivery_attempts < tq.max_delivery_attempts becomes never-true once
    delivery_attempts climbs past the negative threshold) and skips DLQ
    retirement (nack_message + pmq_maintenance_fast both gate on > 0)."""
    cur.execute("SELECT postgremq.create_topic('TestTopic')")
    with pytest.raises(psycopg2.Error) as exc_info:
        cur.execute("SELECT postgremq.create_queue('Bad', 'TestTopic', -1, false)")
    assert exc_info.value.pgcode == 'PMQ03'
    assert "must be >= 0" in str(exc_info.value)

def test_create_queue_missing_topic_raises_pmq02(cur: psycopg2.extensions.cursor) -> None:
    """create_queue must raise PMQ02 (not raw 23503 FK violation) when the
    referenced topic doesn't exist, so the client can map it to its
    QueueNotFoundError sentinel — same shape as publish_message."""
    with pytest.raises(psycopg2.Error) as exc_info:
        cur.execute("SELECT postgremq.create_queue('Q', 'NoSuchTopic', 0, false)")
    assert exc_info.value.pgcode == 'PMQ02', \
        f"expected PMQ02, got {exc_info.value.pgcode}: {exc_info.value}"
    assert "does not exist" in str(exc_info.value)

def test_unlimited_delivery_attempts(cur: psycopg2.extensions.cursor) -> None:
    """Test queue with unlimited delivery attempts (max_delivery_attempts = 0)."""
    # Setup
    cur.execute("""
        SELECT postgremq.create_topic('UnlimitedTopic');
        SELECT postgremq.create_queue('UnlimitedQueue', 'UnlimitedTopic', 0, false);
        SELECT postgremq.publish_message('UnlimitedTopic', '{"test": "unlimited"}'::jsonb);
    """)

    # Try multiple delivery attempts
    for i in range(5):
        cur.execute("""
            SELECT message_id, consumer_token, delivery_attempts
            FROM postgremq.consume_message('UnlimitedQueue', 60, 1)
        """)
        msg_id, token, attempts = cur.fetchone()
        assert msg_id is not None, f"Message should be available for consumption on attempt {i+1}"
        assert attempts == i + 1, f"Delivery attempts should be {i+1}"
        
        cur.execute("SELECT postgremq.nack_message('UnlimitedQueue', %s, %s)", (msg_id, token))

    # Verify message wasn't moved to DLQ
    cur.execute("""
        SELECT * FROM postgremq.pmq_maintenance_fast();
        SELECT count(*)
        FROM postgremq.dead_letter_queue dlq
        WHERE dlq.queue_name = 'UnlimitedQueue';
    """)
    assert cur.fetchone()[0] == 0

def test_default_keep_alive_for_exclusive_queue(cur: psycopg2.extensions.cursor) -> None:
    """Test that exclusive queues get the default 5-minute keep-alive."""
    cur.execute("""
        SELECT postgremq.create_topic('TestTopic');
        SELECT postgremq.create_queue('ExQueue', 'TestTopic', 2, true);
    """)

    cur.execute("SELECT keep_alive_until FROM postgremq.queues WHERE name = 'ExQueue'")
    keep_alive = cur.fetchone()[0]
    now = datetime.now(pytz.UTC)
    assert keep_alive > now + timedelta(minutes=4, seconds=55)
    assert keep_alive < now + timedelta(minutes=5, seconds=5)

def test_queues_table_keep_alive_column_default(cur: psycopg2.extensions.cursor) -> None:
    """The queues.keep_alive_interval column default (used only on direct INSERT
    paths that bypass create_queue) must match the create_queue function
    default — otherwise schema and function disagree."""
    cur.execute("""
        SELECT column_default
        FROM information_schema.columns
        WHERE table_schema = 'postgremq' AND table_name = 'queues' AND column_name = 'keep_alive_interval'
    """)
    column_default = cur.fetchone()[0]
    assert "00:05:00" in column_default or "5 minutes" in column_default, (
        f"queues.keep_alive_interval default should be 5 minutes; got: {column_default!r}"
    )

def test_non_exclusive_queue_keep_alive_ignored(cur: psycopg2.extensions.cursor) -> None:
    """Test that keep_alive is ignored for non-exclusive queues."""
    cur.execute("""
        SELECT postgremq.create_topic('TestTopic');
        SELECT postgremq.create_queue('NonExQueue', 'TestTopic', 2, false, interval '300 seconds');
    """)

    cur.execute("SELECT keep_alive_until FROM postgremq.queues WHERE name = 'NonExQueue'")
    assert cur.fetchone()[0] is None

def test_consume_does_not_own_queue_lease(cur: psycopg2.extensions.cursor) -> None:
    """consume_message advances keep_alive_until for exclusive queues once the
    deadline has drifted into the second half of the interval, so an actively-
    polling consumer cannot have its queue GC'd. (It deliberately SKIPS the
    write while the deadline is still in the first half — see
    test_consume_skips_redundant_keep_alive_write.)"""
    cur.execute("SELECT postgremq.create_topic('TestTopic')")
    cur.execute("SELECT postgremq.create_queue('ExActive', 'TestTopic', 0, true, interval '60 seconds')")
    cur.execute("SELECT postgremq.publish_message('TestTopic', '{\"x\": 1}'::jsonb)")

    # Force the deadline into the second half of the interval (10s of 60s left)
    # so the next consume's refresh predicate fires.
    cur.execute("UPDATE postgremq.queues SET keep_alive_until = NOW() + INTERVAL '10 seconds' WHERE name = 'ExActive'")
    cur.execute("SELECT keep_alive_until FROM postgremq.queues WHERE name = 'ExActive'")
    before = cur.fetchone()[0]

    cur.execute("SELECT postgremq.consume_message('ExActive', 30, 1)")

    cur.execute("SELECT keep_alive_until FROM postgremq.queues WHERE name = 'ExActive'")
    after = cur.fetchone()[0]
    assert after == before, "only keep-alive owns queue lifetime; polling must not lock the queue row"

def test_consume_does_not_touch_keep_alive_for_non_exclusive(cur: psycopg2.extensions.cursor) -> None:
    """Non-exclusive queues have keep_alive_until = NULL; consume must leave it alone."""
    cur.execute("SELECT postgremq.create_topic('TestTopic')")
    cur.execute("SELECT postgremq.create_queue('NonExActive', 'TestTopic', 0, false)")
    cur.execute("SELECT postgremq.publish_message('TestTopic', '{\"x\": 1}'::jsonb)")

    cur.execute("SELECT postgremq.consume_message('NonExActive', 30, 1)")

    cur.execute("SELECT keep_alive_until FROM postgremq.queues WHERE name = 'NonExActive'")
    assert cur.fetchone()[0] is None

def test_consume_does_not_revive_expired_exclusive_queue(cur: psycopg2.extensions.cursor) -> None:
    """If keep_alive_until has already expired the queue is logically dead and
    consume must NOT resurrect it via the implicit refresh — otherwise an
    expired queue could survive indefinitely if a stale consumer keeps polling."""
    cur.execute("SELECT postgremq.create_topic('TestTopic')")
    cur.execute("SELECT postgremq.create_queue('ExExpired', 'TestTopic', 0, true, interval '60 seconds')")
    # Force expiry.
    cur.execute("UPDATE postgremq.queues SET keep_alive_until = NOW() - interval '5 seconds' WHERE name = 'ExExpired'")

    cur.execute("SELECT keep_alive_until FROM postgremq.queues WHERE name = 'ExExpired'")
    expired_at = cur.fetchone()[0]
    with pytest.raises(psycopg2.Error) as error:
        cur.execute("SELECT postgremq.consume_message('ExExpired', 30, 1)")
    assert error.value.pgcode == 'PMQ02'

    cur.execute("SELECT keep_alive_until FROM postgremq.queues WHERE name = 'ExExpired'")
    after = cur.fetchone()[0]
    assert after == expired_at, "expired exclusive queue must not be revived by consume"

def test_pmq_maintenance_fast(cur: psycopg2.extensions.cursor) -> None:
    """pmq_maintenance_fast retires crashed-final-attempt rows to DLQ and
    reaps expired exclusive queues, returning counters for both. Both
    branches must fire when the conditions are met, and the function must
    be idempotent (counters drop to 0 on a second call).

    A "crashed-final-attempt" row has status='processing' AND vt <= NOW()
    AND delivery_attempts >= max — i.e. a consumer started the final
    attempt, the lease has expired, and nobody acked/nacked. The retire
    predicate is gated on all three conditions; merely hitting the attempt
    cap with a still-valid lease (the consumer is mid-handler) must NOT
    retire the row — see test_maintenance_fast_skips_healthy_in_flight."""
    cur.execute("SELECT postgremq.create_topic('MaintTopic')")
    # Queue with max_attempts=2 so we can manufacture a crashed-final-attempt
    # row by bumping delivery_attempts past the threshold AND expiring vt
    # without going through nack — mimicking a consumer that died after
    # consuming the last allowed attempt. consume_message refuses to re-pick
    # those rows (delivery_attempts >= max), so they'd stay stuck in
    # 'processing' without this recovery path.
    cur.execute("SELECT postgremq.create_queue('MaintQueue', 'MaintTopic', 2, false)")
    cur.execute("SELECT postgremq.publish_message('MaintTopic', '{\"x\": 1}'::jsonb)")
    cur.execute("SELECT message_id FROM postgremq.consume_message('MaintQueue', 30, 1)")
    msg_id = cur.fetchone()[0]
    cur.execute(
        """UPDATE postgremq.queue_messages
           SET delivery_attempts = 2, vt = NOW() - interval '1 second'
           WHERE queue_name = 'MaintQueue' AND message_id = %s""",
        (msg_id,),
    )

    # Expired exclusive queue (the inactive-queue branch). Past the 5s grace.
    cur.execute("SELECT postgremq.create_queue('ExpiredEx', 'MaintTopic', 0, true, interval '60 seconds')")
    cur.execute("UPDATE postgremq.queues SET keep_alive_until = NOW() - interval '10 seconds' WHERE name = 'ExpiredEx'")

    cur.execute("SELECT retired_to_dlq, inactive_queues_dropped FROM postgremq.pmq_maintenance_fast()")
    retired, dropped = cur.fetchone()
    assert retired == 1, f"retired_to_dlq should be 1, got {retired}"
    assert dropped == 1, f"inactive_queues_dropped should be 1, got {dropped}"

    # Idempotency: a second call has nothing to do.
    cur.execute("SELECT retired_to_dlq, inactive_queues_dropped FROM postgremq.pmq_maintenance_fast()")
    retired2, dropped2 = cur.fetchone()
    assert retired2 == 0 and dropped2 == 0

    # The retired message landed in the DLQ.
    cur.execute("SELECT message_id FROM postgremq.dead_letter_queue WHERE queue_name = 'MaintQueue'")
    assert cur.fetchone()[0] == msg_id
    # The expired exclusive queue is gone.
    cur.execute("SELECT count(*) FROM postgremq.queues WHERE name = 'ExpiredEx'")
    assert cur.fetchone()[0] == 0

def test_maintenance_fast_skips_healthy_in_flight(cur: psycopg2.extensions.cursor) -> None:
    """A consumer mid-handler on its FINAL attempt (delivery_attempts == max,
    status='processing', vt > NOW()) must NOT be retired by pmq_maintenance_fast.
    Otherwise the consumer's handler commits its side-effects, then ack returns
    PMQ01 because the row was deleted underneath it, and the message ALSO lands
    in the DLQ — silent inconsistency between application and queue state."""
    cur.execute("SELECT postgremq.create_topic('HealthyMaintTopic')")
    cur.execute("SELECT postgremq.create_queue('HealthyMaintQueue', 'HealthyMaintTopic', 1, false)")
    cur.execute("SELECT postgremq.publish_message('HealthyMaintTopic', '{\"x\": 1}'::jsonb)")

    # Consume with a comfortable VT so the lease is solidly in the future.
    # max_delivery_attempts=1 so this single consume puts us at the limit.
    cur.execute("""
        SELECT message_id, consumer_token, delivery_attempts, vt
        FROM postgremq.consume_message('HealthyMaintQueue', 60, 1)
    """)
    row = cur.fetchone()
    msg_id, token, attempts, vt = row
    assert attempts == 1, f"expected delivery_attempts=1 (== max), got {attempts}"

    # Run maintenance: the row hits delivery_attempts >= max but the lease is
    # still valid. Must NOT be retired.
    cur.execute("SELECT retired_to_dlq FROM postgremq.pmq_maintenance_fast()")
    retired = cur.fetchone()[0]
    assert retired == 0, "healthy in-flight final-attempt row must not be retired"

    # Row is still processing, still owned by the original consumer.
    cur.execute("""
        SELECT status, consumer_token FROM postgremq.queue_messages
        WHERE queue_name = 'HealthyMaintQueue' AND message_id = %s
    """, (msg_id,))
    status, current_token = cur.fetchone()
    assert status == 'processing'
    assert current_token == token, "consumer_token must be unchanged (lease still held)"

    # And not in DLQ.
    cur.execute("SELECT count(*) FROM postgremq.dead_letter_queue WHERE queue_name = 'HealthyMaintQueue'")
    assert cur.fetchone()[0] == 0

    # Consumer can still ack successfully — the original goal of the lease.
    cur.execute("SELECT postgremq.ack_message('HealthyMaintQueue', %s, %s)", (msg_id, token))

def test_maintenance_fast_retires_only_when_vt_expired(cur: psycopg2.extensions.cursor) -> None:
    """Boundary test: same setup as the healthy-in-flight test, but force vt
    into the past. Now the row IS abandoned and must be retired."""
    cur.execute("SELECT postgremq.create_topic('ExpVtTopic')")
    cur.execute("SELECT postgremq.create_queue('ExpVtQueue', 'ExpVtTopic', 1, false)")
    cur.execute("SELECT postgremq.publish_message('ExpVtTopic', '{\"x\": 1}'::jsonb)")
    cur.execute("SELECT message_id FROM postgremq.consume_message('ExpVtQueue', 60, 1)")
    msg_id = cur.fetchone()[0]

    # Expire the lease — simulates "consumer crashed mid-handler".
    cur.execute(
        "UPDATE postgremq.queue_messages SET vt = NOW() - interval '1 second' "
        "WHERE queue_name = 'ExpVtQueue' AND message_id = %s",
        (msg_id,),
    )

    cur.execute("SELECT retired_to_dlq FROM postgremq.pmq_maintenance_fast()")
    assert cur.fetchone()[0] == 1

    cur.execute("SELECT count(*) FROM postgremq.dead_letter_queue WHERE queue_name = 'ExpVtQueue'")
    assert cur.fetchone()[0] == 1
    cur.execute("SELECT count(*) FROM postgremq.queue_messages WHERE queue_name = 'ExpVtQueue'")
    assert cur.fetchone()[0] == 0

def test_maintenance_fast_skips_pending_at_limit(cur: psycopg2.extensions.cursor) -> None:
    """Defensive: a row with status='pending' AND delivery_attempts >= max
    can only exist via a direct UPDATE (not via the public API) but should
    still be ignored by maintenance — retirement is reserved for the
    expired-lease abandonment case."""
    cur.execute("SELECT postgremq.create_topic('PendingLimitTopic')")
    cur.execute("SELECT postgremq.create_queue('PendingLimitQueue', 'PendingLimitTopic', 1, false)")
    cur.execute("SELECT postgremq.publish_message('PendingLimitTopic', '{\"x\": 1}'::jsonb)")
    cur.execute("SELECT message_id FROM postgremq.consume_message('PendingLimitQueue', 60, 1)")
    msg_id = cur.fetchone()[0]

    # Pathological state: pending but at the attempt cap (and lease expired
    # for good measure — only the status filter should keep this row alive).
    cur.execute(
        """UPDATE postgremq.queue_messages
           SET status = 'pending',
               vt = NOW() - interval '1 second',
               consumer_token = NULL
           WHERE queue_name = 'PendingLimitQueue' AND message_id = %s""",
        (msg_id,),
    )

    cur.execute("SELECT retired_to_dlq FROM postgremq.pmq_maintenance_fast()")
    assert cur.fetchone()[0] == 0, "pending rows are out of scope of maintenance retirement"
    cur.execute("SELECT count(*) FROM postgremq.queue_messages WHERE queue_name = 'PendingLimitQueue'")
    assert cur.fetchone()[0] == 1

def test_release_floor_at_zero(cur: psycopg2.extensions.cursor) -> None:
    """release_message must floor delivery_attempts at 0. Without the GREATEST
    guard a stale consumer racing a reclaim path could underflow the counter
    on repeated releases. The CHECK constraint on the column is the backstop
    — a direct UPDATE that would push it negative is rejected."""
    cur.execute("SELECT postgremq.create_topic('FloorTopic')")
    cur.execute("SELECT postgremq.create_queue('FloorQueue', 'FloorTopic', 0, false)")
    cur.execute("SELECT postgremq.publish_message('FloorTopic', '{\"x\": 1}'::jsonb)")

    # Consume → delivery_attempts becomes 1, status 'processing'.
    cur.execute("SELECT message_id, consumer_token FROM postgremq.consume_message('FloorQueue', 30, 1)")
    msg_id, token = cur.fetchone()
    cur.execute("SELECT delivery_attempts FROM postgremq.queue_messages WHERE queue_name='FloorQueue' AND message_id=%s", (msg_id,))
    assert cur.fetchone()[0] == 1

    # Release once: 1 → 0.
    cur.execute("SELECT postgremq.release_message('FloorQueue', %s, %s)", (msg_id, token))
    cur.execute("SELECT delivery_attempts FROM postgremq.queue_messages WHERE queue_name='FloorQueue' AND message_id=%s", (msg_id,))
    assert cur.fetchone()[0] == 0

    # Re-consume to obtain a fresh token, force delivery_attempts back to 0
    # (bypass the consume increment), then release again. Without the floor
    # this would write -1; with it, the value stays at 0.
    cur.execute("SELECT message_id, consumer_token FROM postgremq.consume_message('FloorQueue', 30, 1)")
    msg_id2, token2 = cur.fetchone()
    assert msg_id2 == msg_id
    cur.execute("UPDATE postgremq.queue_messages SET delivery_attempts = 0 WHERE queue_name='FloorQueue' AND message_id=%s", (msg_id,))
    cur.execute("SELECT postgremq.release_message('FloorQueue', %s, %s)", (msg_id, token2))
    cur.execute("SELECT delivery_attempts FROM postgremq.queue_messages WHERE queue_name='FloorQueue' AND message_id=%s", (msg_id,))
    assert cur.fetchone()[0] == 0, "release_message must floor delivery_attempts at 0"

def test_delivery_attempts_check_constraint(cur: psycopg2.extensions.cursor) -> None:
    """The queue_messages_delivery_attempts_nonneg CHECK constraint blocks any
    direct UPDATE that would write delivery_attempts < 0 — the backstop for
    paths the application-level floor doesn't cover."""
    cur.execute("SELECT postgremq.create_topic('CheckTopic')")
    cur.execute("SELECT postgremq.create_queue('CheckQueue', 'CheckTopic', 0, false)")
    cur.execute("SELECT postgremq.publish_message('CheckTopic', '{\"x\": 1}'::jsonb)")
    cur.execute("SELECT message_id FROM postgremq.consume_message('CheckQueue', 30, 1)")
    msg_id = cur.fetchone()[0]

    with pytest.raises(psycopg2.errors.CheckViolation):
        cur.execute(
            "UPDATE postgremq.queue_messages SET delivery_attempts = -1 WHERE queue_name='CheckQueue' AND message_id=%s",
            (msg_id,),
        )

def test_message_delivery_and_acknowledgment(cur: psycopg2.extensions.cursor) -> None:
    """Test message publishing, consumption, and acknowledgment."""
    # Setup
    cur.execute("SELECT postgremq.create_topic('TestTopic')")
    cur.execute("SELECT postgremq.create_queue('TestQueue', 'TestTopic', 3, true)")
    
    # Publish message
    cur.execute(
        "SELECT postgremq.publish_message('TestTopic', '{\"test\": \"ack_nack\"}'::jsonb)"
    )

    # Consume message
    cur.execute("""
        SELECT message_id, consumer_token, payload 
        FROM postgremq.consume_message('TestQueue', 300, 1)
    """)
    result = cur.fetchone()
    assert result is not None
    msg_id, consumer_token = result['message_id'], result['consumer_token']

    # Verify message is locked
    cur.execute("""
        SELECT status 
        FROM postgremq.queue_messages qm
        WHERE queue_name = 'TestQueue' AND message_id = %s
    """, (msg_id,))
    assert cur.fetchone()[0] == 'processing'

    # Acknowledge message
    cur.execute(
        "SELECT postgremq.ack_message('TestQueue', %s, %s)",
        (msg_id, consumer_token)
    )

    # Verify message is completed
    cur.execute("""
        SELECT status 
        FROM postgremq.queue_messages qm
        WHERE queue_name = 'TestQueue' AND message_id = %s
    """, (msg_id,))
    assert cur.fetchone()[0] == 'completed'

def test_message_retry_behavior(cur: psycopg2.extensions.cursor) -> None:
    """Test message delivery attempts behavior and dead letter queue functionality."""
    # Setup - queue with max 2 delivery attempts (initial + 1 retry)
    cur.execute("""
        SELECT postgremq.create_topic('RetryTopic');
        SELECT postgremq.create_queue('RetryQueue', 'RetryTopic', 2, true);
        SELECT postgremq.publish_message('RetryTopic', '{"test": "retry"}'::jsonb);
    """)

    # First delivery attempt
    cur.execute("""
        SELECT message_id, consumer_token 
        FROM postgremq.consume_message('RetryQueue', 60, 1)
    """)
    msg_id, token = cur.fetchone()
    cur.execute("SELECT postgremq.nack_message('RetryQueue', %s, %s)", (msg_id, token))

    # Second delivery attempt
    cur.execute("""
        SELECT message_id, consumer_token 
        FROM postgremq.consume_message('RetryQueue', 60, 1)
    """)
    msg_id, token = cur.fetchone()
    cur.execute("SELECT postgremq.nack_message('RetryQueue', %s, %s)", (msg_id, token))

    # Verify no more attempts available
    cur.execute("""
        SELECT count(*) 
        FROM postgremq.consume_message('RetryQueue', 60, 1)
    """)
    assert cur.fetchone()[0] == 0

    # Move to DLQ and verify
    cur.execute("""
        SELECT * FROM postgremq.pmq_maintenance_fast();
        SELECT count(*), MAX(retry_count)
        FROM postgremq.dead_letter_queue dlq
        WHERE dlq.queue_name = 'RetryQueue';
    """)
    count, retries = cur.fetchone()
    assert count == 1
    assert retries == 2

def test_queue_creation_with_delivery_attempts(cur: psycopg2.extensions.cursor) -> None:
    """Test queue creation with different max_delivery_attempts values."""
    cur.execute("""
        SELECT postgremq.create_topic('DeliveryTopic');
        SELECT postgremq.create_queue('UnlimitedQueue', 'DeliveryTopic', 0, true);
        SELECT postgremq.create_queue('LimitedQueue', 'DeliveryTopic', 3, true);
        
        SELECT name, max_delivery_attempts 
        FROM postgremq.queues
        WHERE topic_name = 'DeliveryTopic' 
        ORDER BY name;
    """)
    
    results = cur.fetchall()
    assert len(results) == 2
    assert results[0]['name'] == 'LimitedQueue'
    assert results[0]['max_delivery_attempts'] == 3
    assert results[1]['name'] == 'UnlimitedQueue'
    assert results[1]['max_delivery_attempts'] == 0

def test_concurrent_message_access(cur: psycopg2.extensions.cursor) -> None:
    """Test concurrent access to messages."""
    # Setup
    cur.execute("SELECT postgremq.create_topic('ConcurrentTopic')")
    cur.execute("SELECT postgremq.create_queue('ConcurrentQueue', 'ConcurrentTopic', 3, true)")
    cur.execute(
        "SELECT postgremq.publish_message('ConcurrentTopic', '{\"test\": \"concurrent\"}'::jsonb)"
    )

    # First consumer gets the message
    cur.execute("""
        SELECT message_id, consumer_token 
        FROM postgremq.consume_message('ConcurrentQueue', 60, 1)
    """)
    assert cur.fetchone() is not None

    # Second consumer should get nothing
    cur.execute("""
        SELECT count(*) 
        FROM postgremq.consume_message('ConcurrentQueue', 60, 1)
    """)
    assert cur.fetchone()[0] == 0

def test_queue_cleanup(cur: psycopg2.extensions.cursor) -> None:
    """Test queue cleanup functionality."""
    # Setup
    cur.execute("SELECT postgremq.create_topic('CleanupTopic')")
    cur.execute("SELECT postgremq.create_queue('CleanupQueue', 'CleanupTopic', 3, true)")
    cur.execute(
        "SELECT postgremq.publish_message('CleanupTopic', '{\"test\": \"cleanup\"}'::jsonb)"
    )

    # Clean up topic
    cur.execute("SELECT postgremq.clean_up_topic('CleanupTopic')")
    cur.execute("SELECT postgremq.delete_topic('CleanupTopic')")

    # Verify cleanup
    cur.execute(
        "SELECT count(*) FROM postgremq.queues WHERE topic_name = 'CleanupTopic'"
    )
    assert cur.fetchone()[0] == 0
    cur.execute(
        "SELECT count(*) FROM postgremq.messages WHERE topic_name = 'CleanupTopic'"
    )
    assert cur.fetchone()[0] == 0

def test_set_vt(cur: psycopg2.extensions.cursor) -> None:
    """Test setting visibility timeout for a single message."""
    # Create test topic and queue
    cur.execute("SELECT postgremq.create_topic('test_topic')")
    cur.execute("SELECT postgremq.create_queue('test_queue', 'test_topic')")
    
    # Publish a test message
    cur.execute("SELECT postgremq.publish_message('test_topic', '{\"test\": \"data\"}'::jsonb)")
    
    # Consume the message to get it into processing state
    cur.execute("""
        SELECT queue_name, message_id, consumer_token 
        FROM postgremq.consume_message('test_queue', 30)
    """)
    message = cur.fetchone()
    queue_name, message_id, consumer_token = message
    
    # Test valid extension
    cur.execute("""
        SELECT postgremq.set_vt(%s, %s, %s, 60)
    """, (queue_name, message_id, consumer_token))
    new_vt = cur.fetchone()[0]
    assert new_vt > datetime.now(pytz.UTC)
    assert new_vt < datetime.now(pytz.UTC) + timedelta(seconds=61)
    
    # Test wrong consumer token
    with pytest.raises(Exception):
        cur.execute("""
            SELECT postgremq.set_vt(%s, %s, %s, 60)
        """, (queue_name, message_id, 'wrong-token'))
    
    # Test wrong message ID
    with pytest.raises(Exception):
        cur.execute("""
            SELECT postgremq.set_vt(%s, %s, %s, 60)
        """, (queue_name, message_id + 1, consumer_token))

def _consume_one(cur, queue, vt=30):
    """Consume a single message from a queue, returning (message_id, token)."""
    cur.execute(
        "SELECT message_id, consumer_token FROM postgremq.consume_message(%s, %s, 1)",
        (queue, vt),
    )
    row = cur.fetchone()
    return row['message_id'], row['consumer_token']


def test_set_vt_batch_multi_extends_across_queues(cur):
    """One set_vt_batch_multi call extends in-flight messages spanning several
    queues in a single round-trip, correlating results by (queue, message_id)."""
    cur.execute("SELECT postgremq.create_topic('MTopic')")
    cur.execute("SELECT postgremq.create_queue('MQ1', 'MTopic', 3, false)")
    cur.execute("SELECT postgremq.create_queue('MQ2', 'MTopic', 3, false)")
    # One publish fans out to BOTH queues — same message_id lands in MQ1 and MQ2.
    cur.execute("SELECT postgremq.publish_message('MTopic', '{\"k\": 1}'::jsonb)")

    m1, t1 = _consume_one(cur, 'MQ1', vt=30)
    m2, t2 = _consume_one(cur, 'MQ2', vt=30)
    assert m1 == m2  # same message_id in both queues

    cur.execute(
        """
        SELECT queue_name, message_id, vt
        FROM postgremq.set_vt_batch_multi(%s, %s, %s, %s)
        """,
        (['MQ1', 'MQ2'], [m1, m2], [t1, t2], [600, 600]),
    )
    rows = {(r['queue_name'], r['message_id']) for r in cur.fetchall()}
    assert rows == {('MQ1', m1), ('MQ2', m2)}

    # Both rows pushed out to ~600s.
    cur.execute(
        "SELECT queue_name, vt FROM postgremq.queue_messages WHERE message_id = %s ORDER BY queue_name",
        (m1,),
    )
    now = datetime.now(pytz.UTC)
    for r in cur.fetchall():
        assert r['vt'] > now + timedelta(seconds=590)


def test_set_vt_batch_multi_composite_key_no_cross_queue_bleed(cur):
    """The (queue, message_id) key must be respected: extending one queue's copy
    of a shared message_id must NOT touch the other queue's copy."""
    cur.execute("SELECT postgremq.create_topic('BleedTopic')")
    cur.execute("SELECT postgremq.create_queue('BQ1', 'BleedTopic', 3, false)")
    cur.execute("SELECT postgremq.create_queue('BQ2', 'BleedTopic', 3, false)")
    cur.execute("SELECT postgremq.publish_message('BleedTopic', '{}'::jsonb)")

    m1, t1 = _consume_one(cur, 'BQ1', vt=30)
    m2, t2 = _consume_one(cur, 'BQ2', vt=30)
    assert m1 == m2

    # Record BQ2's current vt, then extend ONLY BQ1's copy.
    cur.execute(
        "SELECT vt FROM postgremq.queue_messages WHERE queue_name = 'BQ2' AND message_id = %s",
        (m2,),
    )
    bq2_vt_before = cur.fetchone()['vt']

    cur.execute(
        "SELECT queue_name, message_id FROM postgremq.set_vt_batch_multi(%s, %s, %s, %s)",
        (['BQ1'], [m1], [t1], [600]),
    )
    rows = [(r['queue_name'], r['message_id']) for r in cur.fetchall()]
    assert rows == [('BQ1', m1)]  # only BQ1 returned

    # BQ1 extended, BQ2 untouched.
    cur.execute(
        "SELECT queue_name, vt FROM postgremq.queue_messages WHERE message_id = %s ORDER BY queue_name",
        (m1,),
    )
    by_q = {r['queue_name']: r['vt'] for r in cur.fetchall()}
    now = datetime.now(pytz.UTC)
    assert by_q['BQ1'] > now + timedelta(seconds=590)
    assert by_q['BQ2'] == bq2_vt_before


def test_set_vt_batch_multi_mixed_vt(cur):
    """Per-row VTs: each message gets its own new visibility timeout."""
    cur.execute("SELECT postgremq.create_topic('MixTopic')")
    cur.execute("SELECT postgremq.create_queue('MixQ', 'MixTopic', 3, false)")
    for _ in range(2):
        cur.execute("SELECT postgremq.publish_message('MixTopic', '{}'::jsonb)")

    cur.execute(
        "SELECT message_id, consumer_token FROM postgremq.consume_message('MixQ', 30, 2)"
    )
    rows = cur.fetchall()
    (a_id, a_tok), (b_id, b_tok) = (
        (rows[0]['message_id'], rows[0]['consumer_token']),
        (rows[1]['message_id'], rows[1]['consumer_token']),
    )

    cur.execute(
        "SELECT message_id, vt FROM postgremq.set_vt_batch_multi(%s, %s, %s, %s)",
        (['MixQ', 'MixQ'], [a_id, b_id], [a_tok, b_tok], [120, 600]),
    )
    got = {r['message_id']: r['vt'] for r in cur.fetchall()}
    now = datetime.now(pytz.UTC)
    # a ~120s, b ~600s — distinct windows.
    assert now + timedelta(seconds=110) < got[a_id] < now + timedelta(seconds=130)
    assert got[b_id] > now + timedelta(seconds=590)


def test_set_vt_batch_multi_partial_lease_loss(cur):
    """A wrong token (lease lost) is silently omitted; valid rows still extend —
    across queues, in the same call."""
    cur.execute("SELECT postgremq.create_topic('PartTopic')")
    cur.execute("SELECT postgremq.create_queue('PQ1', 'PartTopic', 3, false)")
    cur.execute("SELECT postgremq.create_queue('PQ2', 'PartTopic', 3, false)")
    cur.execute("SELECT postgremq.publish_message('PartTopic', '{}'::jsonb)")

    m1, t1 = _consume_one(cur, 'PQ1', vt=30)
    m2, _t2 = _consume_one(cur, 'PQ2', vt=30)

    cur.execute(
        "SELECT queue_name, message_id FROM postgremq.set_vt_batch_multi(%s, %s, %s, %s)",
        (['PQ1', 'PQ2'], [m1, m2], [t1, 'wrong-token'], [600, 600]),
    )
    rows = [(r['queue_name'], r['message_id']) for r in cur.fetchall()]
    assert rows == [('PQ1', m1)]  # only the valid-token row


def test_set_vt_batch_multi_validation(cur):
    """Length mismatch and negative VT raise PMQ03; empty arrays are a no-op."""
    cur.execute("SELECT postgremq.create_topic('VTopic')")
    cur.execute("SELECT postgremq.create_queue('VQ', 'VTopic', 3, false)")

    # Empty arrays -> zero rows, no error.
    cur.execute(
        "SELECT COUNT(*) FROM postgremq.set_vt_batch_multi(%s, %s, %s, %s)",
        ([], [], [], []),
    )
    assert cur.fetchone()[0] == 0

    # Mismatched lengths -> PMQ03.
    with pytest.raises(psycopg2.Error) as exc_info:
        cur.execute(
            "SELECT * FROM postgremq.set_vt_batch_multi("
            "ARRAY['VQ']::varchar[], ARRAY[1,2]::bigint[], "
            "ARRAY['a']::varchar[], ARRAY[60]::int[])"
        )
    assert exc_info.value.pgcode == 'PMQ03'
    cur.connection.rollback()

    # Negative VT -> PMQ03.
    with pytest.raises(psycopg2.Error) as exc_info:
        cur.execute(
            "SELECT * FROM postgremq.set_vt_batch_multi("
            "ARRAY['VQ']::varchar[], ARRAY[1]::bigint[], "
            "ARRAY['a']::varchar[], ARRAY[-1]::int[])"
        )
    assert exc_info.value.pgcode == 'PMQ03'
    cur.connection.rollback()


def _keep_alive_until(cur, queue):
    cur.execute("SELECT keep_alive_until FROM postgremq.queues WHERE name = %s", (queue,))
    return cur.fetchone()['keep_alive_until']


def test_extend_keep_alive_multi_extends_many_queues(cur):
    """One call advances keep_alive_until for several exclusive queues, using a
    per-queue interval, and returns each extended queue name."""
    cur.execute("SELECT postgremq.create_topic('KaTopic')")
    cur.execute("SELECT postgremq.create_queue('KaQ1', 'KaTopic', 0, true, interval '60 seconds')")
    cur.execute("SELECT postgremq.create_queue('KaQ2', 'KaTopic', 0, true, interval '60 seconds')")

    cur.execute(
        "SELECT queue_name FROM postgremq.extend_queue_keep_alive_multi(%s, %s)",
        (['KaQ1', 'KaQ2'], [600_000, 900_000]),  # milliseconds
    )
    extended = {r['queue_name'] for r in cur.fetchall()}
    assert extended == {'KaQ1', 'KaQ2'}

    now = datetime.now(pytz.UTC)
    # KaQ1 ~600s, KaQ2 ~900s — per-row intervals applied.
    ka1 = _keep_alive_until(cur, 'KaQ1')
    ka2 = _keep_alive_until(cur, 'KaQ2')
    assert now + timedelta(seconds=590) < ka1 < now + timedelta(seconds=610)
    assert now + timedelta(seconds=890) < ka2 < now + timedelta(seconds=910)


def test_extend_keep_alive_multi_omits_missing_and_nonexclusive(cur):
    """Missing or non-exclusive queues are silently omitted (permanent failure);
    valid exclusive queues still extend in the same call."""
    cur.execute("SELECT postgremq.create_topic('KaOmitTopic')")
    cur.execute("SELECT postgremq.create_queue('KaExcl', 'KaOmitTopic', 0, true, interval '60 seconds')")
    cur.execute("SELECT postgremq.create_queue('KaNonExcl', 'KaOmitTopic', 0, false)")

    cur.execute(
        "SELECT queue_name FROM postgremq.extend_queue_keep_alive_multi(%s, %s)",
        (
            ['KaExcl', 'KaNonExcl', 'KaGhost'],
            [600_000] * 3,  # milliseconds
        ),
    )
    extended = {r['queue_name'] for r in cur.fetchall()}
    assert extended == {'KaExcl'}  # non-exclusive and missing both omitted


def test_extend_keep_alive_multi_validation(cur):
    """Length mismatch raises PMQ03; empty arrays are a no-op."""
    # Empty arrays -> zero rows.
    cur.execute(
        "SELECT COUNT(*) FROM postgremq.extend_queue_keep_alive_multi(%s, %s)",
        ([], []),
    )
    assert cur.fetchone()[0] == 0

    # Mismatched lengths -> PMQ03.
    with pytest.raises(psycopg2.Error) as exc_info:
        cur.execute(
            "SELECT * FROM postgremq.extend_queue_keep_alive_multi("
            "ARRAY['A','B']::varchar[], ARRAY[60000]::bigint[])"
        )
    assert exc_info.value.pgcode == 'PMQ03'
    cur.connection.rollback()


def test_requeue_dlq_messages_resets_delivery_attempts(cur):
    # Create topic and queue
    topic = "test_requeue_dlq_topic"
    queue = "test_requeue_dlq_queue"
    cur.execute("SELECT postgremq.create_topic(%s)", (topic,))
    cur.execute("SELECT postgremq.create_queue(%s, %s, %s, %s, %s * interval '1 sec')",
                  (queue, topic, 2, True, 60))

    # Publish a message
    cur.execute("SELECT postgremq.publish_message(%s, %s)",
                  (topic, '{"test":"requeue"}'))
    cur.execute("SELECT id FROM postgremq.messages ORDER BY id DESC LIMIT 1")
    msg_id = cur.fetchone()[0]

    # Consume and fail the message until it reaches max attempts
    for _ in range(2):  # max_delivery_attempts = 2
        cur.execute("""
            SELECT queue_name, message_id, payload, consumer_token 
            FROM postgremq.consume_message(%s, 30)
        """, (queue,))
        result = cur.fetchone()
        assert result is not None, "Should get a message"
        
        cur.execute("""
            SELECT postgremq.nack_message(%s, %s, %s)
        """, (queue, msg_id, result['consumer_token']))  # Use dictionary access for DictCursor

    # Move messages to DLQ
    cur.execute("SELECT * FROM postgremq.pmq_maintenance_fast()")

    # Verify message moved to DLQ
    cur.execute("SELECT message_id FROM postgremq.dead_letter_queue WHERE queue_name = %s", (queue,))
    dlq_messages = cur.fetchall()
    assert len(dlq_messages) == 1, "Message should be in DLQ"
    assert dlq_messages[0][0] == msg_id, "DLQ message ID mismatch"

    # Verify message is no longer in queue_messages
    cur.execute("""
        SELECT COUNT(*) FROM postgremq.queue_messages
        WHERE queue_name = %s AND message_id = %s
    """, (queue, msg_id))
    count = cur.fetchone()[0]
    assert count == 0, "Message should not be in queue_messages"

    # Requeue the message
    cur.execute("SELECT postgremq.requeue_dlq_messages(%s)", (queue,))

    # Verify message is back in queue with reset delivery attempts
    cur.execute("""
        SELECT message_id, delivery_attempts 
        FROM postgremq.queue_messages
        WHERE queue_name = %s AND message_id = %s
    """, (queue, msg_id))
    result = cur.fetchone()
    assert result is not None, "Message should be back in queue"
    assert result[0] == msg_id, "Message ID mismatch"
    assert result[1] == 0, "Delivery attempts should be reset to 0"

    # Verify DLQ is empty
    cur.execute("SELECT COUNT(*) FROM postgremq.dead_letter_queue WHERE queue_name = %s", (queue,))
    dlq_count = cur.fetchone()[0]
    assert dlq_count == 0, "DLQ should be empty after requeue"

def test_requeue_dlq_messages_emits_notify(cur: psycopg2.extensions.cursor) -> None:
    """requeue_dlq_messages emits one NOTIFY on `pmq:q:<queue>` per call
    (regardless of how many messages were requeued). Without this signal
    consumers wait for their poll fallback (1s in TS, 10s in Go) before
    re-fetching, defeating the point of using requeue for fast recovery.

    Also asserts no NOTIFY fires when the DLQ is empty for the queue —
    avoids spurious wake-ups."""
    cur.execute("SELECT postgremq.create_topic('RequeueNotifyTopic')")
    cur.execute("SELECT postgremq.create_queue('RequeueNotifyQueue', 'RequeueNotifyTopic', 1, false)")
    cur.execute('LISTEN "pmq:q:RequeueNotifyQueue"')

    # Empty DLQ: requeue should be a silent no-op.
    cur.execute("SELECT postgremq.requeue_dlq_messages('RequeueNotifyQueue')")
    cur.connection.poll()
    assert len(cur.connection.notifies) == 0, "no-op requeue must not NOTIFY"

    # Stage 3 messages in the DLQ via the inline-retire path (max_attempts=1
    # means the first nack moves them straight to DLQ).
    for i in range(3):
        cur.execute("SELECT postgremq.publish_message('RequeueNotifyTopic', %s::jsonb)",
                    (json.dumps({"i": i}),))
        cur.execute("SELECT message_id, consumer_token FROM postgremq.consume_message('RequeueNotifyQueue', 30)")
        msg_id, token = cur.fetchone()
        cur.execute("SELECT postgremq.nack_message('RequeueNotifyQueue', %s, %s)", (msg_id, token))

    cur.execute("SELECT count(*) FROM postgremq.dead_letter_queue WHERE queue_name = 'RequeueNotifyQueue'")
    assert cur.fetchone()[0] == 3

    # Drain any stray notifications from the staging steps so we measure
    # only what requeue itself emits.
    cur.connection.poll()
    cur.connection.notifies.clear()

    cur.execute("SELECT postgremq.requeue_dlq_messages('RequeueNotifyQueue')")
    cur.connection.poll()

    # Exactly one NOTIFY per requeue call, empty payload.
    assert len(cur.connection.notifies) == 1, \
        f"expected exactly 1 NOTIFY, got {len(cur.connection.notifies)}"
    n = cur.connection.notifies[0]
    assert n.channel == 'pmq:q:RequeueNotifyQueue'
    assert n.payload == ''
    cur.connection.notifies.clear()

def test_requeue_dlq_messages_idempotent_on_existing_row(cur: psycopg2.extensions.cursor) -> None:
    """If a queue_messages row already exists for a (queue, message_id) we
    are about to requeue, the function must reset that row to a clean
    pending state instead of aborting the entire requeue with a unique
    constraint violation. This protects against partial-state recovery
    scenarios."""
    cur.execute("SELECT postgremq.create_topic('RequeueIdempotentTopic')")
    cur.execute("SELECT postgremq.create_queue('RequeueIdempotentQueue', 'RequeueIdempotentTopic', 1, false)")

    cur.execute("SELECT postgremq.publish_message('RequeueIdempotentTopic', '{\"x\":1}'::jsonb)")
    cur.execute("SELECT message_id, consumer_token FROM postgremq.consume_message('RequeueIdempotentQueue', 30)")
    msg_id, token = cur.fetchone()
    cur.execute("SELECT postgremq.nack_message('RequeueIdempotentQueue', %s, %s)", (msg_id, token))

    # Now the message is in DLQ. Manually re-insert a stale queue_messages
    # row in 'completed' state to simulate the conflict scenario.
    cur.execute("""
        INSERT INTO postgremq.queue_messages (queue_name, message_id, status, delivery_attempts, processed_at, vt)
        VALUES ('RequeueIdempotentQueue', %s, 'completed', 5, NOW(), NOW())
    """, (msg_id,))

    # Requeue must NOT fail with a unique violation; it must reset the
    # stale row to pending.
    cur.execute("SELECT postgremq.requeue_dlq_messages('RequeueIdempotentQueue')")

    cur.execute("""
        SELECT status, delivery_attempts, consumer_token, processed_at
        FROM postgremq.queue_messages
        WHERE queue_name = 'RequeueIdempotentQueue' AND message_id = %s
    """, (msg_id,))
    status, attempts, ctoken, processed_at = cur.fetchone()
    assert status == 'pending'
    assert attempts == 0
    assert ctoken is None
    assert processed_at is None

    cur.execute("SELECT count(*) FROM postgremq.dead_letter_queue WHERE queue_name = 'RequeueIdempotentQueue'")
    assert cur.fetchone()[0] == 0, "DLQ should be drained after requeue"

def test_delayed_message_delivery_notifications(cur: psycopg2.extensions.cursor) -> None:
    """Publish fires NOTIFY on per-topic channel `pmq:t:<topic>`. Payload is
    empty — the channel name is the wake-up signal; clients re-fetch from
    the queue on receipt."""
    cur.execute("SELECT postgremq.create_topic('TestTopic')")
    cur.execute("SELECT postgremq.create_queue('TestQueue', 'TestTopic', 2, false)")

    # Listen on the per-topic channel.
    cur.execute('LISTEN "pmq:t:TestTopic"')

    # Publish message with 2 second delay
    delay_time = datetime.now(pytz.UTC) + timedelta(seconds=2)
    cur.execute("""
        SELECT postgremq.publish_message('TestTopic', '{"test":"data"}'::jsonb, %s)
    """, (delay_time,))

    # Get notification
    conn = cur.connection
    conn.poll()
    notify = conn.notifies.pop(0)

    assert notify.channel == 'pmq:t:TestTopic'
    assert notify.payload == ''

    # Try to consume immediately - should get no messages
    cur.execute("""
        SELECT * FROM postgremq.consume_message('TestQueue', 30)
    """)
    assert cur.fetchone() is None

    # Wait for delay
    time.sleep(2)

    # Now should get the message
    cur.execute("""
        SELECT * FROM postgremq.consume_message('TestQueue', 30)
    """)
    assert cur.fetchone() is not None

def test_delayed_nack_notifications(cur: psycopg2.extensions.cursor) -> None:
    """Nack fires NOTIFY on the per-queue channel `pmq:q:<queue>`. Payload
    is empty — the channel name is the wake-up signal."""
    cur.execute("SELECT postgremq.create_topic('TestTopic')")
    cur.execute("SELECT postgremq.create_queue('TestQueue', 'TestTopic', 2, false)")

    # Publish and consume message
    cur.execute("SELECT postgremq.publish_message('TestTopic', '{\"test\":\"data\"}'::jsonb)")
    cur.execute("""
        SELECT message_id, consumer_token
        FROM postgremq.consume_message('TestQueue', 30)
    """)
    msg_id, token = cur.fetchone()

    # Listen on the per-queue channel.
    cur.execute('LISTEN "pmq:q:TestQueue"')

    # Nack with 2 second delay
    delay_time = datetime.now(pytz.UTC) + timedelta(seconds=2)
    cur.execute("""
        SELECT postgremq.nack_message('TestQueue', %s, %s, %s)
    """, (msg_id, token, delay_time))

    # Get notification
    conn = cur.connection
    conn.poll()
    notify = conn.notifies.pop(0)

    assert notify.channel == 'pmq:q:TestQueue'
    assert notify.payload == ''

    # Try to consume immediately - should get no messages
    cur.execute("""
        SELECT * FROM postgremq.consume_message('TestQueue', 30)
    """)
    assert cur.fetchone() is None

    # Wait for delay
    time.sleep(2)

    # Now should get the message
    cur.execute("""
        SELECT * FROM postgremq.consume_message('TestQueue', 30)
    """)
    msg = cur.fetchone()
    assert msg is not None
    assert msg[1] == msg_id  # message_id matches

def test_release_message_emits_per_queue_notify(cur: psycopg2.extensions.cursor) -> None:
    """release_message fires NOTIFY on `pmq:q:<queue>` with empty payload —
    the channel is the wake-up signal; clients re-fetch on receipt."""
    cur.execute("SELECT postgremq.create_topic('TestTopic')")
    cur.execute("SELECT postgremq.create_queue('TestQueue', 'TestTopic', 0, false)")

    cur.execute("SELECT postgremq.publish_message('TestTopic', '{\"k\":\"v\"}'::jsonb)")
    cur.execute("SELECT message_id, consumer_token FROM postgremq.consume_message('TestQueue', 30)")
    msg_id, token = cur.fetchone()

    cur.execute('LISTEN "pmq:q:TestQueue"')
    cur.execute("SELECT postgremq.release_message('TestQueue', %s, %s)", (msg_id, token))

    conn = cur.connection
    conn.poll()
    notify = conn.notifies.pop(0)
    assert notify.channel == 'pmq:q:TestQueue'
    assert notify.payload == ''

def test_create_topic_validates_name(cur: psycopg2.extensions.cursor) -> None:
    """create_topic rejects names with characters that wouldn't be safe as NOTIFY channel suffixes."""
    cur.execute("SELECT postgremq.create_topic('valid_topic-1.2:3')")  # all permitted chars
    for bad in ["topic with spaces", "topic'name", 'topic"name', "topic;name", ""]:
        try:
            cur.execute("SELECT postgremq.create_topic(%s)", (bad,))
            raise AssertionError(f"create_topic should have rejected {bad!r}")
        except psycopg2.Error:
            cur.connection.rollback()

def test_create_queue_validates_name(cur: psycopg2.extensions.cursor) -> None:
    """create_queue rejects names with characters that wouldn't be safe as NOTIFY channel suffixes."""
    cur.execute("SELECT postgremq.create_topic('Topic')")
    cur.execute("SELECT postgremq.create_queue('valid_queue-1.2:3', 'Topic', 0, false)")
    for bad in ["queue with spaces", "queue'name", 'queue"name', "queue;name", ""]:
        try:
            cur.execute("SELECT postgremq.create_queue(%s, 'Topic', 0, false)", (bad,))
            raise AssertionError(f"create_queue should have rejected {bad!r}")
        except psycopg2.Error:
            cur.connection.rollback()

def test_clean_up_topic_refuses_when_dlq_has_messages(cur: psycopg2.extensions.cursor) -> None:
    """clean_up_topic must refuse when any of the topic's messages are in
    DLQ. DLQ entries are forensic data; operators must explicitly
    postgremq.purge_dlq() or postgremq.requeue_dlq_messages() before cleaning up."""
    cur.execute("SELECT postgremq.create_topic('CleanDlqTopic')")
    cur.execute("SELECT postgremq.create_queue('CleanDlqQueue', 'CleanDlqTopic', 1, false)")
    cur.execute("SELECT postgremq.publish_message('CleanDlqTopic', '{\"x\":1}'::jsonb)")
    cur.execute("SELECT message_id, consumer_token FROM postgremq.consume_message('CleanDlqQueue', 30)")
    msg_id, token = cur.fetchone()
    # max=1 → first nack retires inline to DLQ.
    cur.execute("SELECT postgremq.nack_message('CleanDlqQueue', %s, %s)", (msg_id, token))

    cur.execute("SELECT count(*) FROM postgremq.dead_letter_queue WHERE queue_name = 'CleanDlqQueue'")
    assert cur.fetchone()[0] == 1

    with pytest.raises(psycopg2.Error) as exc_info:
        cur.execute("SELECT postgremq.clean_up_topic('CleanDlqTopic')")
    assert exc_info.value.pgcode == 'PMQ03'
    assert 'dead letter queue' in str(exc_info.value)
    cur.connection.rollback()

    # After purge_dlq, clean_up_topic succeeds.
    cur.execute("SELECT postgremq.purge_dlq()")
    cur.execute("SELECT postgremq.clean_up_topic('CleanDlqTopic')")
    cur.execute("SELECT count(*) FROM postgremq.messages WHERE topic_name = 'CleanDlqTopic'")
    assert cur.fetchone()[0] == 0

def test_delete_queue_refuses_when_dlq_has_messages(cur: psycopg2.extensions.cursor) -> None:
    """delete_queue must refuse when the queue has DLQ entries. Same
    reason as clean_up_topic."""
    cur.execute("SELECT postgremq.create_topic('DeleteDlqTopic')")
    cur.execute("SELECT postgremq.create_queue('DeleteDlqQueue', 'DeleteDlqTopic', 1, false)")
    cur.execute("SELECT postgremq.publish_message('DeleteDlqTopic', '{\"x\":1}'::jsonb)")
    cur.execute("SELECT message_id, consumer_token FROM postgremq.consume_message('DeleteDlqQueue', 30)")
    msg_id, token = cur.fetchone()
    cur.execute("SELECT postgremq.nack_message('DeleteDlqQueue', %s, %s)", (msg_id, token))

    with pytest.raises(psycopg2.Error) as exc_info:
        cur.execute("SELECT postgremq.delete_queue('DeleteDlqQueue')")
    assert exc_info.value.pgcode == 'PMQ03'
    assert 'dead letter queue' in str(exc_info.value)
    cur.connection.rollback()

    # After purge_dlq, delete_queue succeeds.
    cur.execute("SELECT postgremq.purge_dlq()")
    cur.execute("SELECT postgremq.delete_queue('DeleteDlqQueue')")
    cur.execute("SELECT count(*) FROM postgremq.queues WHERE name = 'DeleteDlqQueue'")
    assert cur.fetchone()[0] == 0

def test_pmq_maintenance_fast_skips_inactive_queue_with_dlq(cur: psycopg2.extensions.cursor) -> None:
    """pmq_maintenance_fast skips inactive exclusive queues that have
    DLQ entries — those entries are forensic data the operator may want
    to keep. The queue stays around until the operator handles its DLQ."""
    cur.execute("SELECT postgremq.create_topic('MaintDlqTopic')")
    cur.execute("SELECT postgremq.create_queue('MaintDlqQueue', 'MaintDlqTopic', 1, true, interval '60 seconds')")
    cur.execute("SELECT postgremq.publish_message('MaintDlqTopic', '{\"x\":1}'::jsonb)")
    cur.execute("SELECT message_id, consumer_token FROM postgremq.consume_message('MaintDlqQueue', 30)")
    msg_id, token = cur.fetchone()
    cur.execute("SELECT postgremq.nack_message('MaintDlqQueue', %s, %s)", (msg_id, token))

    # Force the queue's keep_alive_until past the 5s grace — eligible for reaping.
    cur.execute("UPDATE postgremq.queues SET keep_alive_until = NOW() - interval '10 seconds' WHERE name = 'MaintDlqQueue'")

    cur.execute("SELECT inactive_queues_dropped FROM postgremq.pmq_maintenance_fast()")
    assert cur.fetchone()[0] == 0, "queue with DLQ entries must not be reaped"

    cur.execute("SELECT count(*) FROM postgremq.queues WHERE name = 'MaintDlqQueue'")
    assert cur.fetchone()[0] == 1, "queue must still exist"

    # After purge_dlq, the next maintenance call reaps it.
    cur.execute("SELECT postgremq.purge_dlq()")
    cur.execute("SELECT inactive_queues_dropped FROM postgremq.pmq_maintenance_fast()")
    assert cur.fetchone()[0] == 1
    cur.execute("SELECT count(*) FROM postgremq.queues WHERE name = 'MaintDlqQueue'")
    assert cur.fetchone()[0] == 0

def test_create_topic_rejects_long_name(cur: psycopg2.extensions.cursor) -> None:
    """Topic names > 57 bytes would truncate the NOTIFY channel name and
    risk silent cross-delivery between topics that share their first 57
    bytes. Boundary check at exactly 57 / 58 bytes."""
    name_57 = 'A' * 57
    cur.execute("SELECT postgremq.create_topic(%s)", (name_57,))  # boundary: 57 OK
    name_58 = 'B' * 58
    with pytest.raises(psycopg2.Error) as exc_info:
        cur.execute("SELECT postgremq.create_topic(%s)", (name_58,))
    assert exc_info.value.pgcode == 'PMQ03'
    assert 'too long' in str(exc_info.value)

def test_create_queue_rejects_long_name(cur: psycopg2.extensions.cursor) -> None:
    """Queue names > 57 bytes would truncate the NOTIFY channel name."""
    cur.execute("SELECT postgremq.create_topic('LenTopic')")
    name_57 = 'a' * 57
    cur.execute("SELECT postgremq.create_queue(%s, 'LenTopic', 0, false)", (name_57,))
    name_58 = 'b' * 58
    with pytest.raises(psycopg2.Error) as exc_info:
        cur.execute("SELECT postgremq.create_queue(%s, 'LenTopic', 0, false)", (name_58,))
    assert exc_info.value.pgcode == 'PMQ03'
    assert 'too long' in str(exc_info.value)

def test_sqlstate_codes_pmq01_lease_lost(cur: psycopg2.extensions.cursor) -> None:
    """ack/nack/release/set_vt all raise PMQ01 when token doesn't match or vt expired."""
    cur.execute("SELECT postgremq.create_topic('SqlstateTopic')")
    cur.execute("SELECT postgremq.create_queue('SqlstateQueue', 'SqlstateTopic', 0, false)")
    cur.execute("SELECT postgremq.publish_message('SqlstateTopic', '{}'::jsonb)")
    cur.execute("SELECT message_id, consumer_token FROM postgremq.consume_message('SqlstateQueue', 30)")
    msg_id, _ = cur.fetchone()

    # Wrong token → PMQ01 from each operation.
    for sql, args in [
        ("SELECT postgremq.ack_message('SqlstateQueue', %s, %s)", (msg_id, 'bogus')),
        ("SELECT postgremq.nack_message('SqlstateQueue', %s, %s)", (msg_id, 'bogus')),
        ("SELECT postgremq.release_message('SqlstateQueue', %s, %s)", (msg_id, 'bogus')),
        ("SELECT postgremq.set_vt('SqlstateQueue', %s, %s, 60)", (msg_id, 'bogus')),
    ]:
        with pytest.raises(psycopg2.Error) as exc_info:
            cur.execute(sql, args)
        assert exc_info.value.pgcode == 'PMQ01', f"{sql}: expected PMQ01, got {exc_info.value.pgcode}"
        cur.connection.rollback()

def test_sqlstate_codes_pmq02_topic_not_found(cur: psycopg2.extensions.cursor) -> None:
    """publish_message on a non-existent topic raises PMQ02."""
    with pytest.raises(psycopg2.Error) as exc_info:
        cur.execute("SELECT postgremq.publish_message('NoSuchTopic', '{}'::jsonb)")
    assert exc_info.value.pgcode == 'PMQ02', f"expected PMQ02, got {exc_info.value.pgcode}"
    cur.connection.rollback()

def test_sqlstate_codes_pmq03_validation(cur: psycopg2.extensions.cursor) -> None:
    """Validation paths raise PMQ03: negative vt, p_limit<=0, mismatched arrays, malformed names."""
    cur.execute("SELECT postgremq.create_topic('VTopic')")
    cur.execute("SELECT postgremq.create_queue('VQueue', 'VTopic', 0, false)")

    # consume_message: negative vt
    with pytest.raises(psycopg2.Error) as exc_info:
        cur.execute("SELECT * FROM postgremq.consume_message('VQueue', -1, 1)")
    assert exc_info.value.pgcode == 'PMQ03'
    cur.connection.rollback()

    # consume_message: p_limit <= 0
    with pytest.raises(psycopg2.Error) as exc_info:
        cur.execute("SELECT * FROM postgremq.consume_message('VQueue', 30, 0)")
    assert exc_info.value.pgcode == 'PMQ03'
    cur.connection.rollback()

    # set_vt: negative vt
    with pytest.raises(psycopg2.Error) as exc_info:
        cur.execute("SELECT postgremq.set_vt('VQueue', 1, 'tok', -1)")
    assert exc_info.value.pgcode == 'PMQ03'
    cur.connection.rollback()

    # set_vt_batch_multi: mismatched array lengths
    with pytest.raises(psycopg2.Error) as exc_info:
        cur.execute("""
            SELECT * FROM postgremq.set_vt_batch_multi(
                ARRAY['VQueue']::varchar[], ARRAY[1,2]::bigint[],
                ARRAY['a']::varchar[], ARRAY[60]::int[])
        """)
    assert exc_info.value.pgcode == 'PMQ03'
    cur.connection.rollback()

    # create_topic: invalid name
    with pytest.raises(psycopg2.Error) as exc_info:
        cur.execute("SELECT postgremq.create_topic('bad name')")
    assert exc_info.value.pgcode == 'PMQ03'
    cur.connection.rollback()

    # create_queue: invalid name
    with pytest.raises(psycopg2.Error) as exc_info:
        cur.execute("SELECT postgremq.create_queue('bad queue', 'VTopic', 0, false)")
    assert exc_info.value.pgcode == 'PMQ03'
    cur.connection.rollback()

def test_nack_final_attempt_inline_dlq(cur: psycopg2.extensions.cursor) -> None:
    """Final-attempt nack moves the row to DLQ inline (no maintenance call needed)."""
    cur.execute("SELECT postgremq.create_topic('TestTopic')")
    cur.execute("SELECT postgremq.create_queue('TestQueue', 'TestTopic', 2, false)")  # max_attempts=2
    cur.execute("SELECT postgremq.publish_message('TestTopic', '{\"k\":\"v\"}'::jsonb)")

    # First consume + nack: not the final attempt, message resets to pending.
    cur.execute("""
        SELECT message_id, consumer_token FROM postgremq.consume_message('TestQueue', 30)
    """)
    msg_id, token = cur.fetchone()
    cur.execute("SELECT postgremq.nack_message('TestQueue', %s, %s, NOW())", (msg_id, token))

    cur.execute("""
        SELECT status, delivery_attempts FROM postgremq.queue_messages
        WHERE queue_name='TestQueue' AND message_id=%s
    """, (msg_id,))
    row = cur.fetchone()
    assert row['status'] == 'pending'
    assert row['delivery_attempts'] == 1

    # Second consume + nack: this is the final attempt; should retire inline.
    cur.execute("""
        SELECT message_id, consumer_token FROM postgremq.consume_message('TestQueue', 30)
    """)
    msg_id2, token2 = cur.fetchone()
    assert msg_id2 == msg_id

    cur.execute("SELECT postgremq.nack_message('TestQueue', %s, %s, NOW())", (msg_id, token2))

    # Row should be gone from postgremq.queue_messages and present in DLQ — without
    # any maintenance call.
    cur.execute("""
        SELECT 1 FROM postgremq.queue_messages
        WHERE queue_name='TestQueue' AND message_id=%s
    """, (msg_id,))
    assert cur.fetchone() is None

    cur.execute("""
        SELECT retry_count FROM postgremq.dead_letter_queue
        WHERE queue_name='TestQueue' AND message_id=%s
    """, (msg_id,))
    dlq_row = cur.fetchone()
    assert dlq_row is not None
    assert dlq_row['retry_count'] == 2

def test_nack_non_final_does_not_dlq(cur: psycopg2.extensions.cursor) -> None:
    """A non-final nack must not retire the message to DLQ even if retries remain."""
    cur.execute("SELECT postgremq.create_topic('TestTopic')")
    cur.execute("SELECT postgremq.create_queue('TestQueue', 'TestTopic', 5, false)")
    cur.execute("SELECT postgremq.publish_message('TestTopic', '{\"k\":\"v\"}'::jsonb)")

    cur.execute("SELECT message_id, consumer_token FROM postgremq.consume_message('TestQueue', 30)")
    msg_id, token = cur.fetchone()
    cur.execute("SELECT postgremq.nack_message('TestQueue', %s, %s, NOW())", (msg_id, token))

    cur.execute("""
        SELECT status FROM postgremq.queue_messages
        WHERE queue_name='TestQueue' AND message_id=%s
    """, (msg_id,))
    assert cur.fetchone()['status'] == 'pending'

    cur.execute("""
        SELECT 1 FROM postgremq.dead_letter_queue
        WHERE queue_name='TestQueue' AND message_id=%s
    """, (msg_id,))
    assert cur.fetchone() is None

def test_unlimited_attempts_never_dlq_inline(cur: psycopg2.extensions.cursor) -> None:
    """When max_delivery_attempts=0 (unlimited), nack must never retire inline."""
    cur.execute("SELECT postgremq.create_topic('TestTopic')")
    cur.execute("SELECT postgremq.create_queue('TestQueue', 'TestTopic', 0, false)")
    cur.execute("SELECT postgremq.publish_message('TestTopic', '{\"k\":\"v\"}'::jsonb)")

    for _ in range(5):
        cur.execute("SELECT message_id, consumer_token FROM postgremq.consume_message('TestQueue', 30)")
        msg_id, token = cur.fetchone()
        cur.execute("SELECT postgremq.nack_message('TestQueue', %s, %s, NOW())", (msg_id, token))

    cur.execute("SELECT count(*) FROM postgremq.dead_letter_queue WHERE queue_name='TestQueue'")
    assert cur.fetchone()[0] == 0

def parse_timestamp(ts_str: str) -> datetime:
    """Parse timestamp from notification."""
    return datetime.fromisoformat(ts_str.replace('Z', '+00:00'))

def test_get_next_visible_time(cur: psycopg2.extensions.cursor) -> None:
    """Test getting next message visibility time."""
    cur.execute("SELECT postgremq.create_topic('TestTopic')")
    cur.execute("SELECT postgremq.create_queue('TestQueue', 'TestTopic', 2, false)")
    
    # When no messages, should return NULL
    cur.execute("SELECT postgremq.get_next_visible_time('TestQueue')")
    assert cur.fetchone()[0] is None
    
    # Publish message with 2 second delay
    delay_time = datetime.now(pytz.UTC) + timedelta(seconds=2)
    cur.execute("""
        SELECT postgremq.publish_message('TestTopic', '{"test":"data"}'::jsonb, %s)
    """, (delay_time,))
    
    # Should return the delay time
    cur.execute("SELECT postgremq.get_next_visible_time('TestQueue')")
    next_time = cur.fetchone()[0]
    assert abs(next_time - delay_time) < timedelta(milliseconds=100)
    
    # Publish immediate message
    cur.execute("""
        SELECT postgremq.publish_message('TestTopic', '{"test":"immediate"}'::jsonb)
    """)
    
    # Should return current time (immediate message)
    cur.execute("SELECT postgremq.get_next_visible_time('TestQueue')")
    next_time = cur.fetchone()[0]
    assert abs(next_time - datetime.now(pytz.UTC)) < timedelta(seconds=1)
    
    # Consume one message
    cur.execute("SELECT * FROM postgremq.consume_message('TestQueue', 30)")
    msg = cur.fetchone()
    
    # Should still return delay_time for the delayed message
    cur.execute("SELECT postgremq.get_next_visible_time('TestQueue')")
    next_time = cur.fetchone()[0]
    assert abs(next_time - delay_time) < timedelta(milliseconds=100)

def test_consume_message_published_at(cur: psycopg2.extensions.cursor) -> None:
    """Test that consume_message returns published_at timestamp."""
    cur.execute("SELECT postgremq.create_topic('TestTopic')")
    cur.execute("SELECT postgremq.create_queue('TestQueue', 'TestTopic', 2, false)")
    
    # Publish a message and record approximate time
    before_publish = datetime.now(pytz.UTC)
    cur.execute("SELECT postgremq.publish_message('TestTopic', '{\"test\":\"data\"}'::jsonb)")
    after_publish = datetime.now(pytz.UTC)
    
    # Consume the message
    cur.execute("SELECT * FROM postgremq.consume_message('TestQueue', 30)")
    msg = cur.fetchone()
    
    # Verify published_at is set and within the expected timeframe
    assert msg['published_at'] is not None
    assert abs(msg['published_at'] - before_publish) < timedelta(milliseconds=100)
    assert abs(msg['published_at'] - after_publish) < timedelta(milliseconds=100)

def test_cascade_behavior(cur: psycopg2.extensions.cursor) -> None:
    """Test that CASCADE relationships work correctly between tables."""
    # Setup - Create test data
    cur.execute("SELECT postgremq.create_topic('test_cascade_topic')")
    cur.execute("SELECT postgremq.create_queue('test_cascade_queue', 'test_cascade_topic', 3, false)")
    
    # Publish 3 test messages
    for i in range(1, 4):
        query = f"SELECT postgremq.publish_message('test_cascade_topic', '{{\"test\": \"cascade{i}\"}}'::jsonb)"
        cur.execute(query)
    
    # Verify initial state - should be 3 messages in the queue
    cur.execute("SELECT COUNT(*) FROM postgremq.list_messages('test_cascade_queue')")
    assert cur.fetchone()[0] == 3, "Should have 3 messages initially"
    
    # Get one message ID to delete directly
    cur.execute("SELECT message_id FROM postgremq.queue_messages WHERE queue_name = 'test_cascade_queue' LIMIT 1")
    message_id = cur.fetchone()[0]
    
    # Test 1: Delete a message directly - should cascade to queue_messages
    cur.execute("DELETE FROM postgremq.messages WHERE id = %s", (message_id,))
    
    # Verify message is gone from postgremq.queue_messages too
    cur.execute("SELECT COUNT(*) FROM postgremq.queue_messages WHERE message_id = %s", (message_id,))
    assert cur.fetchone()[0] == 0, "Message reference should be deleted from postgremq.queue_messages"
    
    # Verify we now have 2 messages in the queue
    cur.execute("SELECT COUNT(*) FROM postgremq.list_messages('test_cascade_queue')")
    assert cur.fetchone()[0] == 2, "Should have 2 messages after direct deletion"
    
    # Test 2: Clean up topic - should cascade to queue_messages
    cur.execute("SELECT postgremq.clean_up_topic('test_cascade_topic')")
    
    # Verify all messages are gone
    cur.execute("SELECT COUNT(*) FROM postgremq.list_messages('test_cascade_queue')")
    assert cur.fetchone()[0] == 0, "No messages should remain after topic cleanup"
    
    # Create new messages for test 3
    for i in range(1, 4):
        query = f"SELECT postgremq.publish_message('test_cascade_topic', '{{\"test\": \"cascade_test3_{i}\"}}'::jsonb)"
        cur.execute(query)
    
    # Test 3: Delete topic - should cascade to queues, messages, and queue_messages
    cur.execute("DELETE FROM postgremq.topics WHERE name = 'test_cascade_topic'")
    
    # Verify queue is gone
    cur.execute("SELECT COUNT(*) FROM postgremq.queues WHERE name = 'test_cascade_queue'")
    assert cur.fetchone()[0] == 0, "Queue should be deleted when topic is deleted"
    
    # Verify no messages remain for this topic
    cur.execute("SELECT COUNT(*) FROM postgremq.messages WHERE topic_name = 'test_cascade_topic'")
    assert cur.fetchone()[0] == 0, "Messages should be deleted when topic is deleted"
    
    # Verify no queue_messages entries remain
    cur.execute("SELECT COUNT(*) FROM postgremq.queue_messages WHERE queue_name = 'test_cascade_queue'")
    assert cur.fetchone()[0] == 0, "Queue message entries should be deleted when topic is deleted"

def test_delete_inactive_queues_edge_cases(cur: psycopg2.extensions.cursor) -> None:
    """Test edge cases for delete_inactive_queues function."""
    # Test 1: Empty database
    cur.execute("SELECT postgremq.delete_inactive_queues()")
    # Should complete without error
    
    # Test 2: Queue with NULL keep_alive_until
    cur.execute("SELECT postgremq.create_topic('null_ka_topic')")
    cur.execute("SELECT postgremq.create_queue('null_ka_queue', 'null_ka_topic', 3, true)")
    
    # Manually set keep_alive_until to NULL
    cur.execute("UPDATE postgremq.queues SET keep_alive_until = NULL WHERE name = 'null_ka_queue'")
    
    # Verify it's NULL
    cur.execute("SELECT keep_alive_until FROM postgremq.queues WHERE name = 'null_ka_queue'")
    assert cur.fetchone()[0] is None
    
    # Delete inactive queues
    cur.execute("SELECT postgremq.delete_inactive_queues()")
    
    # Verify the queue was deleted (should be, as it's exclusive with NULL keep_alive_until)
    cur.execute("SELECT COUNT(*) FROM postgremq.queues WHERE name = 'null_ka_queue'")
    assert cur.fetchone()[0] == 0
    
    # Clean up
    cur.execute("DELETE FROM postgremq.topics WHERE name = 'null_ka_topic'")

def test_get_next_visible_time_multiple_queues(cur: psycopg2.extensions.cursor) -> None:
    """Test get_next_visible_time with multiple queues."""
    # Create separate topics for each queue to avoid cross-routing of messages
    cur.execute("SELECT postgremq.create_topic('MultiQTopic1')")
    cur.execute("SELECT postgremq.create_topic('MultiQTopic2')")
    cur.execute("SELECT postgremq.create_queue('Queue1', 'MultiQTopic1', 2, false)")
    cur.execute("SELECT postgremq.create_queue('Queue2', 'MultiQTopic2', 2, false)")
    
    # Add a message to Queue1 with a 5-second delay
    delay1 = datetime.now(pytz.UTC) + timedelta(seconds=5)
    cur.execute("""
        SELECT postgremq.publish_message('MultiQTopic1', '{"test":"queue1"}'::jsonb, %s)
    """, (delay1,))
    
    # Add a message to Queue2 with a 2-second delay
    delay2 = datetime.now(pytz.UTC) + timedelta(seconds=2)
    cur.execute("""
        SELECT postgremq.publish_message('MultiQTopic2', '{"test":"queue2"}'::jsonb, %s)
    """, (delay2,))
    
    # Check both queues
    cur.execute("SELECT postgremq.get_next_visible_time('Queue1')")
    next_time1 = cur.fetchone()[0]
    
    cur.execute("SELECT postgremq.get_next_visible_time('Queue2')")
    next_time2 = cur.fetchone()[0]
    
    # Check that next_time2 is earlier than next_time1
    # We don't compare to the exact delays to avoid test flakiness
    assert next_time2 < next_time1, "Queue2 should have an earlier next visible time than Queue1"
    
    # Clean up
    cur.execute("DELETE FROM postgremq.queues WHERE name IN ('Queue1', 'Queue2')")
    cur.execute("DELETE FROM postgremq.topics WHERE name IN ('MultiQTopic1', 'MultiQTopic2')")

def test_cleanup_functions_edge_cases(cur: psycopg2.extensions.cursor) -> None:
    """Test edge cases for cleanup functions."""
    # Setup
    cur.execute("SELECT postgremq.create_topic('CleanupTopic')")
    cur.execute("SELECT postgremq.create_queue('CleanupQueue', 'CleanupTopic', 3, false)")
    
    # Test 1: Clean up empty topic/queue
    cur.execute("SELECT postgremq.clean_up_queue('CleanupQueue')")
    cur.execute("SELECT postgremq.clean_up_topic('CleanupTopic')")
    
    # Test 2: Clean up non-existent queue/topic - should execute without errors
    cur.execute("SELECT postgremq.clean_up_queue('NonExistentQueue')")
    cur.execute("SELECT postgremq.clean_up_topic('NonExistentTopic')")
    
    # Test 3: Publish messages, then clean up
    for i in range(3):
        cur.execute(
            "SELECT postgremq.publish_message('CleanupTopic', %s::jsonb)",
            (json.dumps({"test": f"cleanup-{i}"}),)
        )
    
    # Consume one message to have mixed statuses
    cur.execute("SELECT * FROM postgremq.consume_message('CleanupQueue', 30, 1)")
    
    # Clean up the queue
    cur.execute("SELECT postgremq.clean_up_queue('CleanupQueue')")
    
    # Verify all messages are gone from queue
    cur.execute("SELECT COUNT(*) FROM postgremq.queue_messages WHERE queue_name = 'CleanupQueue'")
    assert cur.fetchone()[0] == 0
    
    # Clean up the topic
    cur.execute("SELECT postgremq.clean_up_topic('CleanupTopic')")
    
    # Verify all messages are gone from the topic
    cur.execute("SELECT COUNT(*) FROM postgremq.messages WHERE topic_name = 'CleanupTopic'")
    assert cur.fetchone()[0] == 0
    
    # Clean up
    cur.execute("DELETE FROM postgremq.queues WHERE name = 'CleanupQueue'")
    cur.execute("DELETE FROM postgremq.topics WHERE name = 'CleanupTopic'")

def test_cleanup_completed_messages(cur: psycopg2.extensions.cursor) -> None:
    """Test cleanup_completed_messages removes only stale completed entries."""
    cur.execute("SELECT postgremq.create_topic('CleanupRetentionTopic')")
    cur.execute("SELECT postgremq.create_queue('CleanupRetentionQueue', 'CleanupRetentionTopic', 3, false)")
    
    for i in range(2):
        cur.execute(
            "SELECT postgremq.publish_message('CleanupRetentionTopic', %s::jsonb)",
            (json.dumps({"test": f"cleanup-retention-{i}"}),)
        )
    
    cur.execute("""
        SELECT message_id, consumer_token
        FROM postgremq.consume_message('CleanupRetentionQueue', 30, 2)
    """)
    messages = cur.fetchall()
    assert len(messages) == 2, "Should consume both messages"
    
    for msg in messages:
        cur.execute(
            "SELECT postgremq.ack_message('CleanupRetentionQueue', %s, %s)",
            (msg['message_id'], msg['consumer_token'])
        )
    
    older_id = messages[0]['message_id']
    newer_id = messages[1]['message_id']
    
    cur.execute("""
        UPDATE postgremq.queue_messages
        SET processed_at = NOW() - interval '2 hours'
        WHERE queue_name = 'CleanupRetentionQueue' AND message_id = %s
    """, (older_id,))
    cur.execute("""
        UPDATE postgremq.queue_messages
        SET processed_at = NOW() - interval '30 minutes'
        WHERE queue_name = 'CleanupRetentionQueue' AND message_id = %s
    """, (newer_id,))
    
    cur.execute("SELECT postgremq.cleanup_completed_messages(1)")
    deleted = cur.fetchone()[0]
    assert deleted == 1, "Exactly one completed message should be removed"
    
    cur.execute("""
        SELECT COUNT(*)
        FROM postgremq.queue_messages
        WHERE queue_name = 'CleanupRetentionQueue' AND message_id = %s
    """, (older_id,))
    assert cur.fetchone()[0] == 0, "Old completed message should be deleted"
    
    cur.execute("""
        SELECT status
        FROM postgremq.queue_messages
        WHERE queue_name = 'CleanupRetentionQueue' AND message_id = %s
    """, (newer_id,))
    assert cur.fetchone()[0] == 'completed', "Recent completed message should remain"
    
    cur.execute("""
        UPDATE postgremq.queue_messages
        SET processed_at = NOW() - interval '2 days'
        WHERE queue_name = 'CleanupRetentionQueue' AND message_id = %s
    """, (newer_id,))
    
    cur.execute("SELECT postgremq.cleanup_completed_messages()")
    deleted_second = cur.fetchone()[0]
    assert deleted_second == 1, "Default retention should remove stale completed entries"
    
    cur.execute("""
        SELECT COUNT(*)
        FROM postgremq.queue_messages
        WHERE queue_name = 'CleanupRetentionQueue'
    """)
    assert cur.fetchone()[0] == 0, "No queue entries should remain after cleanup"
    
    cur.execute("SELECT postgremq.clean_up_topic('CleanupRetentionTopic')")
    cur.execute("SELECT postgremq.delete_queue('CleanupRetentionQueue')")
    cur.execute("SELECT postgremq.delete_topic('CleanupRetentionTopic')")

def test_purge_all_messages_states(cur: psycopg2.extensions.cursor) -> None:
    """Test purge_all_messages with messages in different states."""
    # Setup multiple topics and queues
    cur.execute("SELECT postgremq.create_topic('PurgeTopic1')")
    cur.execute("SELECT postgremq.create_topic('PurgeTopic2')")
    cur.execute("SELECT postgremq.create_queue('PurgeQueue1', 'PurgeTopic1', 3, false)")
    cur.execute("SELECT postgremq.create_queue('PurgeQueue2', 'PurgeTopic2', 3, false)")
    
    # Publish messages to both topics
    for i in range(3):
        cur.execute(
            "SELECT postgremq.publish_message('PurgeTopic1', %s::jsonb)",
            (json.dumps({"test": f"purge1-{i}"}),)
        )
        cur.execute(
            "SELECT postgremq.publish_message('PurgeTopic2', %s::jsonb)",
            (json.dumps({"test": f"purge2-{i}"}),)
        )
    
    # Consume some messages to get them in different states
    cur.execute("SELECT * FROM postgremq.consume_message('PurgeQueue1', 30, 2)")
    msg = cur.fetchone()
    if msg:
        # Complete one message
        cur.execute("""
            SELECT postgremq.ack_message('PurgeQueue1', %s, %s)
        """, (msg['message_id'], msg['consumer_token']))
    
    # Nack one message from queue2 with delay
    cur.execute("SELECT * FROM postgremq.consume_message('PurgeQueue2', 30, 1)")
    msg = cur.fetchone()
    if msg:
        delay_time = datetime.now(pytz.UTC) + timedelta(seconds=10)
        cur.execute("""
            SELECT postgremq.nack_message('PurgeQueue2', %s, %s, %s)
        """, (msg['message_id'], msg['consumer_token'], delay_time))
    
    # Verify initial message counts
    cur.execute("SELECT COUNT(*) FROM postgremq.messages")
    initial_msg_count = cur.fetchone()[0]
    assert initial_msg_count > 0
    
    # Purge all messages
    cur.execute("SELECT postgremq.purge_all_messages()")
    
    # Verify all messages are gone
    cur.execute("SELECT COUNT(*) FROM postgremq.messages")
    assert cur.fetchone()[0] == 0
    
    cur.execute("SELECT COUNT(*) FROM postgremq.queue_messages")
    assert cur.fetchone()[0] == 0
    
    # Clean up
    cur.execute("DELETE FROM postgremq.queues WHERE name IN ('PurgeQueue1', 'PurgeQueue2')")
    cur.execute("DELETE FROM postgremq.topics WHERE name IN ('PurgeTopic1', 'PurgeTopic2')")

def test_complex_cascade_behavior(cur: psycopg2.extensions.cursor) -> None:
    """Test cascade behavior with complex relationships between topics, queues, and messages."""
    # Setup - create multiple topics and queues
    topics = ['CascadeTopic1', 'CascadeTopic2']
    for topic in topics:
        cur.execute(f"SELECT postgremq.create_topic('{topic}')")
    
    # Create 2 queues per topic (4 total)
    queues = []
    for topic in topics:
        for i in range(1, 3):
            queue_name = f"{topic}_Queue{i}"
            cur.execute(f"SELECT postgremq.create_queue('{queue_name}', '{topic}', 3, false)")
            queues.append(queue_name)
    
    # Publish multiple messages to each topic
    msg_ids = []
    for topic in topics:
        for i in range(3):
            cur.execute(
                "SELECT postgremq.publish_message(%s, %s::jsonb)",
                (topic, json.dumps({"test": f"{topic}-msg{i}"})))
            cur.execute("SELECT lastval()")
            msg_ids.append(cur.fetchone()[0])
    
    # Consume some messages from each queue
    for queue in queues:
        cur.execute(f"SELECT * FROM postgremq.consume_message('{queue}', 30, 1)")
        msg = cur.fetchone()
        if msg:
            # Complete some, leave others in processing state
            if queue.endswith("Queue1"):
                cur.execute(
                    "SELECT postgremq.ack_message(%s, %s, %s)",
                    (queue, msg['message_id'], msg['consumer_token'])
                )
    
    # Test 1: Delete one queue and verify its queue_messages are gone
    test_queue = queues[0]
    cur.execute(f"SELECT postgremq.delete_queue('{test_queue}')")
    
    cur.execute(f"SELECT COUNT(*) FROM postgremq.queue_messages WHERE queue_name = '{test_queue}'")
    assert cur.fetchone()[0] == 0
    
    # Test 2: Delete one topic and verify cascade effects
    test_topic = topics[0]
    
    # Get counts before
    cur.execute(f"SELECT COUNT(*) FROM postgremq.messages WHERE topic_name = '{test_topic}'")
    topic_msg_count = cur.fetchone()[0]
    assert topic_msg_count > 0
    
    topic_queues = [q for q in queues if q.startswith(test_topic)]
    queue_msg_count = 0
    for queue in topic_queues:
        cur.execute(f"SELECT COUNT(*) FROM postgremq.queue_messages WHERE queue_name = '{queue}'")
        queue_msg_count += cur.fetchone()[0]
    
    # Delete the topic
    # First, clean up all messages for this topic 
    cur.execute(f"DELETE FROM postgremq.messages WHERE topic_name = '{test_topic}'")
    
    # Now we can delete the topic
    cur.execute(f"SELECT postgremq.delete_topic('{test_topic}')")
    
    # Verify topic is gone
    cur.execute(f"SELECT COUNT(*) FROM postgremq.messages WHERE topic_name = '{test_topic}'")
    assert cur.fetchone()[0] == 0
    
    for queue in topic_queues:
        cur.execute(f"SELECT COUNT(*) FROM postgremq.queues WHERE name = '{queue}'")
        assert cur.fetchone()[0] == 0
        
        cur.execute(f"SELECT COUNT(*) FROM postgremq.queue_messages WHERE queue_name = '{queue}'")
        assert cur.fetchone()[0] == 0
    
    # Clean up
    # Delete messages for remaining topics
    for topic in topics:
        if topic != test_topic:  # We already deleted messages for test_topic
            cur.execute(f"DELETE FROM postgremq.messages WHERE topic_name = '{topic}'")
            cur.execute(f"SELECT postgremq.delete_topic('{topic}')")

def test_nack_message_delays(cur: psycopg2.extensions.cursor) -> None:
    """Test nack_message with different delay strategies."""
    # Setup
    cur.execute("SELECT postgremq.create_topic('NackTopic')")
    cur.execute("SELECT postgremq.create_queue('NackQueue', 'NackTopic', 3, false)")
    
    # Publish a series of messages
    for i in range(4):
        cur.execute(
            "SELECT postgremq.publish_message('NackTopic', %s::jsonb)",
            (json.dumps({"test": f"nack-{i}"}),)
        )
    
    # Consume all messages
    cur.execute("SELECT * FROM postgremq.consume_message('NackQueue', 30, 4)")
    messages = cur.fetchall()
    
    # Test different nack delay strategies
    # 1. Immediate nack
    msg1 = messages[0]
    cur.execute("""
        SELECT postgremq.nack_message('NackQueue', %s, %s)
    """, (msg1['message_id'], msg1['consumer_token']))
    
    # 2. Short delay (1 second)
    msg2 = messages[1]
    short_delay = datetime.now(pytz.UTC) + timedelta(seconds=1)
    cur.execute("""
        SELECT postgremq.nack_message('NackQueue', %s, %s, %s)
    """, (msg2['message_id'], msg2['consumer_token'], short_delay))
    
    # 3. Medium delay (3 seconds)
    msg3 = messages[2]
    medium_delay = datetime.now(pytz.UTC) + timedelta(seconds=3)
    cur.execute("""
        SELECT postgremq.nack_message('NackQueue', %s, %s, %s)
    """, (msg3['message_id'], msg3['consumer_token'], medium_delay))
    
    # 4. Long delay (5 seconds)
    msg4 = messages[3]
    long_delay = datetime.now(pytz.UTC) + timedelta(seconds=5)
    cur.execute("""
        SELECT postgremq.nack_message('NackQueue', %s, %s, %s)
    """, (msg4['message_id'], msg4['consumer_token'], long_delay))
    
    # Test immediate message availability
    cur.execute("SELECT * FROM postgremq.consume_message('NackQueue', 30, 1)")
    immediately_available = cur.fetchone()
    assert immediately_available is not None
    assert immediately_available['message_id'] == msg1['message_id']
    
    # Wait for 1.1 seconds and check for next message
    time.sleep(1.1)
    cur.execute("SELECT * FROM postgremq.consume_message('NackQueue', 30, 1)")
    short_delay_msg = cur.fetchone()
    assert short_delay_msg is not None
    assert short_delay_msg['message_id'] == msg2['message_id']
    
    # Wait for 2 more seconds and check
    time.sleep(2)
    cur.execute("SELECT * FROM postgremq.consume_message('NackQueue', 30, 1)")
    medium_delay_msg = cur.fetchone()
    assert medium_delay_msg is not None
    assert medium_delay_msg['message_id'] == msg3['message_id']
    
    # Wait final 2 seconds
    time.sleep(2)
    cur.execute("SELECT * FROM postgremq.consume_message('NackQueue', 30, 1)")
    long_delay_msg = cur.fetchone()
    assert long_delay_msg is not None
    assert long_delay_msg['message_id'] == msg4['message_id']
    
    # Clean up
    cur.execute("DELETE FROM postgremq.queues WHERE name = 'NackQueue'")
    cur.execute("DELETE FROM postgremq.topics WHERE name = 'NackTopic'")

def test_release_message(cur: psycopg2.extensions.cursor) -> None:
    """Test release_message function for explicitly releasing a message without redelivery."""
    # Setup
    cur.execute("SELECT postgremq.create_topic('ReleaseTopic')")
    cur.execute("SELECT postgremq.create_queue('ReleaseQueue', 'ReleaseTopic', 3, false)")
    
    # Publish a message
    cur.execute("SELECT postgremq.publish_message('ReleaseTopic', '{\"test\": \"release\"}'::jsonb)")
    
    # Consume the message
    cur.execute("""
        SELECT message_id, consumer_token
        FROM postgremq.consume_message('ReleaseQueue', 30)
    """)
    result = cur.fetchone()
    assert result is not None
    msg_id, token = result['message_id'], result['consumer_token']
    
    # Verify message is in processing state
    cur.execute("""
        SELECT status
        FROM postgremq.queue_messages
        WHERE queue_name = 'ReleaseQueue' AND message_id = %s
    """, (msg_id,))
    assert cur.fetchone()[0] == 'processing'
    
    # Release the message - returns void, no result to assert
    cur.execute("""
        SELECT postgremq.release_message('ReleaseQueue', %s, %s)
    """, (msg_id, token))
    
    # Verify message is back to pending state
    cur.execute("""
        SELECT status, delivery_attempts
        FROM postgremq.queue_messages
        WHERE queue_name = 'ReleaseQueue' AND message_id = %s
    """, (msg_id,))
    row = cur.fetchone()
    assert row[0] == 'pending', "Message should be back in pending state"
    # Should be decremented by 1, so it's back to 0
    assert row[1] == 0, "Delivery attempts should be reset to 0"
    
    # Message should be immediately available for consumption again
    cur.execute("""
        SELECT message_id
        FROM postgremq.consume_message('ReleaseQueue', 30)
    """)
    assert cur.fetchone()[0] == msg_id, "Message should be available for consumption again"
    
    # Test with invalid consumer token
    with pytest.raises(Exception):
        cur.execute("""
            SELECT postgremq.release_message('ReleaseQueue', %s, 'invalid-token')
        """, (msg_id,))

def test_list_topics(cur: psycopg2.extensions.cursor) -> None:
    """Test list_topics function returns all created topics."""
    # Clean up any existing topics first
    cur.execute("SELECT name FROM postgremq.topics")
    existing_topics = [row[0] for row in cur.fetchall()]
    for topic in existing_topics:
        cur.execute("SELECT postgremq.clean_up_topic(%s)", (topic,))
        cur.execute("SELECT postgremq.delete_topic(%s)", (topic,))
    
    # Verify no topics exist
    cur.execute("SELECT COUNT(*) FROM postgremq.list_topics()")
    assert cur.fetchone()[0] == 0, "Should start with no topics"
    
    # Create test topics
    test_topics = ['ListTopicA', 'ListTopicB', 'ListTopicC']
    for topic in test_topics:
        cur.execute(f"SELECT postgremq.create_topic('{topic}')")
    
    # Test list_topics returns all topics - column is named "topic" not "name"
    cur.execute("SELECT topic FROM postgremq.list_topics() ORDER BY topic")
    topics = [row[0] for row in cur.fetchall()]
    assert topics == sorted(test_topics), "list_topics should return all created topics"
    
    # Clean up
    for topic in test_topics:
        cur.execute(f"SELECT postgremq.delete_topic('{topic}')")

def test_list_queues(cur: psycopg2.extensions.cursor) -> None:
    """Test list_queues function returns all created queues."""
    # Setup - create topics and queues
    cur.execute("SELECT postgremq.create_topic('ListQueueTopic1')")
    cur.execute("SELECT postgremq.create_topic('ListQueueTopic2')")
    
    # Create multiple queues for each topic
    queues = [
        ('Queue1A', 'ListQueueTopic1', 2, False),
        ('Queue1B', 'ListQueueTopic1', 3, True),
        ('Queue2A', 'ListQueueTopic2', 0, False),
        ('Queue2B', 'ListQueueTopic2', 1, True)
    ]
    
    for name, topic, attempts, exclusive in queues:
        cur.execute(f"""
            SELECT postgremq.create_queue('{name}', '{topic}', {attempts}, {exclusive})
        """)
    
    # Test list_queues returns all queues - correct column names from function definition
    cur.execute("SELECT queue_name, topic_name, max_delivery_attempts, exclusive FROM postgremq.list_queues() ORDER BY queue_name")
    result = cur.fetchall()
    assert len(result) >= len(queues), "Should return at least our test queues"
    
    # Verify our specific queues are in the results
    for q in queues:
        found = False
        for row in result:
            if row[0] == q[0]:  # Match by queue name
                found = True
                assert row[1] == q[1], f"Topic mismatch for queue {q[0]}"
                assert row[2] == q[2], f"Max delivery attempts mismatch for queue {q[0]}"
                assert row[3] == q[3], f"Exclusive flag mismatch for queue {q[0]}"
        
        assert found, f"Queue {q[0]} not found in results"
    
    # Clean up
    for name, topic, _, _ in queues:
        cur.execute(f"SELECT postgremq.delete_queue('{name}')")
    
    cur.execute("SELECT postgremq.delete_topic('ListQueueTopic1')")
    cur.execute("SELECT postgremq.delete_topic('ListQueueTopic2')")

def test_get_queue_statistics(cur: psycopg2.extensions.cursor) -> None:
    """Test get_queue_statistics function returns correct statistics for queues."""
    # Setup - create topic and queue
    cur.execute("SELECT postgremq.create_topic('StatsTopic')")
    cur.execute("SELECT postgremq.create_queue('StatsQueue', 'StatsTopic', 3, false)")
    
    # Publish messages
    for i in range(5):
        cur.execute(
            "SELECT postgremq.publish_message('StatsTopic', %s::jsonb)",
            (json.dumps({"test": f"stats-{i}"}),)
        )
    
    # Consume some messages to get different states
    cur.execute("SELECT * FROM postgremq.consume_message('StatsQueue', 30, 2)")
    messages = cur.fetchall()
    
    # Complete one message
    if len(messages) > 0:
        cur.execute("""
            SELECT postgremq.ack_message('StatsQueue', %s, %s)
        """, (messages[0]['message_id'], messages[0]['consumer_token']))
    
    # Get statistics for this queue - uses correct column names from SQL function
    cur.execute("SELECT pending_count, processing_count, completed_count, total_count FROM postgremq.get_queue_statistics('StatsQueue')")
    stats = cur.fetchone()
    assert stats is not None
    
    # Verify the statistics - column names match the SQL function definition
    assert stats[0] == 3, "Should have 3 pending messages"
    assert stats[1] == 1, "Should have 1 processing message"
    assert stats[2] == 1, "Should have 1 completed message"
    assert stats[3] == 5, "Should have 5 total messages"
    
    # Test getting statistics for all queues
    cur.execute("SELECT pending_count, processing_count, completed_count, total_count FROM postgremq.get_queue_statistics()")
    all_stats = cur.fetchone()
    assert all_stats is not None, "Should return statistics for all queues"
    assert all_stats[3] >= 5, "Should include at least our test messages in total"
    
    # Clean up - make sure to clean up the topic before deleting it
    cur.execute("SELECT postgremq.clean_up_queue('StatsQueue')")
    cur.execute("SELECT postgremq.delete_queue('StatsQueue')")
    cur.execute("SELECT postgremq.clean_up_topic('StatsTopic')")
    cur.execute("SELECT postgremq.delete_topic('StatsTopic')")

def test_list_dlq_messages(cur: psycopg2.extensions.cursor) -> None:
    """Test list_dlq_messages function returns messages in the dead letter queue."""
    # Setup - create topic and queue
    cur.execute("SELECT postgremq.create_topic('DLQListTopic')")
    cur.execute("SELECT postgremq.create_queue('DLQListQueue', 'DLQListTopic', 1, false)")  # Only 1 retry
    
    # Publish messages
    for i in range(3):
        cur.execute(
            "SELECT postgremq.publish_message('DLQListTopic', %s::jsonb)",
            (json.dumps({"test": f"dlq-{i}"}),)
        )
    
    # Consume and nack all messages to exceed max delivery attempts
    for _ in range(2):  # Need to consume and nack twice to exceed max_delivery_attempts
        cur.execute("SELECT * FROM postgremq.consume_message('DLQListQueue', 30, 3)")
        messages = cur.fetchall()
        
        for msg in messages:
            cur.execute("""
                SELECT postgremq.nack_message('DLQListQueue', %s, %s)
            """, (msg['message_id'], msg['consumer_token']))
    
    # Move to DLQ
    cur.execute("SELECT * FROM postgremq.pmq_maintenance_fast()")
    
    # Test list_dlq_messages with column names from SQL function
    cur.execute("SELECT queue_name, message_id, retry_count, published_at FROM postgremq.list_dlq_messages()")
    dlq_messages = cur.fetchall()
    
    assert len(dlq_messages) == 3, "All 3 messages should be in DLQ"
    
    # Verify message properties
    for msg in dlq_messages:
        assert msg[0] == 'DLQListQueue'  # queue_name
        assert msg[1] is not None  # message_id
        assert msg[2] == 1, "retry_count should be the max attempts (1)"
        assert msg[3] is not None  # published_at
    
    # Clean up - purge DLQ, clean up queue and topic before deletion
    cur.execute("SELECT postgremq.purge_dlq()")
    cur.execute("SELECT postgremq.clean_up_queue('DLQListQueue')")
    cur.execute("SELECT postgremq.delete_queue('DLQListQueue')")
    cur.execute("SELECT postgremq.clean_up_topic('DLQListTopic')")
    cur.execute("SELECT postgremq.delete_topic('DLQListTopic')")

def test_purge_dlq(cur: psycopg2.extensions.cursor) -> None:
    """Test purge_dlq function removes all messages from the dead letter queue."""
    # Setup - create topic and queues
    cur.execute("SELECT postgremq.create_topic('PurgeDLQTopic')")
    cur.execute("SELECT postgremq.create_queue('PurgeDLQQueue1', 'PurgeDLQTopic', 1, false)")
    
    # Publish messages to one queue
    for i in range(2):
        cur.execute(
            "SELECT postgremq.publish_message('PurgeDLQTopic', %s::jsonb)",
            (json.dumps({"test": f"purge-q1-{i}"}),)
        )
    
    # Consume and nack messages to exceed max delivery attempts
    for _ in range(2):  # Need to consume and nack twice to exceed max_delivery_attempts
        cur.execute("SELECT * FROM postgremq.consume_message('PurgeDLQQueue1', 30, 2)")
        messages = cur.fetchall()
        
        for msg in messages:
            cur.execute("""
                SELECT postgremq.nack_message('PurgeDLQQueue1', %s, %s)
            """, (msg['message_id'], msg['consumer_token']))
    
    # Move to DLQ
    cur.execute("SELECT * FROM postgremq.pmq_maintenance_fast()")
    
    # Verify messages are in DLQ
    cur.execute("SELECT COUNT(*) FROM postgremq.list_dlq_messages()")
    assert cur.fetchone()[0] == 2, "Should have 2 messages in DLQ"
    
    # Test purge_dlq
    cur.execute("SELECT postgremq.purge_dlq()")
    
    # Verify DLQ is empty
    cur.execute("SELECT COUNT(*) FROM postgremq.list_dlq_messages()")
    assert cur.fetchone()[0] == 0, "DLQ should be empty after purge"
    
    # Clean up - make sure to clean up topic before deletion
    cur.execute("SELECT postgremq.clean_up_queue('PurgeDLQQueue1')")
    cur.execute("SELECT postgremq.delete_queue('PurgeDLQQueue1')")
    cur.execute("SELECT postgremq.clean_up_topic('PurgeDLQTopic')")
    cur.execute("SELECT postgremq.delete_topic('PurgeDLQTopic')")

def test_delete_queue_message(cur: psycopg2.extensions.cursor) -> None:
    """Test deleting a specific message from a queue."""
    # Create test topic and queue
    topic = "DeleteMsgTopic"
    queue = "DeleteMsgQueue"
    cur.execute("SELECT postgremq.create_topic(%s)", (topic,))
    cur.execute("SELECT postgremq.create_queue(%s, %s)", (queue, topic))
    
    # Publish messages
    message_ids = []
    for i in range(3):
        cur.execute(
            "SELECT postgremq.publish_message(%s, %s::jsonb)",
            (topic, json.dumps({"test": f"delete-test-{i}"}))
        )
        cur.execute("SELECT lastval()")
        message_ids.append(cur.fetchone()[0])
    
    # Delete the second message
    target_msg_id = message_ids[1]
    cur.execute("SELECT postgremq.delete_queue_message(%s, %s)", (queue, target_msg_id))
    
    # Verify message is deleted
    cur.execute("SELECT COUNT(*) FROM postgremq.queue_messages WHERE queue_name = %s AND message_id = %s", (queue, target_msg_id))
    assert cur.fetchone()[0] == 0, f"Message {target_msg_id} should be deleted"
    
    # Count remaining messages
    cur.execute("SELECT COUNT(*) FROM postgremq.queue_messages WHERE queue_name = %s", (queue,))
    assert cur.fetchone()[0] == 2, "Should have 2 messages remaining"
    
    # Clean up
    cur.execute("SELECT postgremq.clean_up_queue(%s)", (queue,))
    cur.execute("SELECT postgremq.delete_queue(%s)", (queue,))
    cur.execute("SELECT postgremq.clean_up_topic(%s)", (topic,))
    cur.execute("SELECT postgremq.delete_topic(%s)", (topic,))

def test_get_message(cur: psycopg2.extensions.cursor) -> None:
    """Test retrieving a specific message by ID."""
    # Create test topic and queue
    topic = "GetMsgTopic"
    queue = "GetMsgQueue"
    cur.execute("SELECT postgremq.create_topic(%s)", (topic,))
    cur.execute("SELECT postgremq.create_queue(%s, %s)", (queue, topic))
    
    # Publish a message
    test_payload = {"test": "get-message-test", "value": 42}
    cur.execute(
        "SELECT postgremq.publish_message(%s, %s::jsonb)",
        (topic, json.dumps(test_payload))
    )
    cur.execute("SELECT lastval()")
    msg_id = cur.fetchone()[0]
    
    # Get the message by ID
    cur.execute("SELECT message_id, topic_name, payload, published_at FROM postgremq.get_message(%s)", (msg_id,))
    msg = cur.fetchone()
    
    # Verify message properties
    assert msg is not None, "Message should be retrieved"
    assert msg[0] == msg_id, "Message ID should match"
    assert msg[1] == topic, "Topic should match"
    assert msg[2] == test_payload, "Payload should match"
    assert msg[3] is not None, "Published timestamp should exist"
    
    # Clean up
    cur.execute("SELECT postgremq.clean_up_queue(%s)", (queue,))
    cur.execute("SELECT postgremq.delete_queue(%s)", (queue,))
    cur.execute("SELECT postgremq.clean_up_topic(%s)", (topic,))
    cur.execute("SELECT postgremq.delete_topic(%s)", (topic,))


def test_heartbeat_contention_is_local_and_clock_is_fresh(cur, conn, db_config, test_db):
    cur.execute("SELECT postgremq.create_topic('t'); SELECT postgremq.create_queue('q','t')")
    for i in range(2): cur.execute("SELECT postgremq.publish_message('t', %s::jsonb)", [json.dumps(i)])
    cur.execute("SELECT * FROM postgremq.consume_message('q', 10, 2)")
    deliveries = cur.fetchall()
    blocker = psycopg2.connect(**{**db_config, 'dbname':test_db})
    try:
        with blocker.cursor() as locked:
            locked.execute("SELECT 1 FROM postgremq.queue_messages WHERE message_id=%s FOR UPDATE", [deliveries[0]['message_id']])
        with pytest.raises(psycopg2.Error) as busy:
            cur.execute("SELECT postgremq.set_vt('q',%s,%s,10)",[deliveries[0]['message_id'],deliveries[0]['consumer_token']])
        assert busy.value.pgcode == '55P03'
        cur.execute("SET statement_timeout='500ms'")
        cur.execute("SELECT * FROM postgremq.set_vt_batch_multi(%s::varchar[], %s::bigint[], %s::varchar[], %s::int[])",
                    [['q','q'], [m['message_id'] for m in deliveries], [m['consumer_token'] for m in deliveries], [10,10]])
        outcomes = {r['message_id']:r['outcome'] for r in cur.fetchall()}
        assert outcomes == {deliveries[0]['message_id']:'busy', deliveries[1]['message_id']:'extended'}
    finally:
        blocker.rollback(); blocker.close()
        cur.execute("SET statement_timeout=0")
    # An old transaction must not extend a lease that expired after BEGIN.
    cur.execute("BEGIN")
    cur.execute("SELECT now()")
    cur.execute("UPDATE postgremq.queue_messages SET vt=clock_timestamp()-interval '1 ms'")
    cur.execute("SELECT * FROM postgremq.set_vt_batch_multi(%s::varchar[], %s::bigint[], %s::varchar[], %s::int[])",
                [['q'], [deliveries[0]['message_id']], [deliveries[0]['consumer_token']], [10]])
    assert cur.fetchall() == []
    cur.execute("ROLLBACK")


def test_payload_gc_preserves_lagging_queues_and_dlq(cur):
    cur.execute("SELECT postgremq.create_topic('t'); SELECT postgremq.create_queue('fast','t'); SELECT postgremq.create_queue('slow','t',1)")
    cur.execute("SELECT postgremq.publish_message('t','{}')")
    id = cur.fetchone()[0]
    cur.execute("SELECT * FROM postgremq.consume_message('fast',30,1)")
    token = cur.fetchone()['consumer_token']
    cur.execute("SELECT postgremq.ack_message('fast',%s,%s)",[id,token])
    cur.execute("SELECT postgremq.cleanup_completed_messages(0)")
    assert cur.fetchone()[0] == 1
    cur.execute("SELECT count(*) FROM postgremq.messages")
    assert cur.fetchone()[0] == 1
    cur.execute("SELECT * FROM postgremq.consume_message('slow',30,1)")
    token = cur.fetchone()['consumer_token']
    cur.execute("SELECT postgremq.nack_message('slow',%s,%s)",[id,token])
    cur.execute("SELECT postgremq.cleanup_unreferenced_messages(0)")
    assert cur.fetchone()[0] == 0
    cur.execute("SELECT postgremq.requeue_dlq_messages('slow')")
    cur.execute("SELECT postgremq.cleanup_unreferenced_messages(0)")
    assert cur.fetchone()[0] == 0
    cur.execute("SELECT postgremq.clean_up_queue('slow')")
    cur.execute("SELECT postgremq.cleanup_unreferenced_messages(0)")
    assert cur.fetchone()[0] == 1


def test_payload_gc_is_bounded_and_collects_unrouted_messages(cur):
    cur.execute("SELECT postgremq.create_topic('t')")
    for i in range(5): cur.execute("SELECT postgremq.publish_message('t', '{}')")
    cur.execute("SELECT postgremq.cleanup_unreferenced_messages(0,2)")
    assert cur.fetchone()[0] == 2
    cur.execute("SELECT count(*) FROM postgremq.messages")
    assert cur.fetchone()[0] == 3


def test_old_queue_generation_cannot_renew_or_consume_replacement(cur):
    cur.execute("SELECT postgremq.create_topic('t'); SELECT postgremq.create_queue('q','t',0,true)")
    old = cur.fetchone()[0]
    cur.execute("SELECT postgremq.delete_queue('q'); SELECT postgremq.create_queue('q','t',0,true)")
    new = cur.fetchone()[0]
    assert old != new
    cur.execute("SELECT * FROM postgremq.extend_queue_keep_alive_multi(ARRAY['q']::varchar[], ARRAY[60000]::bigint[], ARRAY[%s]::uuid[])",[old])
    assert cur.fetchall() == []
    cur.execute("SELECT postgremq.publish_message('t','{}')")
    with pytest.raises(psycopg2.Error) as error:
        cur.execute("SELECT * FROM postgremq.consume_message('q',30,1,%s)",[old])
    assert error.value.pgcode == 'PMQ02'
    cur.execute("SELECT * FROM postgremq.consume_message('q',30,1,%s)",[new])
    assert len(cur.fetchall()) == 1


def test_gc_preserves_a_concurrently_inserted_reference(cur, db_config, test_db):
    cur.execute("SELECT postgremq.create_topic('t'); SELECT postgremq.publish_message('t','{}')")
    id = cur.fetchone()[0]
    cur.execute("SELECT postgremq.create_queue('q','t')")
    writer = psycopg2.connect(**{**db_config, 'dbname': test_db})
    try:
        with writer.cursor() as other:
            other.execute("INSERT INTO postgremq.queue_messages(queue_name,message_id) VALUES ('q',%s)", [id])
        # The new reference is uncommitted, but its FK holds a key-share lock
        # on the payload. GC must skip that payload rather than cascade-delete it.
        cur.execute("SET statement_timeout='500ms'")
        cur.execute("SELECT postgremq.cleanup_unreferenced_messages(0)")
        assert cur.fetchone()[0] == 0
        writer.commit()
        cur.execute("SELECT count(*) FROM postgremq.queue_messages WHERE message_id=%s", [id])
        assert cur.fetchone()[0] == 1
    finally:
        writer.close()
        cur.execute("SET statement_timeout=0")


def test_keepalive_contention_does_not_block_other_queues(cur, db_config, test_db):
    cur.execute("SELECT postgremq.create_topic('t'); SELECT postgremq.create_queue('a','t',0,true); SELECT postgremq.create_queue('b','t',0,true)")
    blocker = psycopg2.connect(**{**db_config, 'dbname': test_db})
    try:
        with blocker.cursor() as locked:
            locked.execute("SELECT 1 FROM postgremq.queues WHERE name='a' FOR UPDATE")
        cur.execute("SET statement_timeout='500ms'")
        cur.execute("SELECT * FROM postgremq.extend_queue_keep_alive_multi(ARRAY['a','b']::varchar[],ARRAY[60000,60000]::bigint[])")
        outcomes = {r['queue_name']:r['outcome'] for r in cur.fetchall()}
        assert outcomes == {'a':'busy','b':'extended'}
    finally:
        blocker.rollback(); blocker.close()
        cur.execute("SET statement_timeout=0")


def test_schema_isolation_with_application_and_temporary_objects(conn):
    """Installation and function bodies must ignore the application's path."""
    with conn.cursor() as cur:
        cur.execute('DROP SCHEMA postgremq CASCADE; CREATE SCHEMA app')
        try:
            for name in ['topics', 'queues', 'messages', 'queue_messages', 'dead_letter_queue']:
                cur.execute(f'CREATE TABLE app.{name} (marker text)')
                cur.execute(f'CREATE TEMP TABLE {name} (marker text)')
            cur.execute('SET search_path = app, pg_temp')
            cur.execute(SQL_FILE.read_text())
            cur.execute('SHOW search_path')
            assert cur.fetchone()[0] == 'app, pg_temp'
            cur.execute("SELECT count(*) FROM pg_class WHERE relnamespace='public'::regnamespace")
            assert cur.fetchone()[0] == 0
            cur.execute("SELECT count(*) FROM pg_proc WHERE pronamespace='public'::regnamespace")
            assert cur.fetchone()[0] == 0
            cur.execute("SELECT postgremq.create_topic('t'); SELECT postgremq.create_queue('q', 't')")
            cur.execute("SELECT postgremq.publish_message('t', '{}'::jsonb)")
            msg_id = cur.fetchone()[0]
            cur.execute("SELECT message_id, consumer_token FROM postgremq.consume_message('q', 30, 1)")
            found_id, token = cur.fetchone()
            assert found_id == msg_id
            cur.execute("SELECT postgremq.set_vt('q', %s, %s, 30)", (msg_id, token))
            cur.execute("SELECT postgremq.ack_message('q', %s, %s)", (msg_id, token))
            cur.execute('SELECT * FROM postgremq.pmq_maintenance_fast()')
            cur.execute('SELECT postgremq.cleanup_completed_messages(0, 1000)')
            assert cur.fetchone()[0] == 1
            cur.execute('SELECT count(*) FROM postgremq.messages')
            assert cur.fetchone()[0] == 0
            for name in ['topics', 'queues', 'messages', 'queue_messages', 'dead_letter_queue']:
                cur.execute(f'SELECT count(*) FROM app.{name}')
                assert cur.fetchone()[0] == 0
                cur.execute(f'SELECT count(*) FROM pg_temp.{name}')
                assert cur.fetchone()[0] == 0
        finally:
            cur.execute('RESET search_path; DROP SCHEMA app CASCADE')


def test_queue_metrics_state_and_read_only_snapshot(cur, conn):
    """Metrics use consumption eligibility, not just stored status; no payload read."""
    cur.execute("""
        SELECT postgremq.create_topic('metrics');
        SELECT postgremq.create_queue('q', 'metrics', 3, false);
        SELECT postgremq.create_queue('expired', 'metrics', 3, true, interval '1 hour');
        SELECT postgremq.publish_message('metrics', '{}'::jsonb) FROM generate_series(1, 8);
        SELECT postgremq.create_queue('empty', 'metrics', 3, false);
        UPDATE postgremq.queues SET keep_alive_until = statement_timestamp() - interval '1 second'
            WHERE name = 'expired';
        -- Ready pending, expired processing, delayed, processing, exhausted,
        -- completed, dead letter, and a final-attempt live lease.
        UPDATE postgremq.queue_messages SET
            status = CASE WHEN message_id IN (2,4,5,8) THEN 'processing'
                          WHEN message_id = 6 THEN 'completed' ELSE 'pending' END,
            vt = statement_timestamp() + CASE WHEN message_id IN (3,4,8)
                THEN interval '1 hour' ELSE interval '-30 seconds' END,
            delivery_attempts = CASE WHEN message_id IN (5,8) THEN 3 ELSE 1 END;
        INSERT INTO postgremq.dead_letter_queue(queue_name, message_id, retry_count)
            VALUES ('q', 7, 3);
        DELETE FROM postgremq.queue_messages WHERE queue_name = 'q' AND message_id = 7;
        BEGIN READ ONLY;
        SET LOCAL search_path = pg_catalog;
        SELECT * FROM postgremq.queue_metrics() ORDER BY queue_name;
    """)
    rows = {r['queue_name']: dict(r) for r in cur.fetchall()}
    cur.execute('ROLLBACK')
    assert rows['q']['ready'] == 2
    assert rows['q']['delayed'] == 1
    assert rows['q']['processing'] == 2
    assert rows['q']['exhausted'] == 1
    assert rows['q']['dead_letter'] == 1
    assert 30 <= rows['q']['oldest_ready_age_seconds'] < 35
    assert rows['expired']['active'] == 0
    assert rows['expired']['ready'] == 0
    assert rows['expired']['oldest_ready_age_seconds'] == 0
    for name in ('ready', 'delayed', 'processing', 'exhausted', 'dead_letter', 'oldest_ready_age_seconds'):
        assert rows['empty'][name] == 0
    assert rows['empty']['active'] == 1


def test_queue_metrics_transaction_visibility(cur, conn, db_config, test_db):
    cur.execute("SELECT postgremq.create_topic('metrics'); SELECT postgremq.create_queue('q', 'metrics', 0, false)")
    writer = psycopg2.connect(**{**db_config, 'dbname': test_db})
    try:
        with writer.cursor() as w:
            w.execute("SELECT postgremq.publish_message('metrics', '{}'::jsonb)")
        cur.execute('SELECT ready FROM postgremq.queue_metrics()')
        assert cur.fetchone()[0] == 0
        writer.rollback()
        cur.execute('SELECT ready FROM postgremq.queue_metrics()')
        assert cur.fetchone()[0] == 0
        with writer.cursor() as w:
            w.execute("SELECT postgremq.publish_message('metrics', '{}'::jsonb)")
        writer.commit()
        cur.execute('SELECT ready FROM postgremq.queue_metrics()')
        assert cur.fetchone()[0] == 1
    finally:
        writer.close()


# ---------------------------------------------------------------------------
# Message groups (ordered delivery)
# ---------------------------------------------------------------------------

import random
import threading


def _connect(db_config, test_db):
    c = psycopg2.connect(**{**db_config, 'dbname': test_db})
    c.autocommit = True
    return c


def _wait_until(predicate, timeout=5.0, interval=0.01):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


def _backend_waits_on_lock(cur, conn):
    cur.execute("""SELECT count(*) FROM pg_stat_activity
                   WHERE pid = %s AND wait_event_type = 'Lock'""", (conn.get_backend_pid(),))
    return cur.fetchone()[0] == 1


def _drain_notifies(conn):
    conn.poll()
    conn.notifies.clear()


def _queue_notifies(conn, queue):
    conn.poll()
    got = [n for n in conn.notifies if n.channel == f'pmq:q:{queue}']
    conn.notifies.clear()
    return got


def _publish(cur, topic, payload, group=None, deliver_after=None):
    cur.execute(
        "SELECT postgremq.publish_message(%s, %s::jsonb, COALESCE(%s::timestamptz, clock_timestamp()), %s)",
        (topic, json.dumps(payload), deliver_after, group))
    return cur.fetchone()[0]


def _consume(cur, queue, vt=30, limit=1):
    cur.execute(
        "SELECT message_id, consumer_token, group_key, group_seq, delivery_attempts "
        "FROM postgremq.consume_message(%s, %s, %s)", (queue, vt, limit))
    return [dict(id=r[0], token=r[1], group=r[2], seq=r[3], attempts=r[4]) for r in cur.fetchall()]


def _ack(cur, queue, m):
    cur.execute("SELECT postgremq.ack_message(%s, %s, %s)", (queue, m['id'], m['token']))


def test_group_publish_assigns_dense_sequence_and_ungrouped_is_unchanged(cur):
    cur.execute("SELECT postgremq.create_topic('gt'); SELECT postgremq.create_queue('gq', 'gt')")
    a1 = _publish(cur, 'gt', {'n': 1}, 'A')
    b1 = _publish(cur, 'gt', {'n': 2}, 'B')
    a2 = _publish(cur, 'gt', {'n': 3}, 'A')
    # The three-argument form keeps working and stays ungrouped.
    cur.execute("SELECT postgremq.publish_message('gt', '{}'::jsonb, clock_timestamp())")
    u = cur.fetchone()[0]
    cur.execute("SELECT postgremq.publish_message('gt', '{}'::jsonb)")
    u2 = cur.fetchone()[0]

    cur.execute("SELECT id, group_key, group_seq FROM postgremq.messages ORDER BY id")
    assert [tuple(r) for r in cur.fetchall()] == [
        (a1, 'A', 1), (b1, 'B', 1), (a2, 'A', 2), (u, None, None), (u2, None, None)]
    cur.execute("SELECT message_id, group_key, group_seq FROM postgremq.list_messages('gq') ORDER BY message_id")
    assert [tuple(r) for r in cur.fetchall()] == [
        (a1, 'A', 1), (b1, 'B', 1), (a2, 'A', 2), (u, None, None), (u2, None, None)]
    cur.execute("SELECT group_key, group_seq FROM postgremq.get_message(%s)", (a2,))
    assert tuple(cur.fetchone()) == ('A', 2)
    cur.execute("SELECT topic_name, group_key, last_seq FROM postgremq.message_groups ORDER BY group_key")
    assert [tuple(r) for r in cur.fetchall()] == [('gt', 'A', 2), ('gt', 'B', 1)]


def test_group_key_validation(cur):
    cur.execute("SELECT postgremq.create_topic('gt')")
    for bad in ('', 'x' * 256):
        with pytest.raises(psycopg2.Error) as exc:
            _publish(cur, 'gt', {}, bad)
        assert exc.value.pgcode == 'PMQ03'
    assert _publish(cur, 'gt', {}, 'x' * 255) > 0
    with pytest.raises(psycopg2.Error) as exc:
        cur.execute("INSERT INTO postgremq.messages(topic_name, payload, group_key) VALUES ('gt', '{}', 'A')")
    assert exc.value.pgcode == '23514'  # group_key and group_seq must be paired


def test_group_order_under_concurrency(cur, db_config, test_db):
    """Spec test 1: 4 consumers, 8 groups, 200 interleaved grouped publishes
    published concurrently with consumption, plus ungrouped traffic in the same
    queue. Every group is claimed and acked in sequence order, every message
    is acked exactly once, and ungrouped rows flow."""
    cur.execute("SELECT postgremq.create_topic('gt'); SELECT postgremq.create_queue('gq', 'gt')")
    groups = [f'g{i}' for i in range(8)]
    plan = [random.choice(groups) for _ in range(200)] + [None] * 50
    random.shuffle(plan)

    lock = threading.Lock()
    claims = []          # (group, seq) in claim order
    acked = {}           # message_id -> ack count
    errors = []
    publishing_done = threading.Event()

    def publisher():
        try:
            c = _connect(db_config, test_db)
            with c.cursor() as pc:
                for i, g in enumerate(plan):
                    _publish(pc, 'gt', {'i': i}, g)
                    if i % 10 == 0:
                        time.sleep(0.002)
            c.close()
        except Exception as e:  # pragma: no cover - surfaced below
            errors.append(e)
        finally:
            publishing_done.set()

    def consumer():
        try:
            c = _connect(db_config, test_db)
            with c.cursor() as cc:
                idle = 0
                while True:
                    batch = _consume(cc, 'gq', vt=30, limit=3)
                    if not batch:
                        with lock:
                            finished = publishing_done.is_set() and sum(acked.values()) >= len(plan)
                        if finished or (publishing_done.is_set() and idle > 400):
                            break
                        idle += 1
                        time.sleep(0.005)
                        continue
                    idle = 0
                    seen = [m['group'] for m in batch if m['group'] is not None]
                    assert len(seen) == len(set(seen)), f'batch holds two rows of a group: {batch}'
                    with lock:
                        claims.extend((m['group'], m['seq']) for m in batch)
                    for m in batch:
                        time.sleep(random.uniform(0, 0.003))
                        _ack(cc, 'gq', m)
                        with lock:
                            acked[m['id']] = acked.get(m['id'], 0) + 1
            c.close()
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=publisher)] + [threading.Thread(target=consumer) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=120)
    assert not errors, errors

    assert len(acked) == len(plan)
    assert all(n == 1 for n in acked.values()), 'a message was acked more than once'
    for g in groups:
        seqs = [s for (gk, s) in claims if gk == g]
        assert seqs == list(range(1, plan.count(g) + 1)), f'group {g} claimed out of order: {seqs}'
    assert sum(1 for (gk, _) in claims if gk is None) == plan.count(None)

    # Commit order of the acks themselves (DB clock) follows the sequence.
    cur.execute("""
        SELECT group_key, array_agg(group_seq ORDER BY processed_at, group_seq)
        FROM postgremq.queue_messages
        WHERE queue_name = 'gq' AND group_key IS NOT NULL
        GROUP BY group_key""")
    for g, seqs in cur.fetchall():
        assert seqs == sorted(seqs), f'group {g} acked out of order: {seqs}'


def test_group_ungrouped_flows_while_head_leased(cur):
    cur.execute("SELECT postgremq.create_topic('gt'); SELECT postgremq.create_queue('gq', 'gt')")
    _publish(cur, 'gt', {}, 'A')
    _publish(cur, 'gt', {}, 'A')
    [head] = _consume(cur, 'gq')
    assert (head['group'], head['seq']) == ('A', 1)
    u1 = _publish(cur, 'gt', {})
    u2 = _publish(cur, 'gt', {})
    got = _consume(cur, 'gq', limit=10)
    assert sorted(m['id'] for m in got) == sorted([u1, u2])


def test_group_leased_head_blocks_successor_and_ack_notifies(cur):
    """Spec test 2."""
    cur.execute("SELECT postgremq.create_topic('gt'); SELECT postgremq.create_queue('gq', 'gt')")
    _publish(cur, 'gt', {}, 'A')
    a2 = _publish(cur, 'gt', {}, 'A')
    cur.execute('LISTEN "pmq:q:gq"')
    [head] = _consume(cur, 'gq')
    assert head['seq'] == 1
    assert _consume(cur, 'gq', limit=10) == []
    _drain_notifies(cur.connection)

    _ack(cur, 'gq', head)
    notes = _queue_notifies(cur.connection, 'gq')
    assert len(notes) == 1 and notes[0].payload == ''
    [succ] = _consume(cur, 'gq', limit=10)
    assert (succ['id'], succ['seq']) == (a2, 2)

    # Ungrouped acks stay silent (nothing new to consume).
    u = _publish(cur, 'gt', {})
    [m] = _consume(cur, 'gq')
    assert m['id'] == u
    _drain_notifies(cur.connection)
    _ack(cur, 'gq', m)
    assert _queue_notifies(cur.connection, 'gq') == []


def test_group_ack_in_caller_transaction_notifies_on_commit(cur, db_config, test_db):
    cur.execute("SELECT postgremq.create_topic('gt'); SELECT postgremq.create_queue('gq', 'gt')")
    _publish(cur, 'gt', {}, 'A')
    _publish(cur, 'gt', {}, 'A')
    [head] = _consume(cur, 'gq')
    cur.execute('LISTEN "pmq:q:gq"')
    _drain_notifies(cur.connection)
    tx = psycopg2.connect(**{**db_config, 'dbname': test_db})
    try:
        with tx.cursor() as t:
            _ack(t, 'gq', head)
            # Uncommitted ack: no wake-up yet, and the successor is still blocked.
            assert _queue_notifies(cur.connection, 'gq') == []
            assert _consume(cur, 'gq') == []
            tx.rollback()
        assert _queue_notifies(cur.connection, 'gq') == []
        assert _consume(cur, 'gq') == []
        with tx.cursor() as t:
            _ack(t, 'gq', head)
        tx.commit()
        got = []
        assert _wait_until(lambda: got.extend(_queue_notifies(cur.connection, 'gq')) or len(got) >= 1)
        assert len(got) == 1
        [succ] = _consume(cur, 'gq')
        assert succ['seq'] == 2
    finally:
        tx.close()


def test_group_nack_with_delay_blocks_group(cur):
    """Spec test 3."""
    cur.execute("SELECT postgremq.create_topic('gt'); SELECT postgremq.create_queue('gq', 'gt')")
    a1 = _publish(cur, 'gt', {}, 'A')
    _publish(cur, 'gt', {}, 'A')
    b1 = _publish(cur, 'gt', {}, 'B')
    [head] = _consume(cur, 'gq')
    assert head['id'] == a1
    cur.execute("SELECT postgremq.nack_message('gq', %s, %s, clock_timestamp() + interval '1 second')",
                (a1, head['token']))
    # Group A is blocked by its delayed head; group B is unaffected.
    got = _consume(cur, 'gq', limit=10)
    assert [m['id'] for m in got] == [b1]
    time.sleep(1.1)
    got = _consume(cur, 'gq', limit=10)
    assert [(m['id'], m['attempts']) for m in got] == [(a1, 2)]


def test_group_lease_expiry_redelivers_head(cur):
    """Spec test 4."""
    cur.execute("SELECT postgremq.create_topic('gt'); SELECT postgremq.create_queue('gq', 'gt')")
    a1 = _publish(cur, 'gt', {}, 'A')
    _publish(cur, 'gt', {}, 'A')
    [head] = _consume(cur, 'gq', vt=1)
    assert head['id'] == a1
    time.sleep(1.1)
    got = _consume(cur, 'gq', limit=10)
    assert [(m['id'], m['seq'], m['attempts']) for m in got] == [(a1, 1, 2)]
    # The stale token is fenced; the successor is still blocked.
    with pytest.raises(psycopg2.Error) as exc:
        _ack(cur, 'gq', head)
    assert exc.value.pgcode == 'PMQ01'
    assert _consume(cur, 'gq', limit=10) == []


def test_group_next_visible_time_ignores_blocked_successors(cur):
    """Clients schedule their next fetch at get_next_visible_time. A blocked
    successor's vt is in the past; reporting it would make them refetch in a
    tight loop while the head is leased."""
    cur.execute("SELECT postgremq.create_topic('gt'); SELECT postgremq.create_queue('gq', 'gt')")
    _publish(cur, 'gt', {}, 'A')
    _publish(cur, 'gt', {}, 'A')
    [head] = _consume(cur, 'gq', vt=60)
    cur.execute("SELECT postgremq.get_next_visible_time('gq') - clock_timestamp()")
    assert cur.fetchone()[0] > timedelta(seconds=55)
    _ack(cur, 'gq', head)
    cur.execute("SELECT postgremq.get_next_visible_time('gq') <= clock_timestamp()")
    assert cur.fetchone()[0] is True


def test_group_final_attempt_nack_retires_head_and_notifies(cur):
    """Spec test 5, inline nack path."""
    cur.execute("SELECT postgremq.create_topic('gt'); SELECT postgremq.create_queue('gq', 'gt', 1)")
    a1 = _publish(cur, 'gt', {}, 'A')
    a2 = _publish(cur, 'gt', {}, 'A')
    cur.execute('LISTEN "pmq:q:gq"')
    [head] = _consume(cur, 'gq')
    _drain_notifies(cur.connection)
    cur.execute("SELECT postgremq.nack_message('gq', %s, %s)", (a1, head['token']))
    cur.execute("SELECT message_id FROM postgremq.dead_letter_queue WHERE queue_name = 'gq'")
    assert [r[0] for r in cur.fetchall()] == [a1]
    assert len(_queue_notifies(cur.connection, 'gq')) == 1
    [succ] = _consume(cur, 'gq', limit=10)
    assert succ['id'] == a2

    # An ungrouped final-attempt retirement stays silent.
    u = _publish(cur, 'gt', {})
    [m] = _consume(cur, 'gq')
    assert m['id'] == u
    _drain_notifies(cur.connection)
    cur.execute("SELECT postgremq.nack_message('gq', %s, %s)", (u, m['token']))
    assert _queue_notifies(cur.connection, 'gq') == []


def test_group_maintenance_retirement_unblocks_and_notifies_once_per_queue(cur):
    """Spec test 5, maintenance path: a crashed final attempt holds its group
    until pmq_maintenance_fast retires it; one NOTIFY per affected queue."""
    cur.execute("SELECT postgremq.create_topic('gt'); SELECT postgremq.create_queue('gq', 'gt', 1)")
    cur.execute("SELECT postgremq.create_queue('quiet', 'gt', 0)")
    a1 = _publish(cur, 'gt', {}, 'A')
    a2 = _publish(cur, 'gt', {}, 'A')
    b1 = _publish(cur, 'gt', {}, 'B')
    b2 = _publish(cur, 'gt', {}, 'B')
    got = _consume(cur, 'gq', vt=1, limit=10)
    assert sorted(m['id'] for m in got) == [a1, b1]
    time.sleep(1.1)
    # Exhausted heads are neither claimable nor do they let successors pass.
    assert _consume(cur, 'gq', limit=10) == []

    cur.execute('LISTEN "pmq:q:gq"; LISTEN "pmq:q:quiet"')
    _drain_notifies(cur.connection)
    cur.execute("SELECT retired_to_dlq FROM postgremq.pmq_maintenance_fast()")
    assert cur.fetchone()[0] == 2
    cur.connection.poll()
    chans = [n.channel for n in cur.connection.notifies]
    cur.connection.notifies.clear()
    assert chans == ['pmq:q:gq']
    got = _consume(cur, 'gq', limit=10)
    assert sorted(m['id'] for m in got) == [a2, b2]


def test_group_batch_claim_returns_one_row_per_group(cur):
    """Spec test 6."""
    cur.execute("SELECT postgremq.create_topic('gt'); SELECT postgremq.create_queue('gq', 'gt')")
    for _ in range(5):
        for g in ('A', 'B', 'C'):
            _publish(cur, 'gt', {}, g)
    u1 = _publish(cur, 'gt', {})
    u2 = _publish(cur, 'gt', {})
    got = _consume(cur, 'gq', limit=100)
    grouped = sorted((m['group'], m['seq']) for m in got if m['group'])
    assert grouped == [('A', 1), ('B', 1), ('C', 1)]
    assert sorted(m['id'] for m in got if not m['group']) == [u1, u2]


def test_group_order_is_per_queue(cur):
    """Spec test 7: two queues on one topic keep order independently."""
    cur.execute("SELECT postgremq.create_topic('gt')")
    cur.execute("SELECT postgremq.create_queue('q1', 'gt'); SELECT postgremq.create_queue('q2', 'gt')")
    a1 = _publish(cur, 'gt', {}, 'A')
    a2 = _publish(cur, 'gt', {}, 'A')
    a3 = _publish(cur, 'gt', {}, 'A')
    [h1] = _consume(cur, 'q1')
    assert h1['id'] == a1
    # q1's leased head does not block q2's copy of the group.
    for expected in (a1, a2, a3):
        [m] = _consume(cur, 'q2', limit=10)
        assert m['id'] == expected
        _ack(cur, 'q2', m)
    assert _consume(cur, 'q1', limit=10) == []
    _ack(cur, 'q1', h1)
    [m] = _consume(cur, 'q1', limit=10)
    assert m['id'] == a2


def test_group_concurrent_publishers_serialise_in_commit_order(cur, db_config, test_db):
    """Spec test 8: a second publisher of the group blocks until the first
    commits; sequences are dense and in commit order (also across a rollback)."""
    cur.execute("SELECT postgremq.create_topic('gt')")
    first = psycopg2.connect(**{**db_config, 'dbname': test_db})
    second = _connect(db_config, test_db)
    try:
        with first.cursor() as f:
            m1 = _publish(f, 'gt', {'who': 'first'}, 'A')
            m2 = _publish(f, 'gt', {'who': 'first'}, 'A')
        result = {}
        errors = []

        def publish_second():
            try:
                with second.cursor() as s:
                    result['id'] = _publish(s, 'gt', {'who': 'second'}, 'A')
            except Exception as e:
                errors.append(e)

        t = threading.Thread(target=publish_second)
        t.start()
        try:
            assert _wait_until(lambda: _backend_waits_on_lock(cur, second)), \
                'second publisher of the group must wait for the first to commit'
            assert t.is_alive()
            # Another group is not serialised behind it.
            _publish(cur, 'gt', {}, 'B')
        finally:
            first.commit()
            t.join(timeout=10)
        assert not t.is_alive() and not errors, errors

        cur.execute("SELECT id, group_seq FROM postgremq.messages WHERE group_key = 'A' ORDER BY group_seq")
        assert [tuple(r) for r in cur.fetchall()] == [(m1, 1), (m2, 2), (result['id'], 3)]

        # A rolled-back publish leaves no gap: the next publisher reuses the sequence.
        with first.cursor() as f:
            _publish(f, 'gt', {}, 'A')
        t = threading.Thread(target=publish_second)
        t.start()
        try:
            assert _wait_until(lambda: _backend_waits_on_lock(cur, second))
        finally:
            first.rollback()
            t.join(timeout=10)
        assert not t.is_alive() and not errors, errors
        cur.execute("SELECT group_seq FROM postgremq.messages WHERE group_key = 'A' ORDER BY group_seq")
        assert [r[0] for r in cur.fetchall()] == [1, 2, 3, 4]
    finally:
        first.close()
        second.close()


def test_group_requeued_dlq_row_reenters_as_head(cur):
    """Spec test 9."""
    cur.execute("SELECT postgremq.create_topic('gt'); SELECT postgremq.create_queue('gq', 'gt', 1)")
    a1 = _publish(cur, 'gt', {}, 'A')
    a2 = _publish(cur, 'gt', {}, 'A')
    a3 = _publish(cur, 'gt', {}, 'A')
    [m] = _consume(cur, 'gq')
    cur.execute("SELECT postgremq.nack_message('gq', %s, %s)", (a1, m['token']))  # -> DLQ
    [m] = _consume(cur, 'gq')
    assert m['id'] == a2
    _ack(cur, 'gq', m)

    cur.execute("SELECT postgremq.requeue_dlq_messages('gq')")
    cur.execute("SELECT group_key, group_seq FROM postgremq.queue_messages WHERE queue_name='gq' AND message_id=%s", (a1,))
    assert tuple(cur.fetchone()) == ('A', 1)
    got = _consume(cur, 'gq', limit=10)
    assert [(m['id'], m['seq']) for m in got] == [(a1, 1)]  # a3 is blocked behind it
    _ack(cur, 'gq', got[0])
    [m] = _consume(cur, 'gq', limit=10)
    assert m['id'] == a3


def test_group_cleanup_prunes_only_empty_groups(cur, db_config, test_db):
    """Spec test 10: cleanup removes only empty groups, never a group with a
    publish in flight, and the next publish to a pruned group restarts at 1."""
    cur.execute("SELECT postgremq.create_topic('gt')")  # no queues: payloads are unreferenced
    for g in ('A', 'B', 'C'):
        _publish(cur, 'gt', {}, g)
    _publish(cur, 'gt', {}, 'B')
    cur.execute("UPDATE postgremq.messages SET published_at = clock_timestamp() - interval '2 days'")
    cur.execute("SELECT postgremq.create_queue('gq', 'gt')")
    b_live = _publish(cur, 'gt', {}, 'B')   # fresh and referenced: group B stays

    writer = psycopg2.connect(**{**db_config, 'dbname': test_db})
    try:
        with writer.cursor() as w:
            _publish(w, 'gt', {}, 'C')       # in flight: holds group C's row lock
        cur.execute("SET statement_timeout = '2s'")
        cur.execute("SELECT postgremq.cleanup_unreferenced_messages(24, 100)")
        assert cur.fetchone()[0] == 4      # A1, B1, B2, C1 payloads
        cur.execute("SELECT group_key, last_seq FROM postgremq.message_groups ORDER BY group_key")
        assert [tuple(r) for r in cur.fetchall()] == [('B', 3), ('C', 1)]  # A pruned; C skipped
        writer.commit()
        cur.execute("SELECT group_key, last_seq FROM postgremq.message_groups ORDER BY group_key")
        assert [tuple(r) for r in cur.fetchall()] == [('B', 3), ('C', 2)]
        assert _publish(cur, 'gt', {}, 'C') and True
        cur.execute("SELECT group_seq FROM postgremq.messages WHERE group_key='C' ORDER BY group_seq")
        assert [r[0] for r in cur.fetchall()] == [2, 3]
        _publish(cur, 'gt', {}, 'A')
        cur.execute("SELECT group_seq FROM postgremq.messages WHERE group_key='A'")
        assert [r[0] for r in cur.fetchall()] == [1]
        cur.execute("SELECT group_seq FROM postgremq.messages WHERE id=%s", (b_live,))
        assert cur.fetchone()[0] == 3
    finally:
        cur.execute("SET statement_timeout = 0")
        writer.close()


def test_group_prune_keeps_group_that_regained_a_message(cur, db_config, test_db):
    """prune_message_groups re-checks emptiness before deleting: a group whose
    message committed after the caller computed its candidate list survives.
    (The narrower race the two-statement design exists for — a publish that
    commits between the lock statement's snapshot and its lock acquisition —
    cannot be scheduled deterministically from a test; it was reproduced
    against the single-statement variants during design review.)"""
    cur.execute("SELECT postgremq.create_topic('gt')")
    _publish(cur, 'gt', {}, 'A')
    cur.execute("DELETE FROM postgremq.messages")
    other = _connect(db_config, test_db)
    try:
        with other.cursor() as o:
            _publish(o, 'gt', {}, 'A')    # committed: group A is not empty any more
        cur.execute("SELECT postgremq.prune_message_groups(ARRAY['gt'], ARRAY['A'])")
        assert cur.fetchone()[0] == 0
        cur.execute("SELECT last_seq FROM postgremq.message_groups WHERE group_key='A'")
        assert cur.fetchone()[0] == 2
    finally:
        other.close()


def test_group_rows_follow_topic_lifecycle(cur):
    cur.execute("SELECT postgremq.create_topic('gt'); SELECT postgremq.create_topic('other')")
    _publish(cur, 'gt', {}, 'A')
    _publish(cur, 'other', {}, 'A')
    cur.execute("SELECT postgremq.clean_up_topic('gt')")
    cur.execute("SELECT topic_name FROM postgremq.message_groups")
    assert [r[0] for r in cur.fetchall()] == ['other']
    cur.execute("SELECT postgremq.delete_topic('gt')")
    cur.execute("SELECT postgremq.purge_all_messages()")
    cur.execute("SELECT count(*) FROM postgremq.message_groups")
    assert cur.fetchone()[0] == 0


def _plan_nodes(node):
    yield node
    for child in node.get('Plans', []):
        yield from _plan_nodes(child)


def _captured_plans(conn, needle):
    """Plans auto_explain sent as notices (JSON format) whose query text
    contains `needle`, as lists of plan nodes."""
    plans = []
    for notice in conn.notices:
        if 'plan:' not in notice:
            continue
        doc = json.loads(notice.split('plan:', 1)[1])
        if needle in doc['Query Text']:
            plans.append(list(_plan_nodes(doc['Plan'])))
    return plans


def test_group_consume_plan_uses_group_head_index(cur):
    """Spec test 11: on a 100k-row backlog in 1k groups consume_message walks
    the queue's vt-ordered index (stopping at LIMIT), probes
    idx_queue_messages_group_head for the head check, and never sorts or
    seq-scans the backlog. Plans of the statements inside the plpgsql function
    are captured with auto_explain (log_nested_statements) as notices; both the
    first (custom) and the steady-state (cached) plan are checked."""
    cur.execute("SELECT postgremq.create_topic('gt'); SELECT postgremq.create_queue('gq', 'gt')")
    cur.execute("SELECT postgremq.create_queue('other', 'gt')")  # rows of another queue in the table
    cur.execute("""
        INSERT INTO postgremq.messages(topic_name, payload, published_at, deliver_after, group_key, group_seq)
        SELECT 'gt', '{}'::jsonb, ts, ts, 'g' || (i % 1000), i / 1000 + 1
        FROM generate_series(0, 99999) AS i,
             LATERAL (SELECT clock_timestamp() - interval '1 hour' + i * interval '1 millisecond' AS ts) t""")
    cur.execute("""INSERT INTO postgremq.message_groups(topic_name, group_key, last_seq)
                   SELECT 'gt', 'g' || g, 100 FROM generate_series(0, 999) g""")
    cur.execute("ANALYZE postgremq.queue_messages; ANALYZE postgremq.messages")
    # Lease some heads so the scan has to step over blocked successors too.
    _consume(cur, 'gq', vt=300, limit=200)

    conn = cur.connection
    cur.execute("LOAD 'auto_explain'")
    cur.execute("""SET auto_explain.log_min_duration = 0;
                   SET auto_explain.log_analyze = on;
                   SET auto_explain.log_nested_statements = on;
                   SET auto_explain.log_format = 'json';
                   SET auto_explain.log_level = 'notice';
                   SET client_min_messages = 'notice'""")
    try:
        for attempt in range(8):
            # Check both the per-call custom plan and the cached generic plan
            # plpgsql switches to after repeated executions.
            mode = 'force_custom_plan' if attempt % 2 == 0 else 'force_generic_plan'
            cur.execute(f"SET plan_cache_mode = {mode}")
            del conn.notices[:]
            got = _consume(cur, 'gq', vt=30, limit=10)
            assert len(got) == 10
            assert len({m['group'] for m in got}) == 10, 'one row per group'
            assert all(m['seq'] == 1 for m in got), 'only heads are claimable'

            plans = _captured_plans(conn, 'next_msg')
            assert len(plans) == 1, conn.notices
            nodes = plans[0]
            types = [n['Node Type'] for n in nodes]
            assert 'Sort' not in types, types
            assert not any(n['Node Type'] == 'Seq Scan' and n.get('Relation Name') == 'queue_messages'
                           for n in nodes), types
            indexes = {n.get('Index Name') for n in nodes}
            assert 'idx_queue_messages_group_head' in indexes, indexes
            assert indexes & {'idx_queue_messages_consume', 'idx_queue_messages_next_visible'}, indexes
            # The vt-ordered walk stops early: it never reads the backlog.
            outer = [n for n in nodes if n.get('Index Name') in
                     ('idx_queue_messages_consume', 'idx_queue_messages_next_visible')]
            assert all(n['Actual Rows'] + n.get('Rows Removed by Filter', 0) < 1000 for n in outer), outer

        # Every remaining head leased: the claim now has to step over the
        # blocked successors, still via the group-head index and still
        # without a Sort or a Seq Scan.
        cur.execute("SET plan_cache_mode = force_custom_plan; SET auto_explain.log_min_duration = -1")
        _consume(cur, 'gq', vt=300, limit=1000)
        cur.execute("SET auto_explain.log_min_duration = 0")
        for mode in ('force_custom_plan', 'force_generic_plan'):
            cur.execute(f"SET plan_cache_mode = {mode}")
            del conn.notices[:]
            assert _consume(cur, 'gq', vt=30, limit=10) == []
            [nodes] = _captured_plans(conn, 'next_msg')
            types = [n['Node Type'] for n in nodes]
            assert 'Sort' not in types, types
            assert not any(n['Node Type'] == 'Seq Scan' and n.get('Relation Name') == 'queue_messages'
                           for n in nodes), types
            probes = [n for n in nodes if n.get('Index Name') == 'idx_queue_messages_group_head']
            assert probes and max(n['Actual Loops'] for n in probes) > 1000, probes
    finally:
        cur.execute("SET auto_explain.log_min_duration = -1; RESET client_min_messages; RESET plan_cache_mode")


def test_group_delete_queue_message_wakes_successor(cur):
    cur.execute("SELECT postgremq.create_topic('gt'); SELECT postgremq.create_queue('gq', 'gt')")
    a1 = _publish(cur, 'gt', {}, 'A')
    a2 = _publish(cur, 'gt', {}, 'A')
    cur.execute('LISTEN "pmq:q:gq"')
    _drain_notifies(cur.connection)
    cur.execute("SELECT postgremq.delete_queue_message('gq', %s)", (a1,))
    assert len(_queue_notifies(cur.connection, 'gq')) == 1
    [m] = _consume(cur, 'gq')
    assert m['id'] == a2


MIGRATIONS_DIR = Path(__file__).parent.parent / 'migrations'


def test_latest_sql_records_the_latest_migration_version(cur):
    """A latest.sql install is recognised by the migrators as current."""
    latest = max(int(p.name.split('_', 1)[0]) for p in MIGRATIONS_DIR.glob('*.up.sql'))
    cur.execute('SELECT version, dirty FROM postgremq.postgremq_migrations')
    assert [tuple(row) for row in cur.fetchall()] == [(latest, False)]


def test_latest_sql_refuses_an_existing_installation(cur):
    """latest.sql is for fresh installs only; upgrades go through the migrations."""
    cur.execute("SELECT postgremq.create_topic('kept')")
    with pytest.raises(psycopg2.errors.RaiseException) as err:
        cur.execute(SQL_FILE.read_text())
    assert 'only for fresh installs' in str(err.value)
    assert 'migrat' in err.value.diag.message_hint
    cur.execute("SELECT name FROM postgremq.topics")
    assert [row[0] for row in cur.fetchall()] == ['kept']


def _fresh_database(admin_conn, name: str) -> None:
    with admin_conn.cursor() as cur:
        cur.execute(f"DROP DATABASE IF EXISTS {name} WITH (FORCE)")
        cur.execute(f"CREATE DATABASE {name}")


def _schema_dump(postgres_container, db_config, dbname: str) -> str:
    code, out = postgres_container.get_wrapped_container().exec_run(
        ['pg_dump', '-U', db_config['user'], '--schema-only', '--schema', 'postgremq', dbname])
    assert code == 0, out.decode()
    # pg_dump 15.14+ brackets the dump with \restrict / \unrestrict lines
    # carrying a random key; they are not part of the schema.
    return ''.join(line for line in out.decode().splitlines(keepends=True)
                   if not line.startswith(('\\restrict ', '\\unrestrict ')))


def _assert_install_equals_migrations(postgres_container, db_config, admin_conn,
                                      latest_sql: str, migrations: list[Path]) -> str:
    """Installs latest_sql and, separately, every migration as a migrator does;
    asserts identical schemas, version rows and info(). Returns info()."""
    by_script, by_migrations = f"mq_latest_{uuid.uuid4().hex[:8]}", f"mq_migrated_{uuid.uuid4().hex[:8]}"
    for name in (by_script, by_migrations):
        _fresh_database(admin_conn, name)
    try:
        with psycopg2.connect(**{**db_config, 'dbname': by_script}) as c, c.cursor() as cur:
            cur.execute(latest_sql)
        numbers = [int(path.name.split('_', 1)[0]) for path in migrations]
        assert len(numbers) == len(set(numbers)), f'duplicate migration numbers: {sorted(numbers)}'
        # The migrators' protocol (golang-migrate): version table first, then
        # each migration between a dirty and a clean version row.
        with psycopg2.connect(**{**db_config, 'dbname': by_migrations}) as c:
            c.autocommit = True
            with c.cursor() as cur:
                cur.execute('CREATE SCHEMA postgremq; CREATE TABLE postgremq.postgremq_migrations '
                            '(version bigint not null primary key, dirty boolean not null)')
                for path in sorted(migrations):
                    version = int(path.name.split('_', 1)[0])
                    cur.execute('TRUNCATE postgremq.postgremq_migrations; '
                                'INSERT INTO postgremq.postgremq_migrations VALUES (%s, true)', (version,))
                    cur.execute(path.read_text())
                    cur.execute('UPDATE postgremq.postgremq_migrations SET dirty = false')
        assert _schema_dump(postgres_container, db_config, by_script) == \
            _schema_dump(postgres_container, db_config, by_migrations)
        rows, versions = [], []
        for name in (by_script, by_migrations):
            with psycopg2.connect(**{**db_config, 'dbname': name}) as c, c.cursor() as cur:
                cur.execute('SELECT version, dirty FROM postgremq.postgremq_migrations')
                rows.append(cur.fetchall())
                cur.execute('SELECT postgremq.info()')
                versions.append(cur.fetchone()[0])
        assert rows[0] == rows[1]
        assert versions[0] == versions[1]
        return versions[0]
    finally:
        with admin_conn.cursor() as cur:
            for name in (by_script, by_migrations):
                cur.execute(f"DROP DATABASE IF EXISTS {name} WITH (FORCE)")


def test_latest_sql_install_equals_the_migrations(postgres_container, db_config, admin_conn):
    """A fresh latest.sql install equals running every migration as a migrator does."""
    _assert_install_equals_migrations(postgres_container, db_config, admin_conn,
                                      SQL_FILE.read_text(), list(MIGRATIONS_DIR.glob('*.up.sql')))


def test_info_reports_the_schema_version_and_protocol_major(cur):
    latest = max(int(p.name.split('_', 1)[0]) for p in MIGRATIONS_DIR.glob('*.up.sql'))
    cur.execute('SELECT postgremq.info()')
    assert cur.fetchone()[0] == {'schema_version': latest, 'protocol_major': 1}


def test_info_follows_the_version_table(cur):
    """schema_version is whatever the migrators last recorded; NULL without the table."""
    cur.execute('UPDATE postgremq.postgremq_migrations SET version = 7')
    cur.execute("SELECT postgremq.info()->'schema_version'")
    assert cur.fetchone()[0] == 7
    cur.execute('DROP TABLE postgremq.postgremq_migrations')
    cur.execute('SELECT postgremq.info()')
    assert cur.fetchone()[0] == {'schema_version': None, 'protocol_major': 1}


def test_info_needs_no_privilege_on_the_version_table(cur):
    """Runtime roles call info() when connecting; they get no grant on the version table."""
    role = f"pmq_info_{uuid.uuid4().hex[:8]}"
    cur.execute(f"CREATE ROLE {role}")
    try:
        cur.execute(f"GRANT USAGE ON SCHEMA postgremq TO {role}")
        cur.execute(f"SET ROLE {role}")
        cur.execute('SELECT postgremq.info()')
        assert cur.fetchone()[0]['protocol_major'] == 1
        with pytest.raises(psycopg2.errors.InsufficientPrivilege):
            cur.execute('SELECT version FROM postgremq.postgremq_migrations')
    finally:
        cur.execute('RESET ROLE')
        cur.execute(f"REVOKE USAGE ON SCHEMA postgremq FROM {role}")
        cur.execute(f"DROP ROLE {role}")


REPO_ROOT = MIGRATIONS_DIR.parent.parent


def _git(*args: str) -> str:
    import subprocess
    return subprocess.run(['git', '-C', str(REPO_ROOT), *args], check=True,
                          capture_output=True, text=True).stdout


def _mq_release_tags() -> list[str]:
    try:
        # Only this branch's releases: tags on other branches are not its history.
        return [tag for tag in _git('tag', '--merged', 'HEAD', '--list', 'mq/v*').split() if tag]
    except Exception:  # no git checkout (e.g. a source archive)
        return []


def test_released_migrations_are_unchanged():
    """Released migrations are immutable: each mq release tag's files are still byte-identical."""
    tags = _mq_release_tags()
    if not tags:
        pytest.skip('no mq release yet')
    for tag in tags:
        for path in _git('ls-tree', '--name-only', f'{tag}:mq/migrations').split():
            released = _git('show', f'{tag}:mq/migrations/{path}')
            current = MIGRATIONS_DIR / path
            assert current.exists(), f'{path} (released in {tag}) was deleted'
            assert current.read_text() == released, f'{path} (released in {tag}) was changed'


def test_upgrade_from_released_versions_equals_a_fresh_install(postgres_container, db_config, admin_conn):
    """Installing a released latest.sql, then migrating, equals installing the current latest.sql."""
    import re
    tags = _mq_release_tags()
    if not tags:
        pytest.skip('no mq release yet')
    fresh = f"mq_fresh_{uuid.uuid4().hex[:8]}"
    _fresh_database(admin_conn, fresh)
    try:
        with psycopg2.connect(**{**db_config, 'dbname': fresh}) as c, c.cursor() as cur:
            cur.execute(SQL_FILE.read_text())
        expected = _schema_dump(postgres_container, db_config, fresh)
        for tag in tags:
            old_sql = _git('show', f'{tag}:mq/sql/latest.sql')
            recorded = int(re.search(
                r'INSERT INTO postgremq\.postgremq_migrations \(version, dirty\) VALUES \((\d+), false\);',
                old_sql).group(1))
            upgraded = f"mq_upgraded_{uuid.uuid4().hex[:8]}"
            _fresh_database(admin_conn, upgraded)
            try:
                with psycopg2.connect(**{**db_config, 'dbname': upgraded}) as c:
                    c.autocommit = True
                    with c.cursor() as cur:
                        cur.execute(old_sql)
                        for path in sorted(MIGRATIONS_DIR.glob('*.up.sql')):
                            version = int(path.name.split('_', 1)[0])
                            if version <= recorded:
                                continue
                            cur.execute('TRUNCATE postgremq.postgremq_migrations; '
                                        'INSERT INTO postgremq.postgremq_migrations VALUES (%s, true)', (version,))
                            cur.execute(path.read_text())
                            cur.execute('UPDATE postgremq.postgremq_migrations SET dirty = false')
                assert _schema_dump(postgres_container, db_config, upgraded) == expected, \
                    f'upgrading from {tag} differs from a fresh install'
            finally:
                with admin_conn.cursor() as cur:
                    cur.execute(f"DROP DATABASE IF EXISTS {upgraded} WITH (FORCE)")
    finally:
        with admin_conn.cursor() as cur:
            cur.execute(f"DROP DATABASE IF EXISTS {fresh} WITH (FORCE)")
