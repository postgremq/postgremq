"""End-to-end: real pinned Collector + PostgreSQL + the three client examples.

Run from the repository root:
  .venv/bin/pytest observability/test_collector.py -v
Requires mq/tests/requirements.txt, Docker, Go, npm ci in postgremq-ts, and a
Rust toolchain (the Rust example is built on first run if not built already).
All containers, networks and databases are isolated and removed on exit.
"""
import os
from pathlib import Path
import subprocess
import time
import urllib.request
import uuid

import docker
import psycopg2

ROOT = Path(__file__).resolve().parents[1]
IMAGE = 'otel/opentelemetry-collector-contrib:0.147.0'


def eventually(fn, timeout=60):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            return fn()
        except (AssertionError, OSError, psycopg2.OperationalError) as err:
            last = err
            time.sleep(0.25)
    raise AssertionError(f'Timed out: {last}')


def test_collector_sql_and_client_examples():
    client = docker.from_env()
    network = None
    containers = []
    try:
        for image in ('postgres:15-alpine', IMAGE):
            try:
                client.images.get(image)
            except docker.errors.ImageNotFound:
                client.images.pull(image)
        network = client.networks.create('pmq-metrics-' + uuid.uuid4().hex[:12])
        db = client.containers.run('postgres:15-alpine', detach=True,
            network=network.name, name='pmq-db-' + uuid.uuid4().hex[:12],
            environment={'POSTGRES_PASSWORD': 'postgremq', 'POSTGRES_DB': 'postgremq'},
            ports={'5432/tcp': ('127.0.0.1', None)})
        containers.append(db)
        db.reload()
        port = db.attrs['NetworkSettings']['Ports']['5432/tcp'][0]['HostPort']
        dsn = f'postgres://postgres:postgremq@127.0.0.1:{port}/postgremq?sslmode=disable'
        connection = eventually(lambda: psycopg2.connect(dsn))
        connection.autocommit = True
        try:
            with connection.cursor() as cur:
                cur.execute((ROOT / 'mq/sql/latest.sql').read_text())
                cur.execute((ROOT / 'observability/demo-init.sql').read_text())
                cur.execute("SELECT postgremq.publish_message('demo','{}')")
                cur.execute("SELECT postgremq.publish_message('demo','{}',clock_timestamp()+interval '1 hour')")
            collector = client.containers.run(IMAGE, detach=True, network=network.name,
                command=['--config=/etc/otelcol/collector.yaml'],
                environment={
                    'POSTGREMQ_METRICS_DSN': f'postgres://postgremq_metrics:metrics@{db.name}:5432/postgremq?sslmode=disable&connect_timeout=5&statement_timeout=5000',
                    'POSTGREMQ_DATABASE_ID': 'collector-test',
                },
                volumes={str(ROOT / 'observability/collector.yaml'): {'bind': '/etc/otelcol/collector.yaml', 'mode': 'ro'}},
                ports={'8889/tcp': ('127.0.0.1', None), '4318/tcp': ('127.0.0.1', None)})
            containers.append(collector)
            collector.reload()
            ports = collector.attrs['NetworkSettings']['Ports']
            metrics_url = f"http://127.0.0.1:{ports['8889/tcp'][0]['HostPort']}/metrics"
            endpoint = f"http://127.0.0.1:{ports['4318/tcp'][0]['HostPort']}"

            def fetch():
                return urllib.request.urlopen(metrics_url, timeout=5).read().decode()

            def check_queue_metrics():
                text = fetch()
                def point(prefix, queue, value):
                    assert any(l.startswith(prefix + '{') and f'queue_name="{queue}"' in l
                               and f'topic_name="demo"' in l and l.endswith(f' {value}')
                               for l in text.splitlines()), (prefix, queue, text)
                # Both queues received fanout; check readiness separately from delays.
                point('postgremq_queue_messages_ready', 'demo', 1)
                point('postgremq_queue_messages_delayed', 'demo', 1)
                point('postgremq_queue_messages_processing', 'demo', 0)
                point('postgremq_queue_messages_dead_letter', 'demo', 0)
                point('postgremq_queue_messages_exhausted', 'demo', 0)
                point('postgremq_queue_active_ratio', 'demo', 1)
                assert 'postgremq_queue_oldest_ready_age_seconds{' in text
                assert 'service_instance_id="collector-test"' in text
            eventually(check_queue_metrics)
            # The scraper login cannot read payloads or publish messages.
            limited = eventually(lambda: psycopg2.connect(dsn.replace('postgres:postgremq@', 'postgremq_metrics:metrics@')))
            try:
                with limited.cursor() as cur:
                    cur.execute('SELECT * FROM postgremq.queue_metrics()')
                    assert len(cur.fetchall()) == 2
                    for forbidden in ('SELECT payload FROM postgremq.messages',
                                      "SELECT postgremq.publish_message('demo','{}')"):
                        try:
                            cur.execute(forbidden)
                        except psycopg2.errors.InsufficientPrivilege:
                            limited.rollback()
                        else:
                            raise AssertionError(f'scraper can execute: {forbidden}')
            finally:
                limited.close()
            env = {**os.environ, 'DATABASE_URL': dsn, 'OTEL_EXPORTER_OTLP_ENDPOINT': endpoint}
            for directory, command, timeout in [
                ('postgremq-go/examples/metrics', ['go', 'run', '.'], 120),
                ('postgremq-ts', ['npm', 'exec', '--', 'ts-node', 'examples/metrics.ts'], 120),
                ('postgremq-rs', ['cargo', 'run', '--locked', '--example', 'metrics', '--features', 'otel'], 900),
            ]:
                result = subprocess.run(command, cwd=ROOT / directory, env=env, text=True,
                                        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=timeout)
                assert result.returncode == 0, result.stdout
            def check_clients():
                text = fetch()
                for service in ('postgremq-go-example', 'postgremq-ts-example', 'postgremq-rs-example'):
                    for metric in ('messaging_client_operation_duration_seconds_count', 'messaging_process_duration_seconds_count',
                                   'messaging_client_sent_messages_total', 'messaging_client_consumed_messages_total', 'postgremq_client_handlers_active'):
                        assert any(l.startswith(metric + '{') and f'service_name="{service}"' in l for l in text.splitlines()), (metric, service, text)
                    for counter in ('messaging_client_sent_messages_total', 'messaging_client_consumed_messages_total'):
                        assert any(l.startswith(counter + '{') and f'service_name="{service}"' in l
                                   and l.endswith(' 1') and 'error_type=' not in l
                                   for l in text.splitlines()), (counter, service, text)

            eventually(check_clients)
            # Verify changes reach the next scrape, including zero for an empty queue.
            with connection.cursor() as cur:
                cur.execute('DELETE FROM postgremq.queue_messages')
            def check_empty():
                text = fetch()
                assert any(l.startswith('postgremq_queue_messages_ready{') and 'queue_name="demo"' in l and l.endswith(' 0') for l in text.splitlines()), text
            eventually(check_empty)
        finally:
            connection.close()
    except Exception:
        for container in containers:
            print(container.logs().decode(errors='replace')[-12000:])
        raise
    finally:
        for container in reversed(containers):
            container.remove(force=True)
        if network:
            network.remove()
        client.close()
