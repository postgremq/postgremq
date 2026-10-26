"""End-to-end: real pinned Collector + PostgreSQL + the three client examples,
and the Grafana dashboards' metric names against what they export.

Run from the repository root:
  .venv/bin/pytest observability/test_collector.py -v
Requires mq/tests/requirements.txt, Docker, Go, npm ci in postgremq-ts, and a
Rust toolchain (the Rust example is built on first run if not built already).
All containers, networks and databases are isolated and removed on exit.
"""
import json
import os
from pathlib import Path
import re
import subprocess
import time
import urllib.request
import uuid

import docker
import psycopg2

ROOT = Path(__file__).resolve().parents[1]
IMAGE = 'otel/opentelemetry-collector-contrib:0.147.0'
DASHBOARDS = ROOT / 'observability/dashboards/grafana'
# PromQL words that are not metric names, once strings, label matchers,
# ranges and grouping label lists are removed.
KEYWORDS = {'and', 'or', 'unless', 'bool', 'offset', 'by', 'without', 'on', 'ignoring',
            'group_left', 'group_right', 'inf', 'nan'}
# Contract counters the client examples never increment: the drift check
# takes their names from client-contract.json instead of the scenario.
UNPRODUCED_COUNTERS = ['postgremq.client.renewal.lost']


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


def dashboards():
    return {path.name: json.loads(path.read_text()) for path in sorted(DASHBOARDS.glob('*.json'))}


def queries(node):
    """Every PromQL query in a dashboard: panel targets and variable queries
    (both the stored `query` and its `definition`)."""
    if isinstance(node, dict):
        if node.get('type') == 'datasource':  # its query is a plugin id
            return
        for key, value in node.items():
            if key in ('expr', 'definition', 'query') and isinstance(value, str):
                yield value
            else:
                yield from queries(value)
    elif isinstance(node, list):
        for item in node:
            yield from queries(item)


def metric_names(query):
    assert not re.search(r'__name__\s*(=~|!=|!~)', query), f'unsupported metric name matcher: {query}'
    selected = set(re.findall(r'__name__\s*=\s*"([^"]+)"', query))
    query = re.sub(r'"(?:[^"\\]|\\.)*"', '""', query)   # string literals
    query = re.sub(r'\{[^}]*\}|\[[^]]*\]', '', query)       # label matchers, ranges
    query = re.sub(r'\b(by|without|on|ignoring|group_left|group_right)\s*\([^)]*\)', '', query)
    query = re.sub(r'label_values\(([^,)]*),[^)]*\)', r'\1', query)
    query = re.sub(r'\$\w+', '', query)                      # dashboard variables
    # Not inside a number or duration (0.95, 5m, 1e3).
    names = re.findall(r'(?<![\w.:])([A-Za-z_:][\w:]*)\s*(\()?', query)
    return selected | {name for name, call in names if not call and name.lower() not in KEYWORDS}


def dashboard_metrics():
    names = {name for board in dashboards().values() for query in queries(board) for name in metric_names(query)}
    assert names, 'no metric names found in the dashboards'
    return names


def unproduced_series():
    """Prometheus names of the contract counters the client examples never
    increment (classic translation of a `{unit}` counter: underscores, _total)."""
    contract = json.loads((ROOT / 'observability/client-contract.json').read_text())['metrics']
    assert set(UNPRODUCED_COUNTERS) <= contract.keys()
    return {name.replace('.', '_') + '_total' for name in UNPRODUCED_COUNTERS}


def test_metric_names_are_extracted_from_promql():
    assert metric_names('sum by (le, messaging_destination_name) (rate(a_bucket{x="b{c}", y=~"$v"}[$__rate_interval]))'
                        ' and on (messaging_destination_name) label_replace(b_ratio{q="1"}, "d", "$1", "q", "(.+)")'
                        ' / ignoring(le) c_total offset 5m > NaN') == {'a_bucket', 'b_ratio', 'c_total'}
    assert metric_names('label_values(m_ratio{a=~"$a", b="x"}, messaging_destination_name)') == {'m_ratio'}
    assert metric_names('sum(rate({__name__="d_total", a="b"}[5m]))') == {'d_total'}
    assert set(queries({'templating': {'list': [
        {'type': 'datasource', 'query': 'prometheus'},
        {'type': 'query', 'definition': 'label_values(e_ratio, a)', 'query': {'query': 'label_values(f_ratio, a)'}},
    ]}})) == {'label_values(e_ratio, a)', 'label_values(f_ratio, a)'}


def test_dashboards_link_to_each_other():
    boards = dashboards()
    assert set(boards) == {'postgremq-overview.json', 'postgremq-topic.json', 'postgremq-queue.json',
                           'postgremq-clients.json'}
    uids = {board['uid'] for board in boards.values()}
    assert len(uids) == len(boards)
    links = {(name, uid) for name, board in boards.items()
             for uid in re.findall(r'/d/([^/?"]+)', json.dumps(board))}
    assert {('postgremq-overview.json', 'postgremq-topic'), ('postgremq-overview.json', 'postgremq-queue'),
            ('postgremq-topic.json', 'postgremq-queue')} <= links
    assert {uid for _, uid in links} <= uids
    for board in boards.values():
        # Portable: every query names the datasource through the dashboard's variable.
        assert '__inputs' not in board
        datasources = re.findall(r'"datasource": (\{[^}]*\})', json.dumps(board))
        assert datasources and all('${datasource}' in d or '-- Grafana --' in d for d in datasources)


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
                                   'messaging_client_sent_messages_total', 'messaging_client_consumed_messages_total', 'postgremq_client_handlers_active',
                                   'postgremq_client_first_delivery_wait_seconds_count'):
                        assert any(l.startswith(metric + '{') and f'service_name="{service}"' in l for l in text.splitlines()), (metric, service, text)
                    for counter in ('messaging_client_sent_messages_total', 'messaging_client_consumed_messages_total'):
                        assert any(l.startswith(counter + '{') and f'service_name="{service}"' in l
                                   and l.endswith(' 1') and 'error_type=' not in l
                                   for l in text.splitlines()), (counter, service, text)

            eventually(check_clients)
            # Drift: every metric a dashboard queries is exported by the scenario,
            # or is a contract counter the examples never increment.
            exported = {line.split('{')[0].split(' ')[0] for line in fetch().splitlines() if not line.startswith('#')}
            unexported = dashboard_metrics() - exported
            assert unexported <= unproduced_series(), f'dashboard metrics not exported: {unexported - unproduced_series()}'
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
