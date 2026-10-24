# grafana.com listings

The four dashboards are uploaded to grafana.com by hand after a change is
merged: sign in to grafana.com, open **My Account → Dashboards**, and upload
the JSON file as a new revision of its listing (or as a new dashboard the
first time). Use the text below for each listing and the screenshot next to
this file, taken from the compose demo with `observability/loadgen` running
(docs/observability.md).

All four listings share:

- **Requirements**: Grafana 11.6 or later (tested with 11.6.0 and 13.2.3), a
  Prometheus datasource, and the PostgreMQ metrics translated by the
  OpenTelemetry Collector's Prometheus exporter (or Prometheus's OTLP receiver
  with `service.name` and `service.instance.id` promoted to labels).
- **Collector configuration**:
  https://github.com/postgremq/postgremq/blob/main/observability/collector.yaml
- **Documentation**:
  https://github.com/postgremq/postgremq/blob/main/docs/observability.md#grafana-dashboards
- **Tags**: postgremq, postgresql, message queue, opentelemetry

## PostgreMQ Overview

File: `postgremq-overview.json` · Screenshot: `postgremq-overview.png`

> Queue state of PostgreMQ (a PostgreSQL message queue) databases: queue
> count, ready, processing, delayed, exhausted and dead-letter totals, the
> oldest ready message, backlog by queue and topic, and the top queues by
> backlog, age and dead letters. Links to the PostgreMQ Topic and Queue
> dashboards. Built on `postgremq.queue_metrics()`, scraped by the
> OpenTelemetry Collector's SQL Query receiver.

## PostgreMQ Topic

File: `postgremq-topic.json` · Screenshot: `postgremq-topic.png`

> One PostgreMQ topic: the state of every queue it fans out to, its
> producers' publish rate, latency, failures and transactional share, and
> consume, ack and handler latency per queue. Producer and consumer panels need
> applications instrumented with the PostgreMQ client's OpenTelemetry metrics.

## PostgreMQ Queue

File: `postgremq-queue.json` · Screenshot: `postgremq-queue.png`

> One PostgreMQ queue over time: ready, delayed, processing, exhausted and
> dead-letter messages and the oldest ready age, with its consumers' delivery,
> settlement and redelivery rates, handler duration and errors, active
> handlers and lease-renewal loss (from the client OpenTelemetry metrics).

## PostgreMQ Clients

File: `postgremq-clients.json` · Screenshot: `postgremq-clients.png`

> PostgreMQ client activity per application and instance, from the Go,
> TypeScript and Rust clients' OpenTelemetry metrics: operation rate, latency
> and errors, sent and consumed messages, handler concurrency, duration and
> errors, and the background lease-renewal and keep-alive calls.
