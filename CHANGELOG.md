# Changelog

Each component is versioned and released on its own (see
[RELEASE.md](./RELEASE.md)):

| Component | Changelog | Tags |
|-----------|-----------|------|
| SQL schema (mq) | the `mq/vN` [GitHub releases](https://github.com/postgremq/postgremq/releases) | `mq/vN` (N = last migration) |
| Go client and CLI | [postgremq-go/CHANGELOG.md](./postgremq-go/CHANGELOG.md) | `postgremq-go/vX.Y.Z` |
| Rust client | [postgremq-rs/CHANGELOG.md](./postgremq-rs/CHANGELOG.md) | `rust/vX.Y.Z` |
| TypeScript client | [postgremq-ts/CHANGELOG.md](./postgremq-ts/CHANGELOG.md) | `npm/vX.Y.Z` |

GitHub releases carry the same notes per tag.

## Shared, unversioned parts

### Observability

- A shared client metrics contract (`docs/observability.md`,
  `observability/client-contract.json`), a tested OpenTelemetry Collector
  configuration, and runnable metrics examples for every client.
- Grafana dashboards (Overview, Topic, Queue, Clients) in
  `observability/dashboards/grafana/`, with Prometheus and Grafana in the
  compose demo and a continuous demo workload (`observability/loadgen`).
