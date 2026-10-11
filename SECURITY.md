# Security Policy

## Supported Versions

PostgreMQ is pre-1.0. Security fixes land on the latest release of each
component (SQL schema, Go client, TypeScript client, Rust client, CLI); older
releases are not patched.

## Reporting a Vulnerability

**Please do not open a public issue, discussion or pull request for a security
vulnerability.**

Report it privately through GitHub's private vulnerability reporting:

1. Open the repository's **Security** tab.
2. Click **Report a vulnerability**.
3. Describe the issue.

Please include, where you can:

- the affected component(s) and version or commit;
- the type of issue (for example SQL injection, privilege escalation,
  message disclosure across queues, denial of service);
- steps to reproduce, or a proof of concept;
- the impact as you understand it, and any suggested fix.

### What to Expect

This project is maintained on a best-effort basis. You can expect:

- an acknowledgment of your report, normally within a week;
- an assessment of severity and a plan, shared through the advisory thread;
- coordinated disclosure: a fix is released before the advisory is published,
  and you are credited in the advisory unless you prefer otherwise.

## Security Model

PostgreMQ is a set of SQL functions and tables plus thin clients. It runs with
the privileges of the database role the application connects as and adds no
authentication or authorization layer of its own.

- **Database access is the trust boundary.** Any role that can read the
  `postgremq` schema can read every message payload; any role that can execute
  its functions can publish, consume, settle and delete queues. Isolate
  PostgreMQ in a database or schema reachable only by trusted roles, and grant
  the least privilege each application needs.
- **Payloads are stored as-is.** PostgreMQ does not encrypt payloads. Use
  application-level encryption for sensitive data, and TLS for database
  connections.
- **Delivery tokens are ownership tokens, not credentials.** Each delivery gets
  a fresh `gen_random_uuid()` token so a stale consumer cannot settle a
  redelivered message. A token does not authorize anything beyond what the
  database role can already do.
- **Message IDs are sequential** (`BIGSERIAL`); do not treat them as secret.
- **Names are parameters.** All clients pass topic, queue and payload values as
  bind parameters. Still, avoid passing untrusted input straight through as
  topic or queue names.
- **Resource limits are the application's job.** PostgreMQ does not cap payload
  size, publish rate or queue depth. Enforce limits in the application and
  monitor queue depth (see `docs/observability.md`).

## Security Updates

Fixes are announced through GitHub Security Advisories and noted in
`CHANGELOG.md` and the GitHub release notes.
