# Security Policy

## Supported Versions

Only the latest release receives security fixes.

| Version | Supported |
| ------- | --------- |
| latest release | ✅ |
| older releases | ❌ |

## Reporting a Vulnerability

Please report vulnerabilities privately via
[GitHub's private vulnerability reporting](https://github.com/briananderson1222/mqtt-recorder/security/advisories/new)
("Report a vulnerability" on the Security tab). Do not open a public issue
for security problems.

You can expect an acknowledgment within a week. This is a single-maintainer
project, so fixes are best-effort but security reports are prioritized.

## Security Model — What to Know Before Deploying

- **The embedded broker (`--serve`) has no authentication and no TLS.** It
  binds to `127.0.0.1` by default. If you pass `--bind-addr` to expose it on
  a network interface, every host that can reach the port can read all
  mirrored/replayed traffic and publish arbitrary messages. Only do this on
  trusted networks.
- **Recorded CSVs and audit logs may contain sensitive payloads.** They are
  created with mode `0600` on Unix; treat the files accordingly when copying
  or sharing them.
- **`--tls-insecure` disables server certificate verification** for external
  broker connections. The connection stays encrypted, but the peer is not
  authenticated — never use it where man-in-the-middle is a concern.
- Prefer `MQTT_PASSWORD`/`MQTT_USERNAME` environment variables over the
  `--password`/`--username` flags; flags are visible in `ps` output and shell
  history.

## Dependency Auditing

CI runs `cargo audit` on every push/PR and on a monthly schedule. Advisories
that cannot be fixed from this crate (transitive pins in upstream MQTT
libraries) are documented with justification in `.cargo/audit.toml`.
