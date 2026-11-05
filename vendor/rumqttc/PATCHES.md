# Local dependency patch

Source: crates.io rumqttc 0.25.1, archive SHA-256 `0feff8d882bff0b2fddaf99355a10336d43dd3ed44204f85ece28cf9626ab519`.
Upstream repository: https://github.com/bytebeamio/rumqtt.

The upstream 0.25.1 manifest uses rustls-webpki 0.102.8, affected by
RUSTSEC-2026-0382 with no patched 0.102 release. This copy changes only that
dependency to 0.103.16 in the normalized and original manifests. Source code
and upstream license files are preserved. The crate uses webpki only as its
TLS error type; certificate verification remains owned by rustls.

Remove the path patch when a published rumqttc release permits a patched webpki.
Validate with cargo audit and the root crate's locked tests, including TLS integration.
