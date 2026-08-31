# agentd-hub

`agentd-hub` is a local, read-only multi-machine aggregator for
[Agentd](https://github.com/clickety-clacks/agentd). It discovers machines from
Tailscale or a hosts file, runs the documented Agentd CLI through existing SSH
access, and serves complete current snapshots on one loopback HTTP listener.

The current pre-release candidate version is 0.0.0. The reviewed release flow
will make a dedicated version change to 0.1.0 before tagging `v0.1.0`.

## Build and test

Rust 1.97 or later is required.

```sh
cargo build --release --locked
cargo test --locked --all-targets
```

## Run

By default, the hub listens only on `127.0.0.1:8787`.

```sh
agentd-hub
agentd-hub --hosts-file ./hosts.txt
agentd-hub --listen '[::1]:8787' --hosts-file ./hosts.txt
```

Tailscale discovery runs first. The hosts file is used only when Tailscale
fails, returns invalid JSON, or returns no machine. Each non-empty,
non-comment line is one SSH target.

The server exposes only:

- `GET /snapshot` for one complete JSON snapshot.
- `GET /events` for complete-snapshot SSE events.
- `GET /` for the self-contained text-only roster page.

The hub has no authentication and refuses non-loopback listeners. A deployment
that exposes the listener owns that choice. The hub stores no roster, event, or
revision history and sends no commands to Agentd or agents.

## Package a candidate

Build the locked release binary, then create the deterministic archive and
checksum receipt:

```sh
cargo build --release --locked
scripts/package-release.sh --dry-run
scripts/package-release.sh
```

The package command writes
`target/release-assets/agentd-hub-0.0.0-<rust-host>.tar.gz` and
`target/release-assets/SHA256SUMS`. It does not publish a release.
