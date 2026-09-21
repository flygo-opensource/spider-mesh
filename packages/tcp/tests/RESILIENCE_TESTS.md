# TCP resilience tests

Run all fault scenarios:

```bash
bun run test:resilience
```

Coverage:

1. Full metadata replacement removes stale services and transporter endpoints.
2. Bounded reconnect preserves Topology membership; only a new endpoint snapshot starts another cycle.
3. Parameterized soak with repeated provider restarts and pinned RPC calls.
4. Provider `SIGKILL`, endpoint unreachable state, and one-shot rediscovery after restart.
5. Duplicate `node_id` processes must not merge into a synthetic peer.
6. Interrupted RPC stream emits exactly one terminal `MICROSERVICE_OFFLINE` error.

UDP packet/auth/socket behavior is tested by `@simple-discovery/udp`; TCP resilience no longer owns UDP
heartbeat, TTL, partition, socket-restart, or legacy-wire scenarios.

The standard test uses a 30-second soak. Run an accelerated longer profile directly:

```bash
SPIDERMESH_NAMESPACE=soak-long \
SIMPLE_DISCOVERY_PORT=27888 \
SPIDERMESH_HTTP2_RECONNECT_ATTEMPTS=2 \
SPIDERMESH_HTTP2_RECONNECT_DELAY_MS=50 \
SPIDERMESH_HTTP2_CONNECT_TIMEOUT_MS=500 \
SOAK_PROVIDER_COUNT=20 \
SOAK_DURATION_MS=43200000 \
SOAK_RESTART_EVERY_MS=1000 \
bun run examples/resilience/soak-test.ts
```

`43200000` is 12 hours. Use `86400000` for 24 hours.
