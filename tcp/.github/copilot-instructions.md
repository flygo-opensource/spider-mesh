# Spider Mesh TCP Copilot Instructions

Use these instructions when editing or generating code in this repository.

## Current API

- Register `UdpDiscovery`, `Http2Rpc`, and `Http2Pubsub` separately on `SpiderMesh`.
- Use `new SpiderMesh(new Registry())` for normal TCP runtime setup.
- `Http2Rpc.send(packet, node_id?)` routes by peer id.
- `Http2Pubsub` publishes to topic subscribers resolved from the linked registry.

## Runtime Semantics

- `UdpDiscovery` rebroadcasts full node metadata over multicast.
- `Http2Rpc` emits `rpc`, `offline`, and `endpoints` events.
- `Http2Pubsub.listen(topic)` is local-stream based; remote fan-out happens on `publish()`.

## Source Of Truth

- `src/UdpDiscovery.ts`
- `src/Http2Rpc.ts`
- `src/Http2Pubsub.ts`
- `src/runtime.ts`
- `src/types.ts`

## Testing Guidance

- Validate with `bun run build`.
- Prefer the e2e tests under `tests/` for behavior changes.
- Run `bun run test:e2e` when changing transporter behavior.