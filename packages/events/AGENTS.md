# @spider-mesh/events — Contributor Guide

```bash
bun install
bun run build
bun run test:e2e
```

- Keep this package independent of RPC and discovery implementations.
- Event identity defaults to the event class name; renaming is wire-breaking.
- Default delivery is single-transporter. Fanout must remain explicit.
- Mỗi event transporter tự hardcode `readonly name`; không suy luận từ tên class.
- `EventMeshHost` is a structural bridge; do not add a runtime dependency on core.
