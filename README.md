# `@spider-mesh/tcp`

HTTP/2 RPC transporter và brokerless HTTP/2 event transporter cho Spider Mesh.

```bash
bun add @spider-mesh/core @spider-mesh/tcp rxjs reflect-metadata
```

## RPC transporter

`Http2Rpc` có wire name hardcode là `http2`. Constructor chỉ nhận network option; Topology
được Core truyền qua lifecycle `start()`.

### Pure Linux / PM2

```ts
const topology = new Topology({
  discovery: new TopologyDiscoveryAdapter(udpDiscovery, {
    heartbeatIntervalMs: 5_000,
  }),
  staleAfterMs: 15_000,
})

const mesh = new SpiderMesh({
  topology,
  transporters: [new Http2Rpc()],
})
```

Topology chọn node; `Http2Rpc` kết nối trực tiếp tới `node.host` và
`node.transporters.http2.port`. Nếu request không chỉ định routing, direct HTTP/2 dùng
round-robin mặc định.

### Hạ tầng tự route

Không cần Topology khi resolver trả về một load-balanced endpoint:

```ts
const mesh = new SpiderMesh({
  transporters: [new Http2Rpc({
    port: 8080,
    resolveService: service => ({
      host: `${service.toLowerCase()}.internal`,
      port: 8080,
    }),
  })],
})
```

Trong chế độ này `wait()` dùng HTTP/2 `probe()`, còn hạ tầng quyết định backend thật.

## Per-request routing

```ts
const users = RemoteServiceLinker.link<UserService>(mesh, {
  service: 'UserService',
  transporter: 'http2',
  routing: { strategy: 'consistent-hash', key: tenantId },
})
```

`Http2Rpc` báo `suspect`, `unreachable` và `reachable` vào Topology. Topology loại endpoint mất kết
nối khỏi candidate ngay, phát event cho `watch()`, rồi nhờ Discovery xác minh node. `Http2Rpc`
không xóa membership; Discovery vẫn là nguồn duy nhất quyết định node tồn tại hay không.

## Event transporter

`Http2Pubsub` có wire name `http2-pubsub` và cần Topology để enumerate node đã subscribe topic:

```ts
const events = new EventBus({ mesh })
events.registerTransporter(new Http2Pubsub(topology))
```

RPC và event là hai transporter độc lập; ứng dụng chỉ import phần cần dùng.

## API

```ts
new Http2Rpc({
  port?: number
  resolveService?: (service: string) => { host: string; port: number } | undefined
})
```

Metadata được quảng bá dưới key `http2`; package không dùng tên class làm protocol identifier.

| Export | Wire name | Topology | Vai trò |
| --- | --- | --- | --- |
| `Http2Rpc` | `http2` | Optional | RPC qua HTTP/2 |
| `Http2Pubsub` | `http2-pubsub` | Bắt buộc | Brokerless event tới subscriber nodes |

## Ví dụ hoàn chỉnh

- `examples/src/rpc/http2-pm2`: direct node routing với UDP Discovery + Topology.
- `examples/src/rpc/http2-k8s`: resolver tới Kubernetes Service, không Topology.

`Http2Rpc` giống nhau ở cả hai môi trường; chỉ cách resolve target thay đổi.

## Chạy thử

```bash
bun run build
bun run test:e2e
```
