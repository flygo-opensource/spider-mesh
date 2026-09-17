# Spider Mesh Core

`@spider-mesh/core` là runtime RPC độc lập hạ tầng. Core không biết UDP, WebSocket,
Kubernetes, Redis hay NATS; các package ngoài đưa khả năng mạng vào qua transporter và
Discovery.

## Thành phần

| Thành phần | Trách nhiệm |
| --- | --- |
| `SpiderMesh` | Quản lý service local, RPC lifecycle, retry, timeout và transporter |
| `Topology` | Lưu membership, trạng thái endpoint và routing state dùng chung |
| `RpcTransporter` | Truyền packet; tự khai báo `public readonly name` |
| `TopologyDiscovery` | Đưa remote node vào Topology và phát local node ra ngoài |
| `RemoteServiceLinker` | Tạo typed proxy để gọi remote service |

`Topology` là optional. `Registry` chỉ còn là alias chuyển tiếp của `Topology` để code cũ
có thể nâng cấp dần.

## Chọn cấu hình nhanh

| Hạ tầng | Có cần Topology? | Availability | Routing |
| --- | --- | --- | --- |
| K3s/K8s có Service | Không | `transporter.probe()` | Kubernetes Service |
| WebSocket relay | Không | relay directory qua `probe()` | Relay |
| Linux/PM2 kết nối trực tiếp | Có | membership + `canRoute()` | Topology theo request |
| K8s cần chọn từng Pod | Có | Pod discovery + `canRoute()` | Topology theo request |

## Cài đặt

```bash
bun add @spider-mesh/core rxjs reflect-metadata
```

## Không cần Topology

Phù hợp khi hạ tầng hoặc transporter tự route, ví dụ Kubernetes Service hay WebSocket relay:

```ts
const mesh = new SpiderMesh({
  transporters: [new MyRpcTransporter()],
})
```

Trong chế độ này, `wait()` poll `transporter.probe()`. Core không giả lập danh sách node,
nên `nodes`, `watch()` và batch enumeration không có remote node để trả về.

## Có Topology

Phù hợp cho pure Linux/PM2 hoặc khi ứng dụng cần chọn Pod/node cụ thể:

```ts
import { SpiderMesh, Topology, type SpiderMeshNode } from '@spider-mesh/core'
import { TopologyDiscoveryAdapter } from '@spider-mesh/discovery'
import { UdpDiscovery } from '@ohayo/udp'
import { Http2Rpc } from '@spider-mesh/tcp'

const udp = new UdpDiscovery<SpiderMeshNode>({
  namespace: 'default',
  tags: ['spider-mesh', 'node'],
})

const topology = new Topology({
  discovery: new TopologyDiscoveryAdapter(udp, {
    // UDP không có offline frame nên phát lại snapshot để TTL không xóa nhầm node còn sống.
    heartbeatIntervalMs: 5_000,
  }),
  staleAfterMs: 15_000,
})

const mesh = new SpiderMesh({
  topology,
  transporters: [new Http2Rpc()],
})
```

Luồng dữ liệu:

```text
SpiderMesh localNode$ -> Topology -> Discovery -> hạ tầng
hạ tầng -> Discovery -> Topology -> transporter routing
```

Local node cũng nằm trong Topology để local service có thể được route giống remote service.

`TopologyDiscovery` là interface nằm trong Core vì `Topology` cần contract đó. Adapter cụ thể
không nằm trong Core: `TopologyDiscoveryAdapter` biết discovery envelope và
`DiscoveryTransporter`, nên thuộc `@spider-mesh/discovery`. Một Discovery chuyên biệt cũng có thể
implement thẳng `TopologyDiscovery` mà không cần adapter.

## Viết transporter

Mỗi transporter sở hữu một wire name ổn định:

```ts
class GrpcRpcTransporter extends Subject<RpcEvent> implements RpcTransporter {
  public readonly name = 'grpc'

  async send(packet: RpcRequestPacket | RpcResponsePacket | RpcCancelPacket) {
    return { cancel: () => {} }
  }

  canRoute(service: string, nodeId?: string) {
    return true
  }

  async probe({ service }: RpcProbeRequest) {
    return { reachable: await pingService(service) }
  }
}
```

Không truyền tên lúc đăng ký và không dùng `constructor.name`:

```ts
const mesh = new SpiderMesh({
  transporters: [new Http2Rpc(), new GrpcRpcTransporter()],
})
```

Hai transporter cùng tên sẽ bị từ chối ngay.

## Service local và remote proxy

```ts
@Microservice({ version: '1.0.0' })
class UserService {
  async getUser(id: string) {
    return { id, name: 'Ada' }
  }
}

new UserService()

type UserServiceContract = {
  getUser(id: string): Promise<{ id: string; name: string }>
}

const users = RemoteServiceLinker.link<UserServiceContract>(mesh, {
  service: 'UserService',
  timeout: 3_000,
  retry: 2,
})

await users.wait()
const user = await users.getUser('42')
```

Remote method vừa là Observable vừa có thể `await`.

## Routing theo request

Routing không phải cấu hình của Topology. Mỗi request tự chọn strategy:

```ts
await users.set({
  transporter: 'http2',
  routing: { strategy: 'round-robin' },
}).getUser('42')

await users.set({
  routing: { strategy: 'consistent-hash', key: 'tenant-42' },
}).getUser('42')
```

Các strategy có sẵn: `round-robin`, `random`, `consistent-hash`, `least-active`.
Nếu request không có `routing` và `node_id`, transporter/hạ tầng được quyền tự route.

## Availability và reachability

Availability nghĩa là service có endpoint mà ít nhất một transporter hiện route được:

- Có Topology: Core kết hợp membership với trạng thái endpoint do transporter báo.
- Không có Topology: Core dùng `probe()` khi `wait()`.
- `watch()`/`nodes`/`__batch__*` cần Topology vì phải enumerate node thật.

Membership và reachability cố ý là hai state khác nhau:

| State | Ai cập nhật | Ý nghĩa |
| --- | --- | --- |
| Membership | Discovery | Node có còn thuộc mesh hay không |
| Reachability | Transporter -> Topology | Endpoint của một protocol đang gọi được hay không |

Khi HTTP/2 mất kết nối, transporter gọi `topology.reportReachability()` để loại endpoint khỏi
routing ngay. Nếu Discovery có `verify(nodeId)`, Topology đồng thời yêu cầu kiểm tra lại node.
Chỉ kết quả `dead` từ Discovery mới xóa membership; `unknown` giữ node để tránh xóa nhầm do lỗi
mạng tạm thời.

```ts
topology.events$.subscribe(event => {
  if (event.type === 'endpoint-unreachable') {
    console.warn('Endpoint mất kết nối:', event.node_id, event.transporter)
  }
})
```

Các event chính: `node-online`, `node-offline`, `endpoint-suspect`,
`endpoint-unreachable`, `endpoint-recovered`.

## Public API chính

| Export | Ý nghĩa |
| --- | --- |
| `SpiderMesh` | RPC runtime và lifecycle transporter |
| `Topology` | Kho membership, endpoint state và request-routing state optional |
| `TopologyDiscovery` | Port để Discovery publish/ingest và xác minh node |
| `RpcTransporter` | Contract data plane RPC |
| `Microservice` | Đăng ký instance service local |
| `RemoteServiceLinker` | Tạo typed remote proxy |
| `Registry` | Alias deprecated của `Topology` |

## Ví dụ hoàn chỉnh

Workspace `examples/src/rpc` có ba mô hình độc lập:

- `http2-pm2`: Core + Topology + Discovery + HTTP/2.
- `http2-k8s`: Core + HTTP/2 + Kubernetes resolver, không Topology.
- `websocket-relay`: Core + WebSocket transporter, relay tự route.

## Phát triển

```bash
bun run build
bun run test:e2e
```
