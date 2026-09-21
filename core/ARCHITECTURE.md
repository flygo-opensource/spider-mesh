# Kiến trúc `@spider-mesh/core`

Core chỉ quản lý RPC runtime và model node. Mọi I/O nằm ở package transporter hoặc Discovery.

## Biên trách nhiệm

```text
Application
   |
SpiderMesh ── local services, request lifecycle, timeout/retry
   | \
   |  └── RpcTransporter[] ── HTTP/2, gRPC, raw TCP, WebSocket...
   |
   └── Topology? ── membership + endpoint state + request routing state
            |
            └── TopologyDiscovery? ── UDP, WebSocket, EndpointSlice...
```

### `SpiderMesh`

- Nhận `transporters` và `topology` optional trong constructor.
- Theo dõi `@Microservice()` và phát snapshot qua `localNode$`.
- Chọn transporter bằng wire name hoặc `canRoute()`.
- Correlate response, xử lý stream, cancel, retry và timeout.
- Không sở hữu discovery và không suy luận transporter bằng tên class.

### `Topology`

- Là một concrete class duy nhất dùng cho mọi môi trường.
- Nhận `discovery` trong constructor.
- Chứa local node và remote nodes trong cùng `nodes$`.
- Route khi request có `node_id` hoặc `routing`.
- Giữ round-robin/least-active state qua các request.
- Nhận reachability report từ transporter và loại endpoint lỗi khỏi candidate ngay.
- Yêu cầu Discovery xác minh node khi endpoint chuyển sang `unreachable`.
- Có thể evict announcement cũ bằng `staleAfterMs`.

`Registry` export lại đúng class này dưới tên cũ; không có runtime Registry thứ hai.

### `TopologyDiscovery`

Contract hai chiều:

```ts
type TopologyDiscovery = {
  bind(context: {
    localNode$: Observable<SpiderMeshNode>
    upsertRemote(node: SpiderMeshNode): void
    removeRemote(nodeId: string): void
  }): { unsubscribe(): void } | void
  verify?(nodeId: string): Promise<'alive' | 'dead' | 'unknown'>
  close?(): void | Promise<void>
}
```

Discovery quyết định membership. Transporter quan sát kết nối thật và báo endpoint reachability
cho Topology. Topology chỉ xóa node sau khi Discovery trả `dead` hoặc snapshot hết TTL.

```text
HTTP/2 connection lost
        |
        v
Http2Rpc -> Topology.reportReachability(unreachable)
        |          |
        |          └─ loại endpoint khỏi routing, phát event ngay
        v
Discovery.verify(node)
   dead    -> xóa membership
   alive   -> giữ membership; transporter tiếp tục reconnect
   unknown -> giữ membership; heartbeat/TTL hoặc offline frame quyết định sau
```

`Topology.events$` là kênh thay đổi tức thời cho availability. `nodes$` chỉ biểu diễn membership,
vì thế mất một endpoint không làm biến mất node có thể vẫn dùng protocol khác.

### `RpcTransporter`

```ts
type RpcTransporter = Observable<RpcEvent> & {
  readonly name: string
  start?(context: RpcTransporterContext): void | Promise<void>
  stop?(): void | Promise<void>
  send(packet: RpcRequestPacket | RpcResponsePacket | RpcCancelPacket): Promise<{ cancel(): void }>
  canRoute?(service: string, nodeId?: string): boolean
  probe?(request: RpcProbeRequest): Promise<RpcProbeResult>
}
```

`name` là wire identifier ổn định (`http2`, `grpc`, `raw-tcp`, `websocket`). Metadata endpoint
trên node luôn dùng đúng key này.

## Hai chế độ vận hành

| Môi trường | Topology | Ai route | Availability |
| --- | --- | --- | --- |
| Kubernetes Service | Không cần | K8s Service/mesh | `probe()` |
| WebSocket relay | Không cần | Relay | relay directory qua `probe()` |
| PM2/pure Linux | Có | Topology + transporter | membership + `canRoute()` |
| K8s per-Pod routing | Có | Topology + transporter | discovery + `canRoute()` |

## Invariants

- `LOCAL_SERVICES$` là process-global; một process nên đại diện một logical node.
- Service identity mặc định là class name; rename/minify class là wire-breaking.
- Discovery snapshot là authoritative full snapshot.
- Transporter không được tự xóa membership; nó chỉ báo reachability của endpoint mình sở hữu.
- Trạng thái endpoint chưa từng được báo mặc định là reachable để tương thích transporter cũ.
- Routing config thuộc request, không thuộc Topology constructor.
- Không có Topology thì Core không bịa danh sách node từ kết quả probe.
- Source là ESM và relative import dùng đuôi `.js`.

## Thứ tự đọc source

1. `src/types.ts`
2. `src/SpiderMesh.ts`
3. `src/Topology.ts`
4. `src/RemoteService.ts`
5. `src/decorators/Microservice.ts`
