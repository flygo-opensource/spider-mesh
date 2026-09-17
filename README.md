# `@spider-mesh/ws`

WebSocket RPC/event transporter, relay server và discovery-only server/client cho Spider Mesh.

```bash
bun add @spider-mesh/core @spider-mesh/ws rxjs
```

## Relay tự route, không cần Topology

```ts
import { SpiderMesh } from '@spider-mesh/core'
import { WebsocketTransporter } from '@spider-mesh/ws/node'

const websocket = new WebsocketTransporter()
websocket.connect('ws://relay.internal:8787')

const mesh = new SpiderMesh({
  transporters: [websocket],
})
```

Transporter có name hardcode `websocket`. Nó quảng bá local node, giữ directory nhận từ relay,
route RPC và implement `probe()` để `wait()` hoạt động khi không có Topology.

## Khi cần enumerate/watch node

Cho cùng transporter làm Discovery của Topology:

```ts
const websocket = new WebsocketTransporter()
websocket.connect('ws://relay.internal:8787')

const topology = new Topology({ discovery: websocket })
const mesh = new SpiderMesh({
  topology,
  transporters: [websocket],
})
```

Chỉ bật Topology khi cần `nodes`, `watch()`, batch RPC, pin node hoặc request-level routing.
Khi đã kết nối relay, transporter cũng implement `TopologyDiscovery.verify(nodeId)` bằng directory
hiện tại: `alive` nếu node còn trong directory và `dead` nếu relay xác nhận node đã biến mất.
Nếu relay đang offline, kết quả là `unknown` để Topology không xóa membership nhầm.
Mất toàn bộ relay làm endpoint WebSocket chuyển `unreachable` ngay, nhưng không tự xóa node;
offline frame từ relay mới là bằng chứng thay đổi membership.

## Relay server

```ts
import { WebsocketRelayServer } from '@spider-mesh/ws/relay-server'

const relay = new WebsocketRelayServer({ port: 8787 })
```

Relay giữ service index và round-robin các request không có `destination_node_id`. Khi request
đã được Topology route, relay chuyển packet đến đúng node.

## Nhiều runtime

- `@spider-mesh/ws/node`: dùng package `ws`.
- `@spider-mesh/ws/browser`: dùng `globalThis.WebSocket`.
- `@spider-mesh/ws/react-native`: dùng `globalThis.WebSocket`.
- `@spider-mesh/ws/relay-server`: WebSocket relay.

| Import | Class chính | Chạy ở đâu |
| --- | --- | --- |
| `@spider-mesh/ws/node` | `WebsocketTransporter` | Node.js/Bun server |
| `@spider-mesh/ws/browser` | `WebsocketTransporter` | Browser |
| `@spider-mesh/ws/react-native` | `WebsocketTransporter` | React Native |
| `@spider-mesh/ws/relay-server` | `WebsocketRelayServer` | Node.js/Bun relay |

## Event

Cùng một socket có thể đăng ký riêng với EventBus:

```ts
const events = new EventBus({ mesh })
events.registerTransporter(websocket)
```

RPC ownership thuộc SpiderMesh; event ownership thuộc EventBus.

## Ví dụ hoàn chỉnh

Workspace `examples/src/rpc/websocket-relay` có ba process relay/provider/client và cố ý không
dùng Topology. Các ví dụ trong package `examples/` của repo này bao phủ thêm reconnect, failover,
round-robin, browser-compatible protocol và chế độ có Topology.

## Test

```bash
bun run build
bun run test:e2e
```
