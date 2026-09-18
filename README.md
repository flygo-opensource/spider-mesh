# @spider-mesh/ws

Transporter WebSocket cho Spider Mesh, kèm relay server. Mọi node kết nối tới một relay; relay biết
node nào phục vụ service nào và tự chuyển request tới đó, nên node không cần biết địa chỉ của nhau.
Chạy được trên Node/Bun, trình duyệt và React Native.

## Cài đặt

```bash
bun add @spider-mesh/core @spider-mesh/ws rxjs
```

Cách khai báo và gọi service được mô tả trong `@spider-mesh/core`.

| Import | Class | Dùng ở |
| --- | --- | --- |
| `@spider-mesh/ws/relay-server` | `WebsocketRelayServer` | Node/Bun (process relay) |
| `@spider-mesh/ws/node` | `WebsocketTransporter` | Node/Bun |
| `@spider-mesh/ws/browser` | `WebsocketTransporter` | Trình duyệt |
| `@spider-mesh/ws/react-native` | `WebsocketTransporter` | React Native |

Gói không có import gốc (`@spider-mesh/ws`); luôn import theo đường dẫn con ở trên.

## 1. Chạy relay

```ts
import { WebsocketRelayServer } from '@spider-mesh/ws/relay-server'

const relay = new WebsocketRelayServer({ port: 8787 })
console.log('relay listening on', relay.port)
```

| Tuỳ chọn | Mặc định | Ý nghĩa |
| --- | --- | --- |
| `port` | `8787` | Cổng lắng nghe. `0` để hệ điều hành chọn cổng trống. |
| `host` | mọi interface | Địa chỉ lắng nghe. |
| `path` | mọi path | Chỉ nhận kết nối tới path này. |
| `isServerConnection` | mọi kết nối là server | Phân loại kết nối, xem [Kết nối không tin cậy](#kết-nối-không-tin-cậy). |

`relay.close()` dừng relay.

## 2. Provider

```ts
import { Microservice, SpiderMesh } from '@spider-mesh/core'
import { WebsocketTransporter } from '@spider-mesh/ws/node'

@Microservice()
class GreetingService {
  hello(name: string) {
    return `Hello ${name}`
  }
}

const transporter = new WebsocketTransporter()
transporter.connect('ws://127.0.0.1:8787')

new SpiderMesh({ transporters: [transporter] })
new GreetingService()
```

## 3. Client

```ts
import { RemoteServiceLinker, SpiderMesh } from '@spider-mesh/core'
import { WebsocketTransporter } from '@spider-mesh/ws/node'

type GreetingService = {
  hello(name: string): string
}

const transporter = new WebsocketTransporter()
transporter.connect('ws://127.0.0.1:8787')
const mesh = new SpiderMesh({ transporters: [transporter] })

const greeting = RemoteServiceLinker.link<GreetingService>(mesh, {
  service: 'GreetingService',
  timeout: 3_000,
})

await greeting.wait()
console.log(await greeting.hello('Spider Mesh'))
```

Có nhiều provider cùng service thì relay chia request lần lượt cho từng provider (round-robin).
Trong trình duyệt và React Native, chỉ đổi import thành `@spider-mesh/ws/browser` hoặc
`@spider-mesh/ws/react-native`.

## Tuỳ chọn transporter

```ts
import { WebsocketTransporter } from '@spider-mesh/ws/node'

const transporter = new WebsocketTransporter({
  heartbeatIntervalMs: 30_000, // gửi ping giữ kết nối
  reconnectIntervalMs: 1_000,  // chờ bao lâu trước khi kết nối lại
  unsubscribeDelayMs: 10_000,  // giữ subscription event thêm một lúc sau khi không còn ai nghe
})

// Kết nối nhiều relay để dự phòng.
transporter.connect('ws://relay-a.internal:8787')
transporter.connect('ws://relay-b.internal:8787')

// Trạng thái từng relay: 'connecting' | 'connected' | 'error' | 'not_connected'
transporter.status$.subscribe(status => console.log(Object.fromEntries(status)))

// Ngắt một relay, hoặc tất cả.
transporter.close('ws://relay-b.internal:8787')
transporter.close()
```

Kết nối tự động nối lại sau khi rớt. Tên transporter trên wire là `'websocket'`, dùng cho tuỳ chọn
`transporter` khi gọi service.

## Khi mất kết nối

Mọi lời gọi đều kết thúc bằng kết quả hoặc lỗi, kể cả khi không đặt `timeout`:

| Tình huống | Bên gọi nhận |
| --- | --- |
| Không có provider nào cho service | `MICROSERVICE_OFFLINE` ngay |
| Provider rớt khi đang xử lý (kể cả stream đang chạy) | `MICROSERVICE_OFFLINE` ngay |
| Kết nối từ bên gọi tới relay rớt khi request đang chờ | `MICROSERVICE_OFFLINE` ngay |
| Relay đã mất từ trước khi gọi | `MICROSERVICE_OFFLINE` ngay |

Chiều ngược lại: bên gọi rớt thì relay huỷ stream đang chạy ở provider.

Request **không** tự gửi lại, vì có thể provider đã xử lý xong. Muốn thử lại khi gặp
`MICROSERVICE_OFFLINE` thì dùng tuỳ chọn `retry` khi gọi. Mất kết nối tới relay cũng không có nghĩa
là các node khác đã chết: danh sách node chỉ thay đổi khi relay báo node rời đi.

## Liệt kê và chọn node (Topology)

Mặc định relay tự chọn provider. Khi cần biết danh sách node, gọi hàng loạt, hoặc chọn node theo
chiến lược cho từng lời gọi, dùng chính transporter làm discovery của `Topology`:

```ts
import { RemoteServiceLinker, SpiderMesh, Topology } from '@spider-mesh/core'
import { WebsocketTransporter } from '@spider-mesh/ws/node'

type GreetingService = {
  hello(name: string): string
}

const transporter = new WebsocketTransporter()
transporter.connect('ws://127.0.0.1:8787')

const topology = new Topology({ discovery: transporter })
const mesh = new SpiderMesh({ topology, transporters: [transporter] })

const greeting = RemoteServiceLinker.link<GreetingService>(mesh, { service: 'GreetingService' })
await greeting.wait(nodes => nodes.length >= 2)

console.log(greeting.nodes.map(node => node.node_id))
await greeting.set({ routing: { strategy: 'least-active' } }).hello('Spider Mesh')
```

## Event

Cùng một transporter dùng được cho pub/sub của `@spider-mesh/events`:

```ts
import { SpiderMesh } from '@spider-mesh/core'
import { EventBus } from '@spider-mesh/events'
import { WebsocketTransporter } from '@spider-mesh/ws/node'

class OrderPaid {
  constructor(public orderId = '', public amount = 0) {}
}

const transporter = new WebsocketTransporter()
transporter.connect('ws://127.0.0.1:8787')

const mesh = new SpiderMesh({ transporters: [transporter] })
const events = new EventBus({ mesh })
events.registerTransporter(transporter)

const orderPaid = events.link(OrderPaid)
orderPaid.listen().subscribe(event => console.log('paid', event.orderId))
await orderPaid.publish(new OrderPaid('o-1', 100))
```

Relay chỉ chuyển event tới các node đang lắng nghe topic đó.

## Kết nối không tin cậy

Khi relay mở cho cả client bên ngoài (ví dụ trình duyệt), dùng `isServerConnection` để phân biệt
node nội bộ với client. Kết nối **không phải server**:

- không gửi được RPC request, nên không gọi được service;
- không nhận danh sách node và thông báo node rời đi;
- vẫn trả lời RPC (làm provider) và dùng event được.

```ts
import { WebsocketRelayServer } from '@spider-mesh/ws/relay-server'

const relay = new WebsocketRelayServer({
  port: 8787,
  // Node nội bộ kết nối qua path /internal; mọi kết nối khác là client.
  isServerConnection: (_socket, request) => request.url?.startsWith('/internal') ?? false,
})
```
