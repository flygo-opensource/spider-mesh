# @spider-mesh/tcp

Transporter HTTP/2 cho Spider Mesh: các node kết nối thẳng tới nhau, không qua broker hay relay. Kèm
`Http2Pubsub` để gửi event trực tiếp giữa các node.

Có hai cách dùng:

| Cách | Khi nào | Cần gì |
| --- | --- | --- |
| **Linux/PM2 với UDP discovery** | Các server tự tìm nhau trong mạng LAN | `Topology` + `@ohayo/udp` |
| **Hạ tầng tự chọn đích** | Có sẵn load balancer/DNS (ví dụ Kubernetes Service) | Chỉ `resolveService` |

Cách khai báo và gọi service được mô tả trong `@spider-mesh/core`.

## Linux/PM2 với UDP discovery

```bash
bun add @spider-mesh/core @spider-mesh/tcp @ohayo/udp rxjs
```

Mọi process (provider lẫn client) dùng cùng một cấu hình:

```ts
// mesh.ts
import { SpiderMesh, Topology, type SpiderMeshNode } from '@spider-mesh/core'
import { Http2Rpc, TopologyDiscoveryAdapter } from '@spider-mesh/tcp'
import { UdpDiscovery } from '@ohayo/udp'

// Phải trùng namespace của mesh (đọc từ cùng biến SPIDERMESH_NAMESPACE, mặc định 'default').
const namespace = process.env.SPIDERMESH_NAMESPACE ?? 'default'

const udp = new UdpDiscovery<SpiderMeshNode>({
  namespace,
  tags: ['spider-mesh', 'node'],
  // Khoá ký gói discovery; mọi node trong mesh dùng cùng một khoá bí mật.
  key: process.env.DISCOVERY_KEY!,
})

const topology = new Topology({
  discovery: new TopologyDiscoveryAdapter(udp, {
    // UDP không báo khi node rời đi: phát lại định kỳ, node im lặng quá staleAfterMs thì bị xoá.
    heartbeatIntervalMs: 5_000,
    onError: error => console.error('discovery broadcast failed', error),
  }),
  staleAfterMs: 15_000,
})

export const mesh = new SpiderMesh({
  topology,
  // Cổng được công bố qua discovery, nên để trống (cổng ngẫu nhiên) là đủ. Cần cổng cố định cho
  // firewall thì đặt RPC_PORT riêng cho từng process: nhiều process trên một máy không dùng chung cổng.
  transporters: [new Http2Rpc({ port: Number(process.env.RPC_PORT) || undefined })],
})
export { topology }
```

`TopologyDiscoveryAdapter` nối discovery vào `Topology`: broadcast node local mỗi khi đổi, và đưa node
nhận được vào Topology. Nó nhận mọi discovery có `broadcast()` và phát ra message, không riêng
`@ohayo/udp`.

| Tuỳ chọn adapter | Mặc định | Ý nghĩa |
| --- | --- | --- |
| `heartbeatIntervalMs` | tắt | Broadcast lại định kỳ để node còn sống không bị `staleAfterMs` xoá. Khoảng 1/3 `staleAfterMs`. |
| `onError` | bỏ qua | Nhận lỗi broadcast. **Nên luôn đặt**, xem bảng lỗi bên dưới. |
| `tags` | `['spider-mesh', 'node']` | Tag gắn vào message; tag của `UdpDiscovery` phải nằm trong danh sách này. |
| `closeTransporter` | `true` | Đóng discovery khi `Topology` đóng. |

Chạy mỗi process với địa chỉ LAN của **chính máy đó**, để các node khác kết nối tới được:

```bash
SPIDERMESH_NODE_HOSTNAME=192.168.1.21 SPIDERMESH_NAMESPACE=shop DISCOVERY_KEY=... bun run provider.ts
```

Provider và client chỉ cần import `mesh.ts`:

```ts
// provider.ts
import { Microservice } from '@spider-mesh/core'
import './mesh.js'

@Microservice()
class GreetingService {
  hello(name: string) {
    return `Hello ${name}`
  }
}

new GreetingService()
```

```ts
// client.ts
import { RemoteServiceLinker } from '@spider-mesh/core'
import { mesh } from './mesh.js'

type GreetingService = {
  hello(name: string): string
}

const greeting = RemoteServiceLinker.link<GreetingService>(mesh, { service: 'GreetingService' })
await greeting.wait()
console.log(await greeting.hello('Spider Mesh'))
```

### Những lỗi hay gặp

| Triệu chứng | Nguyên nhân |
| --- | --- |
| Node không bao giờ thấy nhau, `onError` báo `namespace must be …` | Namespace của `UdpDiscovery` khác `SPIDERMESH_NAMESPACE`. Không có `onError` thì lỗi này **im lặng**. |
| Node thấy nhau nhưng gọi bị `MICROSERVICE_OFFLINE` | Chưa đặt `SPIDERMESH_NODE_HOSTNAME`, hoặc đặt địa chỉ mà máy khác không tới được. |
| Máy khác subnet / mạng chặn multicast không thấy nhau | Khai báo `peers` (xem bên dưới). |
| Thấy node lạ | Các mesh dùng chung `key` và `namespace`. Đặt `key` riêng. |

### Mạng cần mở

| Giao thức | Cổng | Để làm gì |
| --- | --- | --- |
| UDP | `11001` (tuỳ chọn `port` của `UdpDiscovery`) | Discovery |
| UDP multicast | `239.0.1.1` (tuỳ chọn `multicastAddress`) | Discovery trong cùng subnet |
| TCP | `port` của `Http2Rpc` (mặc định ngẫu nhiên) | RPC |
| TCP | cổng ngẫu nhiên | Event (`Http2Pubsub`) |

### Khi multicast không dùng được

Liệt kê địa chỉ các máy trong `peers`. Có thể ghi IP đầy đủ, hoặc 3 octet đầu để quét cả dải `/24`:

```ts
import { type SpiderMeshNode } from '@spider-mesh/core'
import { UdpDiscovery } from '@ohayo/udp'

const udp = new UdpDiscovery<SpiderMeshNode>({
  namespace: process.env.SPIDERMESH_NAMESPACE ?? 'default',
  tags: ['spider-mesh', 'node'],
  key: process.env.DISCOVERY_KEY!,
  peers: ['192.168.1.21', '192.168.1.22', '10.0.5'],
})
```

Nhiều process trên cùng một máy chia nhau cùng cổng discovery; gói từ máy khác được chuyển tiếp cho
mọi process trên máy.

### Chọn node cho từng lời gọi

Không chỉ định gì thì lời gọi được chia lần lượt cho các node (round-robin). Xem các chiến lược khác
(`consistent-hash`, `least-active`, …) và cách gọi hàng loạt trong `@spider-mesh/core`.

## Hạ tầng tự chọn đích

Không cần `Topology` khi mỗi service đã có một địa chỉ ổn định phía sau load balancer:

```ts
import { SpiderMesh } from '@spider-mesh/core'
import { Http2Rpc } from '@spider-mesh/tcp'

const mesh = new SpiderMesh({
  transporters: [new Http2Rpc({
    port: 8080,
    // Tên service -> địa chỉ; ví dụ DNS của Kubernetes Service.
    resolveService: service => ({
      host: `${service.toLowerCase()}.default.svc.cluster.local`,
      port: 8080,
    }),
  })],
})
```

`wait()` thử kết nối tới địa chỉ đó; hạ tầng quyết định request vào pod nào.

## Event

```ts
// events.ts
import { EventBus } from '@spider-mesh/events'
import { Http2Pubsub } from '@spider-mesh/tcp'
import { mesh, topology } from './mesh.js'

class OrderPaid {
  constructor(public orderId = '', public amount = 0) {}
}

const events = new EventBus({ mesh })
events.registerTransporter(new Http2Pubsub(topology))

const orderPaid = events.link(OrderPaid)
orderPaid.listen().subscribe(event => console.log('paid', event.orderId))
await orderPaid.publish(new OrderPaid('o-1', 100))
```

Event được gửi thẳng tới những node đang lắng nghe topic, không qua broker.

## Khi mất kết nối

- Node mất kết nối bị loại khỏi danh sách chọn đích ngay; lời gọi đang chạy tới node đó kết thúc
  bằng `MICROSERVICE_OFFLINE`.
- `Http2Rpc` thử nối lại `SPIDERMESH_HTTP2_RECONNECT_ATTEMPTS` lần (mặc định 3), với độ trễ tăng dần.
- **Hết lượt thì dừng hẳn với node đó.** Node chỉ được thử lại khi nó xuất hiện với `node_id` mới
  (process khởi động lại) hoặc địa chỉ mới. Heartbeat của cùng node **không** kích hoạt nối lại, nên
  một lần mất mạng dài hơn tổng thời gian thử lại sẽ làm node đó không gọi được cho tới khi một trong
  hai process khởi động lại. Tăng `SPIDERMESH_HTTP2_RECONNECT_ATTEMPTS` nếu mạng hay chập chờn.
- Node chỉ bị xoá khỏi danh sách khi quá `staleAfterMs` mà không có tin tức.

## Tuỳ chọn và biến môi trường

| `Http2Rpc` | Ý nghĩa |
| --- | --- |
| `port` | Cổng nhận RPC. Mặc định cổng ngẫu nhiên; mỗi process trên cùng máy cần một cổng khác nhau. |
| `resolveService` | Hàm trả `{ host, port }` cho một service; dùng khi hạ tầng tự chọn đích. |

| Biến | Mặc định | Ý nghĩa |
| --- | --- | --- |
| `SPIDERMESH_NODE_HOSTNAME` | *(rỗng)* | Địa chỉ node khác dùng để kết nối tới node này. **Bắt buộc** với UDP discovery. |
| `SPIDERMESH_NAMESPACE` | `default` | Namespace của mesh. |
| `SPIDERMESH_HTTP2_CONNECT_TIMEOUT_MS` | `2000` | Thời gian chờ mở kết nối. |
| `SPIDERMESH_HTTP2_RECONNECT_ATTEMPTS` | `3` | Số lần thử nối lại trước khi dừng. |
| `SPIDERMESH_HTTP2_RECONNECT_DELAY_MS` | `250` | Độ trễ giữa các lần nối lại (tăng dần). |

Tên transporter trên wire: `'http2'` (RPC) và `'http2-pubsub'` (event).
