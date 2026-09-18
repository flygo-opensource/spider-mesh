# @spider-mesh/core

Runtime RPC cho TypeScript: khai báo service bằng class, gọi service ở process khác qua một typed
proxy, nhận kết quả dạng `Promise` hoặc stream `Observable`. Core không tự làm việc mạng; việc truyền
packet do **transporter** đảm nhận:

| Gói | Vai trò |
| --- | --- |
| `@spider-mesh/ws` | WebSocket qua một relay trung tâm (Node/Bun, trình duyệt, React Native) |
| `@spider-mesh/tcp` | HTTP/2 kết nối thẳng giữa các node |
| `@spider-mesh/events` | Pub/sub theo topic, dùng chung transporter với RPC |

## Cài đặt

```bash
bun add @spider-mesh/core rxjs
```

Service được khai báo bằng decorator kiểu legacy, nên `tsconfig.json` cần:

```json
{
  "compilerOptions": {
    "experimentalDecorators": true,
    "emitDecoratorMetadata": true
  }
}
```

## Ví dụ nhanh

Provider và client là hai process khác nhau, cùng dùng một transporter (ở đây là WebSocket relay;
xem `@spider-mesh/ws` để chạy relay).

```ts
// provider.ts
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

```ts
// client.ts
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
console.log(await greeting.hello('Spider Mesh')) // Hello Spider Mesh
```

Client chỉ cần một **type** mô tả service, không cần import class của provider.

## Viết service

- Tên service là **tên class**. Client gọi đúng tên đó.
- Service chỉ được công bố khi đã có một instance (`new GreetingService()`). Mọi `SpiderMesh` trong
  process đều thấy service này.
- Method có thể trả về giá trị thường, `Promise`, hoặc `Observable` (stream nhiều giá trị).

```ts
import { BeforeMicroserviceOnline, LimitConcurrentRunning, Microservice } from '@spider-mesh/core'
import { interval, map, take } from 'rxjs'

@Microservice({ version: '1.2.0' }) // metadata tuỳ ý, được công bố kèm service
class OrderService {
  #ready = false

  // Chạy xong trước khi service được công bố; client chưa gọi được cho tới lúc đó.
  @BeforeMicroserviceOnline()
  async warmUp() {
    this.#ready = true
  }

  async get(id: string) {
    if (!this.#ready) throw new Error('not ready')
    return { id, status: 'paid' }
  }

  // Tối đa 2 lời gọi chạy đồng thời; các lời gọi sau xếp hàng. Chỉ áp dụng cho method async.
  @LimitConcurrentRunning(2)
  async export(from: string, to: string) {
    return `export ${from}..${to}`
  }

  // Stream: client nhận 5 giá trị rồi complete.
  progress(orderId: string) {
    return interval(1_000).pipe(take(5), map(step => ({ orderId, step })))
  }

  async refund(id: string) {
    // `code` là chuỗi tuỳ ý; client nhận lại đúng `code` và `message`.
    throw { code: 'REFUND_WINDOW_CLOSED', message: `Order ${id} can no longer be refunded` }
  }
}

new OrderService()
```

## Gọi service

`RemoteServiceLinker.link()` trả về một proxy có đủ method của service:

- Method của provider trả giá trị hoặc `Promise` thì bên gọi dùng được với `await`.
- Method trả `Observable` thì bên gọi nhận `Observable`. Unsubscribe sẽ huỷ stream ở provider.

```ts
import { RemoteServiceLinker, SpiderMesh } from '@spider-mesh/core'
import type { Observable } from 'rxjs'
import { WebsocketTransporter } from '@spider-mesh/ws/node'

type OrderService = {
  get(id: string): Promise<{ id: string; status: string }>
  progress(orderId: string): Observable<{ orderId: string; step: number }>
  refund(id: string): Promise<void>
}

const transporter = new WebsocketTransporter()
transporter.connect('ws://127.0.0.1:8787')
const mesh = new SpiderMesh({ transporters: [transporter] })

const orders = RemoteServiceLinker.link<OrderService>(mesh, {
  service: 'OrderService',
  timeout: 5_000,
  retry: 2,
})

await orders.wait()

const order = await orders.get('o-1')

const subscription = orders.progress('o-1').subscribe({
  next: ({ step }) => console.log('step', step),
  error: error => console.error(error.code, error.message),
})
subscription.unsubscribe() // huỷ stream ở provider

try {
  await orders.refund('o-1')
} catch (error: any) {
  console.log(error.code) // 'REFUND_WINDOW_CLOSED'
}

// Ghi đè tuỳ chọn cho một nhóm lời gọi, không ảnh hưởng `orders`.
const withFallback = orders.set<null>({ timeout: 500, fallback: null })
const maybeOrder = await withFallback.get('o-2') // null nếu lỗi
```

### Tuỳ chọn

| Tuỳ chọn | Ý nghĩa |
| --- | --- |
| `service` | Tên service (tên class ở provider). Bắt buộc. |
| `timeout` | Thời gian tối đa (ms) **giữa hai lần nhận dữ liệu**. Với lời gọi `await` là thời gian chờ kết quả; với stream là thời gian im lặng tối đa giữa hai giá trị. Hết giờ → `MICROSERVICE_RPC_TIMEOUT`. Không đặt thì không giới hạn. |
| `retry` | Số lần thử lại, **chỉ khi lỗi là `MICROSERVICE_OFFLINE`**, cách nhau 1 giây. Lỗi khác không được thử lại. |
| `fallback` | Giá trị trả về thay cho mọi lỗi (sau khi đã hết lượt `retry`). |
| `node_id` | Gọi đúng một node cụ thể. |
| `routing` | Chiến lược chọn node cho từng lời gọi (cần `Topology`, xem bên dưới). |
| `transporter` | Ép dùng một transporter theo tên (`'websocket'`, `'http2'`, …). |

### Lỗi

Lỗi luôn có dạng `{ code?: string, message: string }`.

| `code` | Khi nào |
| --- | --- |
| `MICROSERVICE_OFFLINE` | Không có node nào phục vụ service, hoặc kết nối tới node/relay mất giữa chừng. Request **không** tự gửi lại; dùng `retry` nếu muốn. |
| `MICROSERVICE_NOT_FOUND` | Node nhận request nhưng không có service/method đó. |
| `MICROSERVICE_RPC_TIMEOUT` | Hết `timeout`. |
| mã tuỳ ý | Provider ném `{ code, message }` hoặc `Error` (khi đó chỉ có `message`). |

### Chờ và theo dõi service

```ts
import { RemoteServiceLinker, SpiderMesh } from '@spider-mesh/core'
import { WebsocketTransporter } from '@spider-mesh/ws/node'

const transporter = new WebsocketTransporter()
transporter.connect('ws://127.0.0.1:8787')
const mesh = new SpiderMesh({ transporters: [transporter] })
const orders = RemoteServiceLinker.link(mesh, { service: 'OrderService' })

// Đợi tới khi gọi được service.
await orders.wait()
```

Không có `Topology`, `wait()` hỏi transporter mỗi 500 ms cho tới khi service gọi được, rồi trả về
mảng rỗng; tham số `checker` bị bỏ qua. Danh sách node (`nodes`, `watch()`) và gọi hàng loạt chỉ có
dữ liệu khi dùng `Topology`.

## Topology: biết và chọn từng node

Mặc định core không giữ danh sách node: transporter hoặc hạ tầng tự chọn đích (relay WebSocket,
Kubernetes Service, …). Thêm `Topology` khi ứng dụng cần:

- liệt kê/theo dõi các node đang phục vụ một service,
- gọi hàng loạt tới mọi node,
- chọn node theo chiến lược cho từng lời gọi,
- transporter kết nối thẳng tới từng node (như `@spider-mesh/tcp`).

`Topology` lấy danh sách node từ một **discovery**. Ví dụ dưới dùng chính transporter WebSocket làm
discovery; `@spider-mesh/tcp` hướng dẫn cách dùng UDP.

```ts
import { RemoteServiceLinker, SpiderMesh, Topology } from '@spider-mesh/core'
import { WebsocketTransporter } from '@spider-mesh/ws/node'

type OrderService = {
  get(id: string): Promise<{ id: string; status: string }>
}

const transporter = new WebsocketTransporter()
transporter.connect('ws://127.0.0.1:8787')

const topology = new Topology({ discovery: transporter })
const mesh = new SpiderMesh({ topology, transporters: [transporter] })
const orders = RemoteServiceLinker.link<OrderService>(mesh, { service: 'OrderService' })

// Đợi có ít nhất 2 node.
await orders.wait(nodes => nodes.length >= 2)

console.log(orders.nodes.map(node => node.node_id))
orders.watch().subscribe(nodes => console.log('nodes:', nodes.length))

// Cùng một khách hàng luôn vào cùng một node.
await orders.set({ routing: { strategy: 'consistent-hash', key: 'customer-42' } }).get('o-1')

// Gọi mọi node, mỗi node trả về { node, data } hoặc { node, error }.
orders.__batch__get('o-1').subscribe(result => console.log(result.node.node_id, result))

// Sự kiện tức thời: node vào/ra, endpoint nghi lỗi/mất/hồi phục.
topology.events$.subscribe(event => console.log(event.type))
```

| Tuỳ chọn `Topology` | Ý nghĩa |
| --- | --- |
| `discovery` | Nguồn danh sách node. |
| `removeUnreachableAfterMs` | Xoá node khi mọi transporter báo không kết nối được tới nó liên tục trong khoảng này. Dùng khi discovery chỉ để các node tìm thấy nhau và kết nối của transporter mới cho biết node còn sống (như `@spider-mesh/tcp` với UDP). |
| `staleAfterMs` | Xoá node không nhận được thông báo nào từ discovery trong khoảng này. Chỉ dùng khi discovery tự phát lại định kỳ (heartbeat). |

| `routing.strategy` | Cách chọn node |
| --- | --- |
| `round-robin` | Lần lượt (mặc định). |
| `random` | Ngẫu nhiên. |
| `consistent-hash` | Cùng `key` luôn vào cùng node. |
| `least-active` | Node đang xử lý ít request nhất. |

Mỗi chiến lược (trừ `random`) nhận thêm `state_key` để tách thành các nhóm trạng thái độc lập.

### Discovery tự viết

Discovery là bất kỳ object nào có `bind()`: nhận node local để công bố ra ngoài, và đẩy node nhận
được vào Topology.

```ts
import { Topology, type SpiderMeshNode, type TopologyDiscovery } from '@spider-mesh/core'

declare function announce(node: SpiderMeshNode): void
declare function onRemote(callback: (node: SpiderMeshNode) => void): void
declare function onRemoteGone(callback: (nodeId: string) => void): void

const discovery: TopologyDiscovery = {
  bind(context) {
    const subscription = context.localNode$.subscribe(node => announce(node))
    onRemote(node => context.upsertRemote(node))
    onRemoteGone(nodeId => context.removeRemote(nodeId))
    return subscription
  },
  // Tuỳ chọn: xác nhận node còn sống khi transporter báo mất kết nối.
  async verify(nodeId) {
    return 'unknown' // 'alive' | 'dead' | 'unknown'
  },
}

const topology = new Topology({ discovery, staleAfterMs: 15_000 })
```

Topology xoá node khi discovery báo rời đi, `verify()` trả `'dead'`, quá `staleAfterMs`, hoặc mọi
transporter không kết nối được liên tục quá `removeUnreachableAfterMs`. Transporter vừa mất kết nối
chỉ khiến node không được chọn để gọi; tự nó không xoá node.

## Viết transporter

Transporter là một `Observable<RpcEvent>` có tên ổn định và hàm `send()`:

```ts
import { SpiderMesh, type RpcCancelPacket, type RpcEvent, type RpcProbeRequest, type RpcRequestPacket, type RpcResponsePacket, type RpcTransporter } from '@spider-mesh/core'
import { Subject } from 'rxjs'

class LoopbackTransporter extends Subject<RpcEvent> implements RpcTransporter {
  // Tên trên wire; mọi node dùng cùng tên cho cùng một giao thức.
  public readonly name = 'loopback'

  async send(packet: RpcRequestPacket | RpcResponsePacket | RpcCancelPacket) {
    // Gửi packet tới node đích; packet nhận được thì phát ra bằng this.next({ rpc: packet }).
    queueMicrotask(() => this.next({ rpc: packet }))
    return { cancel: () => {} }
  }

  // Đang có đường tới service/node này không (không được gây side effect).
  canRoute(service: string, nodeId?: string) {
    return true
  }

  // Dùng cho wait() khi không có Topology.
  async probe({ service }: RpcProbeRequest) {
    return { reachable: true }
  }
}

const mesh = new SpiderMesh({ transporters: [new LoopbackTransporter()] })
```

Transporter báo node rời đi bằng `this.next({ offline: nodeId })`; core sẽ kết thúc mọi lời gọi
đang chờ node đó bằng `MICROSERVICE_OFFLINE`.

## NestJS

```ts
import { Inject, Injectable, Module } from '@nestjs/common'
import { NestJSExposeMicroservice, NestJSLinkMicroservice, SpiderMesh } from '@spider-mesh/core'
import { WebsocketTransporter } from '@spider-mesh/ws/node'

// Service chạy ở process khác. Class chỉ dùng làm token và mô tả kiểu; tên class là tên service.
abstract class PaymentService {
  abstract charge(orderId: string, amount: number): Promise<{ ok: boolean }>
}

@Injectable()
class OrderService {
  constructor(@Inject(PaymentService) private readonly payments: PaymentService) {}

  async checkout(orderId: string) {
    return this.payments.charge(orderId, 100)
  }
}

const transporter = new WebsocketTransporter()
transporter.connect('ws://127.0.0.1:8787')

@Module({
  providers: [
    { provide: SpiderMesh, useValue: new SpiderMesh({ transporters: [transporter] }) },
    OrderService,
    // Công bố provider NestJS `OrderService` thành service trong mesh.
    NestJSExposeMicroservice(OrderService),
    // Inject `PaymentService` là proxy tới service ở process khác.
    NestJSLinkMicroservice(PaymentService),
  ],
})
export class AppModule {}
```

## Biến môi trường

| Biến | Mặc định | Ý nghĩa |
| --- | --- | --- |
| `SPIDERMESH_NAMESPACE` | `default` | Namespace của node. |
| `SPIDERMESH_NODE_HOSTNAME` | *(rỗng)* | Địa chỉ node khác dùng để kết nối tới node này. **Bắt buộc** với transporter kết nối thẳng như `@spider-mesh/tcp`. |
| `APP_VERSION`, `GIT_TAG`, `GIT_BRANCH`, `GIT_COMMIT`, `BUILD_TIME`, `APP_ENV`, `APP_TAGS` | – | Thông tin build được công bố kèm node (`node.build`). |

Mỗi `SpiderMesh` luôn nhận một `node_id` ngẫu nhiên mới.
