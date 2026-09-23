# Hướng dẫn dùng Spider Mesh 3.x (dành cho agent)

Tài liệu này dành cho agent tích hợp Spider Mesh vào một dự án. Làm theo thứ tự: chọn mô hình (mục 2),
chép mẫu tương ứng (mục 4), rồi đối chiếu phần **Quy tắc bắt buộc** (mục 9), vì đó là những lỗi hay gặp
nhất. Chi tiết đầy đủ nằm trong README của từng gói (mục 12).

## 1. Spider Mesh là gì

Là RPC và pub/sub giữa các process TypeScript. Bên provider khai báo service bằng một class, bên client
gọi nó qua một proxy có kiểu, như gọi object bình thường. Method có thể trả về giá trị thường, `Promise`
hoặc `Observable` (stream).

| Gói (npm, bản 3.0.0) | Vai trò | Chạy trên |
| --- | --- | --- |
| `@spider-mesh/core` | `SpiderMesh`, `@Microservice`, `RemoteServiceLinker`, `Topology` | Node, Bun, trình duyệt, React Native |
| `@spider-mesh/events` | Pub/sub theo topic (`EventBus`) | như core |
| `@spider-mesh/ws` | Transporter WebSocket + relay server | client: Node, Bun, trình duyệt, React Native; relay: Node, Bun |
| `@spider-mesh/tcp` | HTTP/2 nối thẳng giữa các node | Node, Bun |
| `@simple-discovery/udp` | Các node tự tìm nhau qua UDP, dùng kèm `tcp` | Node, Bun |
| `@spider-mesh/k8s` | Các pod tự tìm nhau qua EndpointSlice của Kubernetes, dùng kèm `tcp` | Node, Bun |

Core không tự làm việc mạng; **transporter** (`ws` hoặc `tcp`) chở request. Mọi gói đều là ESM.

## 2. Chọn mô hình

| Tình huống | Mô hình | Mẫu |
| --- | --- | --- |
| Có trình duyệt/mobile, hoặc node nằm sau NAT, hoặc muốn một điểm kết nối duy nhất | WebSocket relay | 4.A |
| Nhiều server Linux/PM2 trong mạng nội bộ hoặc VPN, muốn gọi thẳng không qua trung gian | HTTP/2 + UDP discovery | 4.B |
| Đã có load balancer / Kubernetes Service cho mỗi service, chỉ cần RPC | HTTP/2 + `resolveService` | 4.C |
| Chạy trên Kubernetes, cần event hoặc chọn node (`consistent-hash`, `least-active`), hoặc muốn pod chết rời mesh ngay | HTTP/2 + Kubernetes discovery | 4.D |

Không chắc thì chọn **4.A**: dễ chạy nhất và dùng được ở mọi môi trường.

## 3. Cài đặt

```bash
# Mô hình A
bun add @spider-mesh/core @spider-mesh/ws rxjs
# Mô hình B
bun add @spider-mesh/core @spider-mesh/tcp @simple-discovery/udp rxjs
# Mô hình C
bun add @spider-mesh/core @spider-mesh/tcp rxjs
# Mô hình D
bun add @spider-mesh/core @spider-mesh/tcp @spider-mesh/k8s rxjs
# Cần pub/sub thì thêm
bun add @spider-mesh/events
```

`tsconfig.json` **bắt buộc** có (decorator kiểu legacy):

```json
{
  "compilerOptions": {
    "experimentalDecorators": true,
    "emitDecoratorMetadata": true
  }
}
```

## 4. Mẫu tích hợp

Mọi mô hình dùng chung một file contract. Client chỉ cần **type**, không import class của provider.

```ts
// contracts.ts — dùng chung giữa các process
import type { Observable } from 'rxjs'

export type OrderServiceContract = {
  get(id: string): Promise<{ id: string; status: string }>
  progress(id: string): Observable<{ id: string; step: number }>
}
```

### 4.A. WebSocket relay

```ts
// relay.ts — một process riêng
import { WebsocketRelayServer } from '@spider-mesh/ws/relay-server'

const relay = new WebsocketRelayServer({ port: 8787 })
console.log('relay listening on', relay.port)
```

```ts
// provider.ts
import { Microservice, SpiderMesh } from '@spider-mesh/core'
import { WebsocketTransporter } from '@spider-mesh/ws/node'
import { interval, map, take } from 'rxjs'

@Microservice()
class OrderService {
  async get(id: string) {
    if (id === 'missing') throw { code: 'ORDER_NOT_FOUND', message: `Order ${id} not found` }
    return { id, status: 'paid' }
  }

  progress(id: string) {
    return interval(500).pipe(take(3), map(step => ({ id, step })))
  }
}

const transporter = new WebsocketTransporter()
transporter.connect(process.env.RELAY_URL ?? 'ws://127.0.0.1:8787')
new SpiderMesh({ transporters: [transporter] })
new OrderService() // bắt buộc: service chỉ được công bố khi đã có instance
```

```ts
// client.ts
import { RemoteServiceLinker, SpiderMesh } from '@spider-mesh/core'
import { WebsocketTransporter } from '@spider-mesh/ws/node'
import type { OrderServiceContract } from './contracts'

const transporter = new WebsocketTransporter()
transporter.connect(process.env.RELAY_URL ?? 'ws://127.0.0.1:8787')
const mesh = new SpiderMesh({ transporters: [transporter] })

const orders = RemoteServiceLinker.link<OrderServiceContract>(mesh, {
  service: 'OrderService', // đúng tên class ở provider
  timeout: 5_000,
})

await orders.wait()
console.log(await orders.get('o-1'))
orders.progress('o-1').subscribe(step => console.log(step))
```

Trình duyệt và React Native: giống hệt, chỉ đổi import thành `@spider-mesh/ws/browser` hoặc
`@spider-mesh/ws/react-native`, và dùng `wss://` (xem mục 9).

### 4.B. HTTP/2 + UDP discovery (Linux/PM2, LAN hoặc VPN)

Mọi process (provider lẫn client) dùng cùng một file cấu hình:

```ts
// mesh.ts
import { SpiderMesh, Topology, type SpiderMeshNode } from '@spider-mesh/core'
import { Http2Rpc, TopologyDiscoveryAdapter } from '@spider-mesh/tcp'
import { UdpDiscovery } from '@simple-discovery/udp'

const udp = new UdpDiscovery<SpiderMeshNode>({
  namespace: process.env.SPIDERMESH_NAMESPACE ?? 'default', // phải trùng namespace của mesh
  tags: ['spider-mesh', 'node'],
  key: process.env.SIMPLE_DISCOVERY_KEY!, // khoá bí mật dùng chung cho cả hệ thống
})

export const topology = new Topology({
  discovery: new TopologyDiscoveryAdapter(udp, {
    onError: error => console.error('discovery broadcast failed', error), // luôn đặt
  }),
  removeUnreachableAfterMs: 5 * 60_000,
})

export const mesh = new SpiderMesh({
  topology,
  transporters: [new Http2Rpc({ port: Number(process.env.RPC_PORT) || undefined })],
})
```

Provider và client viết như 4.A, nhưng import `mesh` từ `mesh.ts` thay vì tự tạo transporter. Chạy:

```bash
SPIDERMESH_NODE_HOSTNAME=10.0.0.5 SIMPLE_DISCOVERY_KEY=... bun run provider.ts
```

- UDP chỉ để các node tìm thấy nhau. Kết nối HTTP/2 mới quyết định node còn sống; **không cần heartbeat**.
- Khác subnet hoặc qua VPN (NetBird, WireGuard, Tailscale) thì tắt multicast và liệt kê máy:
  `SIMPLE_DISCOVERY_UDP_MULTICAST=off SIMPLE_DISCOVERY_UDP_WHITELIST_ADDRESS=host-2,host-3`, và
  `SPIDERMESH_NODE_HOSTNAME` phải là địa chỉ trong VPN.
- Quy tắc riêng của discovery nằm trong
  [`AGENT_GUIDE.md` của simple-discovery](https://github.com/flygo-opensource/simple-discovery/blob/main/packages/udp/AGENT_GUIDE.md).

### 4.C. HTTP/2 sau load balancer (Kubernetes Service)

```ts
import { SpiderMesh } from '@spider-mesh/core'
import { Http2Rpc } from '@spider-mesh/tcp'

const mesh = new SpiderMesh({
  transporters: [new Http2Rpc({
    port: 8080,
    // OrderService -> order-service.default.svc.cluster.local
    resolveService: service => ({
      host: `${service.replace(/(?<!^)([A-Z])/g, '-$1').toLowerCase()}.default.svc.cluster.local`,
      port: 8080,
    }),
  })],
})
```

Không cần `Topology` hay discovery; hạ tầng chọn pod.

Lưu ý: HTTP/2 giữ **một** kết nối dài tới địa chỉ Service, mà kube-proxy chia tải theo kết nối, nên mọi
lời gọi từ một process dồn vào một pod. Cần chia đều thì dùng 4.D, hoặc một proxy chia theo request
(ví dụ waypoint của Istio).

### 4.D. HTTP/2 + Kubernetes discovery

```ts
// mesh.ts
import { SpiderMesh, Topology } from '@spider-mesh/core'
import { Http2Rpc } from '@spider-mesh/tcp'
import { KubernetesDiscovery } from '@spider-mesh/k8s'

export const topology = new Topology({
  discovery: new KubernetesDiscovery({ service: 'spider-mesh' }),
})
export const mesh = new SpiderMesh({ topology, transporters: [new Http2Rpc({ port: 7000 })] })
```

Cần trong cluster (manifest đầy đủ ở
[README của k8s](https://github.com/flygo-opensource/spider-mesh/blob/main/packages/k8s/README.md#manifest)):

- **Một** headless Service (`clusterIP: None`) chọn mọi pod của mesh, cổng tên `discovery` = 7001.
- ServiceAccount có quyền `list`, `watch` trên `endpointslices.discovery.k8s.io` trong namespace đó.
- Mỗi Deployment: label khớp selector của Service, `serviceAccountName`, `readinessProbe` phản ánh app
  sẵn sàng thật, `preStop: sleep 5`.

Không đặt `SPIDERMESH_NODE_HOSTNAME`: host của node là IP pod mà pod khác đã kéo được `/node`.

## 5. Viết service (provider)

- **Tên service = tên class.** Client gọi đúng tên đó.
- Phải `new` class thì service mới được công bố. Mọi `SpiderMesh` trong process đều thấy nó.
- Method trả về giá trị, `Promise` hoặc `Observable`. Tham số và kết quả phải truyền được qua mạng (mục 6.4).
- Lỗi có mã: `throw { code: 'MY_CODE', message: '...' }` hoặc `Error` có thuộc tính `code`. Bên gọi nhận
  lại đúng `code` và `message`.
- `@BeforeMicroserviceOnline()` trên một method async: chạy xong mới công bố service (warm-up).
- `@LimitConcurrentRunning(n)` trên method async: tối đa `n` lời gọi chạy cùng lúc, còn lại xếp hàng.
- `@Microservice({ version: '1.2.0' })`: metadata tuỳ ý, được công bố kèm service.

## 6. Gọi service (client)

```ts
const orders = RemoteServiceLinker.link<OrderServiceContract>(mesh, {
  service: 'OrderService',
  timeout: 5_000,   // ms giữa hai lần nhận dữ liệu; hết giờ → MICROSERVICE_RPC_TIMEOUT
  retry: 2,         // chỉ thử lại khi MICROSERVICE_OFFLINE, cách nhau 1 giây
})

await orders.wait()                                       // chờ service gọi được
const order = await orders.get('o-1')                     // như Promise: await/then/catch/finally
const sub = orders.progress('o-1').subscribe(console.log) // như Observable; mỗi subscribe là một lời gọi
sub.unsubscribe()                                         // huỷ stream ở provider

// Ghi đè tuỳ chọn cho một nhóm lời gọi, không ảnh hưởng `orders`.
const safe = orders.set<null>({ timeout: 500, fallback: null })
await safe.get('o-2') // null nếu lỗi
```

### 6.1. Tuỳ chọn

| Tuỳ chọn | Ý nghĩa |
| --- | --- |
| `service` | Tên service. Bắt buộc. |
| `timeout` | Thời gian im lặng tối đa (ms). Không đặt thì chờ không giới hạn. |
| `retry` | Số lần thử lại khi `MICROSERVICE_OFFLINE`. Lỗi khác không thử lại. |
| `fallback` | Giá trị trả về thay cho mọi lỗi (sau khi hết `retry`). |
| `node_id` | Gọi đúng một node. |
| `routing` | `{ strategy: 'round-robin' \| 'random' \| 'consistent-hash' \| 'least-active', key? }`; cần `Topology`. |
| `transporter` | Ép dùng transporter theo tên (`'websocket'`, `'http2'`). |

### 6.2. Lỗi

Lỗi luôn có dạng `{ code?: string, message: string }` (type `SpiderMeshError`).

| `code` | Khi nào |
| --- | --- |
| `MICROSERVICE_OFFLINE` | Không có node phục vụ, hoặc kết nối mất giữa chừng. Request **không** tự gửi lại. |
| `MICROSERVICE_NOT_FOUND` | Node có nhận nhưng không có service/method đó. |
| `MICROSERVICE_RPC_TIMEOUT` | Hết `timeout`. |
| mã tuỳ ý | Provider ném `{ code, message }` hoặc `Error` có `code`. |

Mọi lời gọi đều kết thúc (kết quả hoặc lỗi), kể cả khi provider hay relay rớt giữa chừng.

### 6.3. `await` và `subscribe` khác nhau thế nào

| Provider trả về | `subscribe` nhận | `await` nhận |
| --- | --- | --- |
| giá trị / `Promise` | giá trị rồi `complete` | giá trị |
| `Observable` | mọi giá trị rồi `complete` | **giá trị đầu tiên**; stream ở provider bị huỷ |
| `Observable` rỗng | chỉ `complete` | ném `EmptyError` |
| lỗi | các giá trị trước lỗi, rồi `error` | lỗi (trừ khi đã có giá trị trước lỗi) |

### 6.4. Dữ liệu truyền được

`string`, `number`, `boolean`, `null`, `undefined`, mảng, object lồng nhau, `Date`, `Uint8Array` giữ
nguyên. `Map` thành object thường. `Set` và instance class mất kiểu (thành object thường, không còn method).

## 7. Pub/sub (events)

```ts
import { EventBus } from '@spider-mesh/events'

class OrderPaid {
  constructor(public orderId = '', public amount = 0) {} // tên class là tên topic
}

const events = new EventBus({ mesh })
events.registerTransporter(transporter)            // mô hình A: chính WebsocketTransporter
// mô hình B: events.registerTransporter(new Http2Pubsub(topology))  (import từ '@spider-mesh/tcp')

const orderPaid = events.link(OrderPaid)
orderPaid.listen().subscribe(event => console.log(event.orderId))
await orderPaid.publish(new OrderPaid('o-1', 100))
```

- Bên nhận được dữ liệu thuần (field), không phải instance.
- `publish()` ném lỗi nếu chưa đăng ký transporter nào.
- Topic theo tên, không cần class: `events.linkTopic<{ text: string }>('chat.message')`.
- Event không được lưu lại: node chưa `listen()` lúc publish thì không nhận.

## 8. Topology (tuỳ chọn)

Chỉ cần khi muốn liệt kê node, gọi hàng loạt, hoặc chọn node theo chiến lược. Mô hình B đã có sẵn;
với mô hình A thì dùng chính transporter làm discovery:

```ts
import { Topology } from '@spider-mesh/core'

const topology = new Topology({ discovery: transporter })
const mesh = new SpiderMesh({ topology, transporters: [transporter] })
const orders = RemoteServiceLinker.link<OrderServiceContract>(mesh, { service: 'OrderService' })

await orders.wait(nodes => nodes.length >= 2)        // chờ đủ 2 node
orders.nodes                                          // danh sách node hiện tại
orders.watch().subscribe(nodes => console.log(nodes.length))
orders.__batch__get('o-1').subscribe(r => console.log(r.node.node_id, r)) // gọi mọi node
await orders.set({ routing: { strategy: 'consistent-hash', key: 'customer-42' } }).get('o-1')
```

Không có `Topology` thì `wait()` bỏ qua tham số, `nodes` rỗng và `__batch__` không có dữ liệu.

## 9. Quy tắc bắt buộc

1. **Mọi node và relay phải cùng dòng 3.x.** Bản 3.0 đổi cách mã hoá dữ liệu trên đường truyền, nên
   không nói chuyện được với node hay relay 2.x.
2. **Client chỉ `import type` contract**, không import class provider. Import class sẽ chạy `@Microservice`
   và đăng ký service ngoài ý muốn trong process client.
3. **Phải `new` class service** ở provider; chỉ khai báo class thì chưa công bố gì.
4. **Không đổi tên hay minify tên class service.** Tên class là tên service trên mạng; bundler minify
   tên class sẽ làm client không tìm thấy service (`MICROSERVICE_NOT_FOUND` / `MICROSERVICE_OFFLINE`).
5. **Mỗi process một `SpiderMesh`, một `EventBus`.** Service đăng ký theo process, không theo mesh.
6. **Luôn đặt `timeout` cho lời gọi từ trình duyệt/mobile.** App chạy nền hoặc máy khoá màn hình có thể
   bị cắt mạng mà không đóng kết nối ngay.
7. **Dùng `wss://` ở production** cho trình duyệt và React Native. Relay không tự làm TLS: đặt sau
   reverse proxy (nginx, Caddy, load balancer). Android bản release chặn `ws://` mà không báo lỗi rõ.
8. **Mô hình B:**
   - `SPIDERMESH_NODE_HOSTNAME` phải là địa chỉ máy khác kết nối tới được. Thiếu nó thì node thấy nhau
     nhưng gọi bị `MICROSERVICE_OFFLINE`.
   - `namespace` của `UdpDiscovery` phải trùng `SPIDERMESH_NAMESPACE`, và luôn đặt `onError` cho adapter;
     không thì lỗi này im lặng.
   - Mọi node dùng cùng `SIMPLE_DISCOVERY_KEY` và cùng cổng discovery; đồng hồ các máy đồng bộ (NTP).
   - Mở UDP `11001` và cổng TCP của `Http2Rpc` giữa các máy.
9. **`retry` chỉ áp dụng cho `MICROSERVICE_OFFLINE`**, và request không bao giờ tự gửi lại. Method không
   idempotent thì cân nhắc trước khi đặt `retry`.
10. **Dùng Bun ≥ 1.4.2** nếu cài gói bằng đường dẫn `file:`; bản cũ hơn cài sai.
11. **Mô hình D:**
    - Log có `[spider-mesh/k8s] Cannot watch EndpointSlices … Falling back to DNS` nghĩa là thiếu RBAC.
      Mesh vẫn chạy nhưng pod vào/ra được nhận ra chậm vài giây; thêm Role/RoleBinding rồi khởi động lại pod.
    - Cổng 7001 (`/node`) và cổng của `Http2Rpc` phải mở giữa các pod nếu có NetworkPolicy.
    - `@spider-mesh/k8s` 2.x là thiết kế cũ (`K8sRpcTransporter`), không trộn với 3.x.

## 10. Biến môi trường

| Biến | Mặc định | Ý nghĩa |
| --- | --- | --- |
| `SPIDERMESH_NAMESPACE` | `default` | Namespace của node. |
| `SPIDERMESH_NODE_HOSTNAME` | *(rỗng)* | Địa chỉ để node khác kết nối tới. Bắt buộc với mô hình B. |
| `SPIDERMESH_HTTP2_CONNECT_TIMEOUT_MS` | `2000` | Thời gian chờ mở kết nối HTTP/2. |
| `SPIDERMESH_HTTP2_RECONNECT_ATTEMPTS` | `3` | Số lần lỗi liên tiếp trước khi coi node là không kết nối được (vẫn tiếp tục thử). |
| `SPIDERMESH_HTTP2_RECONNECT_DELAY_MS` | `250` | Độ trễ nối lại ban đầu. |
| `SPIDERMESH_HTTP2_RECONNECT_MAX_DELAY_MS` | `30000` | Độ trễ nối lại tối đa. |
| `SPIDERMESH_K8S_DISCOVERY_PORT` | `7001` | Mô hình D: cổng `GET /node` của mỗi pod. |
| `SPIDERMESH_K8S_DNS_INTERVAL_MS` | `5000` | Mô hình D: chu kỳ phân giải DNS khi rơi về DNS. |
| `SPIDERMESH_K8S_NODE_IDLE_TIMEOUT_MS` | `45000` | Mô hình D: stream `/node` im lặng quá mức này thì nối lại. |
| `APP_VERSION`, `GIT_TAG`, `GIT_BRANCH`, `GIT_COMMIT`, `BUILD_TIME`, `APP_ENV`, `APP_TAGS` | – | Thông tin build công bố kèm node. |

Biến `SIMPLE_DISCOVERY_*` của discovery: xem guide của `@simple-discovery/udp`.

## 11. Tự kiểm tra sau khi tích hợp

1. Chạy provider và client (mô hình A: chạy relay trước). `await link.wait()` phải trả về trong vài giây
   và một lời gọi phải ra đúng kết quả.
2. Gọi một method ném `{ code }`: client phải nhận đúng `code` và `message`.
3. Subscribe một method trả `Observable` rồi `unsubscribe()` giữa chừng: stream ở provider phải dừng.
4. Tắt provider khi đang gọi: client phải nhận `MICROSERVICE_OFFLINE`, không treo.
5. Mô hình B, hai máy: nếu thấy nhau nhưng gọi lỗi thì kiểm tra `SPIDERMESH_NODE_HOSTNAME` và firewall
   TCP. Nếu không thấy nhau thì kiểm tra theo thứ tự: cùng `SIMPLE_DISCOVERY_KEY` → cùng cổng → cùng
   namespace → UDP mở → đồng hồ → mạng chặn multicast (dùng whitelist) và bật
   `SIMPLE_DISCOVERY_UDP_DEBUG=1`.
6. Mô hình D: xoá một pod provider (`kubectl delete pod`) trong lúc client đang gọi; client phải ngừng
   gọi tới pod đó trong dưới một giây và không có lời gọi nào lỗi khi `rollout restart`. Log không được
   có cảnh báo `Falling back to DNS`.

| Triệu chứng | Nguyên nhân thường gặp |
| --- | --- |
| `wait()` không bao giờ trả về | Sai tên `service`, provider chưa `new` class, hoặc khác relay/namespace. |
| `MICROSERVICE_NOT_FOUND` | Sai tên method, hoặc provider đang chạy bản code cũ. |
| Kết nối được nhưng dữ liệu hỏng / không hiểu nhau | Trộn node 2.x với 3.x. |
| `publish()` ném lỗi | Chưa `registerTransporter()` cho `EventBus`. |
| Decorator báo lỗi khi build | Thiếu `experimentalDecorators` / `emitDecoratorMetadata`. |

## 12. Tham khảo

- Repo: https://github.com/flygo-opensource/spider-mesh
- README từng gói (đầy đủ nhất):
  [core](https://github.com/flygo-opensource/spider-mesh/blob/main/packages/core/README.md),
  [events](https://github.com/flygo-opensource/spider-mesh/blob/main/packages/events/README.md),
  [ws](https://github.com/flygo-opensource/spider-mesh/blob/main/packages/ws/README.md),
  [tcp](https://github.com/flygo-opensource/spider-mesh/blob/main/packages/tcp/README.md),
  [k8s](https://github.com/flygo-opensource/spider-mesh/blob/main/packages/k8s/README.md)
- npm: `@spider-mesh/core`, `@spider-mesh/events`, `@spider-mesh/ws`, `@spider-mesh/tcp`, `@spider-mesh/k8s`
- Discovery: https://github.com/flygo-opensource/simple-discovery
