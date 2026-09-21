# Spider Mesh

RPC và pub/sub giữa các service TypeScript. Khai báo service bằng một class, gọi nó từ process khác
như gọi một object bình thường; kết quả có thể là giá trị, `Promise` hoặc `Observable`. Chạy trên
Node/Bun, trình duyệt và React Native.

| Gói | Thư mục | Vai trò |
| --- | --- | --- |
| [`@spider-mesh/core`](core/README.md) | [`core/`](core) | `SpiderMesh`, `@Microservice`, `RemoteServiceLinker`, `Topology` |
| [`@spider-mesh/events`](events/README.md) | [`events/`](events) | Pub/sub theo topic (`EventBus`) |
| [`@spider-mesh/ws`](ws/README.md) | [`ws/`](ws) | Transporter WebSocket và relay server; dùng được trong trình duyệt và React Native |
| [`@spider-mesh/tcp`](tcp/README.md) | [`tcp/`](tcp) | HTTP/2 giữa các node, discovery qua UDP ([`@simple-discovery/udp`](https://github.com/flygo-opensource/simple-discovery)) |

## Chọn transporter

| Môi trường | Dùng |
| --- | --- |
| Trình duyệt, mobile, sau NAT, muốn một điểm kết nối | `@spider-mesh/ws` + relay |
| Server Linux/PM2, mạng nội bộ hoặc VPN | `@spider-mesh/tcp` + `@simple-discovery/udp` |
| Kubernetes Service, load balancer có sẵn | `@spider-mesh/tcp` với `resolveService` |

## Ví dụ nhanh

```bash
bun add @spider-mesh/core @spider-mesh/ws rxjs
```

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

const transporter = new WebsocketTransporter()
transporter.connect('ws://127.0.0.1:8787')
const mesh = new SpiderMesh({ transporters: [transporter] })

const greeting = RemoteServiceLinker.link<{ hello(name: string): string }>(mesh, {
  service: 'GreetingService',
})
await greeting.wait()
console.log(await greeting.hello('Spider Mesh'))
```

Relay: xem [`ws/README.md`](ws/README.md).

## Phát triển

Mỗi gói build, test và publish riêng. Các gói trỏ tới nhau bằng `file:../<gói>`, nên build và test
theo thứ tự phụ thuộc:

```bash
git clone git@github.com:flygo-opensource/spider-mesh.git && cd spider-mesh
for pkg in core events ws tcp; do
  (cd $pkg && bun install && bun run build && bun run test:e2e) || break
done
```

Cần Bun ≥ 1.4.2: bản cũ hơn cài dependency `file:` sai.

Publish theo thứ tự `core` → `events` → `ws` → `tcp`, từ thư mục của từng gói.

## License

ISC
