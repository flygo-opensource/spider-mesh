# Bắt đầu với TCP trên pure Linux/PM2

## Provider và client

Cả hai process dùng cùng Discovery namespace và cấu hình giống nhau:

```ts
const discovery = new UdpDiscovery<SpiderMeshNode>({
  namespace: 'my-app',
  tags: ['spider-mesh', 'node'],
})

const topology = new Topology({
  discovery: new TopologyDiscoveryAdapter(discovery),
  staleAfterMs: 15_000,
})

const mesh = new SpiderMesh({
  topology,
  transporters: [new Http2Rpc()],
})
```

Provider đăng ký service bằng decorator:

```ts
@Microservice()
class GreetingService {
  hello(name: string) {
    return `Hello ${name}`
  }
}

new GreetingService()
```

Client tạo typed proxy:

```ts
type GreetingService = {
  hello(name: string): Promise<string>
}

const greeting = RemoteServiceLinker.link<GreetingService>(mesh, {
  service: 'GreetingService',
  timeout: 3_000,
})

await greeting.wait()
console.log(await greeting.hello('Spider Mesh'))
```

## Nhiều transporter

```ts
const mesh = new SpiderMesh({
  topology,
  transporters: [http2, grpc, rawTcp],
})

await remote.set({ transporter: 'grpc' }).method()
```

Tên `http2`, `grpc`, `raw-tcp` do chính implementation hardcode. Không truyền name khi đăng ký.

## Khi không cần Topology

Nếu có load balancer/DNS resolver, truyền `resolveService` cho `Http2Rpc` và bỏ Topology.
`wait()` lúc này dùng `probe()`; các API enumerate node không khả dụng.
