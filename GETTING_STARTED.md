# Bắt đầu với WebSocket

## 1. Chạy relay

```ts
const relay = new WebsocketRelayServer({ port: 8787 })
```

## 2. Provider

```ts
const transporter = new WebsocketTransporter()
transporter.connect('ws://127.0.0.1:8787')

const mesh = new SpiderMesh({ transporters: [transporter] })

@Microservice()
class GreetingService {
  hello(name: string) {
    return `Hello ${name}`
  }
}

new GreetingService()
```

## 3. Client

```ts
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

Không có Topology, `wait()` dùng directory/probe của transporter và relay tự chọn provider.

Nếu ứng dụng cần biết chính xác danh sách node:

```ts
const topology = new Topology({ discovery: transporter })
const mesh = new SpiderMesh({ topology, transporters: [transporter] })
```
