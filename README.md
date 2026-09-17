# @spider-mesh/events

Event API độc lập transporter cho Spider Mesh. RPC nằm trong `@spider-mesh/core`; event
transporter được đăng ký trên một `EventBus` riêng.

## Cài đặt

```bash
bun add @spider-mesh/events rxjs
```

```ts
import { SpiderMesh } from '@spider-mesh/core'
import { EventBus } from '@spider-mesh/events'

const mesh = new SpiderMesh()
const events = new EventBus({ mesh })

events.registerTransporter(eventTransporter)

class UserCreated {
  constructor(readonly id: string) {}
}

const userCreated = events.link(UserCreated)
userCreated.listen().subscribe(event => console.log(event.id))
await userCreated.publish(new UserCreated('42'))
```

Mỗi event transporter phải tự khai báo wire name ổn định, ví dụ
`public readonly name = 'nats-events'`. `EventBus` không nhận tên từ bên ngoài và không suy luận
từ `constructor.name`.

Mặc định `single` chỉ chọn một transporter để tránh nhận event trùng khi đăng ký nhiều transport.
Dùng `{ mode: 'fanout' }` khi chủ ý publish/listen qua tất cả transporter.

`EventBus({ mesh })` updates the node's topic and transporter metadata through
core's generic metadata bridge. This is required by brokerless transports such
as `Http2Pubsub` from `@spider-mesh/tcp`; broker-backed transports may omit `mesh`.

Mỗi mesh node nên có một `EventBus`. Nhiều bus nối cùng mesh sẽ cùng sở hữu và ghi đè topic
snapshot được quảng bá.

## Tự viết transporter

```ts
class NatsEventTransporter implements EventTransporter {
  public readonly name = 'nats-events'

  async publish<T>(topic: string, data: T): Promise<void> {
    nats.publish(topic, encode(data))
  }

  listen<T>(topic: string): Observable<T> {
    return listenNats<T>(nats, topic)
  }
}
```

Tên là wire identity do implementation hardcode. Không truyền tên vào `registerTransporter()`.

## Public API

| Export | Ý nghĩa |
| --- | --- |
| `EventBus` | Quản lý event topic và chọn transporter |
| `EventTransporter` | Contract publish/listen với `readonly name` |
| `EventMeshHost` | Bridge structural để công bố topic/metadata qua Core |
| `NestJSLinkEvent` | Provider helper cho NestJS |

Ví dụ chạy độc lập: workspace `examples/src/events/in-memory.ts`.
