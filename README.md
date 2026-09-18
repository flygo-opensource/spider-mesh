# @spider-mesh/events

Pub/sub theo topic cho Spider Mesh. Một `EventBus` gửi và nhận event qua các **event transporter**,
thường là chính transporter đang dùng cho RPC:

| Transporter | Gói |
| --- | --- |
| `WebsocketTransporter` | `@spider-mesh/ws`: event đi qua relay |
| `Http2Pubsub` | `@spider-mesh/tcp`: event gửi thẳng giữa các node |

## Cài đặt

```bash
bun add @spider-mesh/core @spider-mesh/events rxjs
```

## Ví dụ nhanh

```ts
import { SpiderMesh } from '@spider-mesh/core'
import { EventBus } from '@spider-mesh/events'
import { WebsocketTransporter } from '@spider-mesh/ws/node'

// Class mô tả event; tên class là tên topic.
class OrderPaid {
  constructor(public orderId = '', public amount = 0) {}
}

const transporter = new WebsocketTransporter()
transporter.connect('ws://127.0.0.1:8787')

const mesh = new SpiderMesh({ transporters: [transporter] })
const events = new EventBus({ mesh })
events.registerTransporter(transporter)

const orderPaid = events.link(OrderPaid)

// Mọi process link cùng class đều nhận được.
orderPaid.listen().subscribe(event => console.log(event.orderId, event.amount))

await orderPaid.publish(new OrderPaid('o-1', 100))
```

- `listen()` trả `Observable`. Có thể gọi trước khi đăng ký transporter; nó bắt đầu nhận ngay khi
  có transporter.
- Event nhận được là dữ liệu thuần (các field), không phải instance của class.
- `publish()` ném lỗi nếu chưa có transporter nào được đăng ký.

## Topic theo tên

Không muốn dùng class thì đặt tên topic trực tiếp:

```ts
import { EventBus } from '@spider-mesh/events'

const events = new EventBus()
const chat = events.linkTopic<{ room: string; text: string }>('chat.message')

chat.listen().subscribe(message => console.log(message.room, message.text))
```

## Nhiều transporter

Đăng ký nhiều transporter thì mặc định (`mode: 'single'`) mỗi event chỉ đi qua **transporter đăng ký
đầu tiên**, để không nhận trùng. Có thể đổi cho cả bus hoặc cho từng topic:

```ts
import { EventBus } from '@spider-mesh/events'

class AuditLog {
  constructor(public message = '') {}
}

// Mọi topic gửi và nhận qua tất cả transporter.
const events = new EventBus({ mode: 'fanout' })

// Hoặc chỉ một topic, hoặc chỉ định đúng một transporter theo tên.
const audit = events.link(AuditLog, { mode: 'fanout' })
const auditOverRelay = events.link(AuditLog, { transporter: 'websocket' })
```

## Truyền `mesh` vào `EventBus`

`new EventBus({ mesh })` công bố danh sách topic node đang nghe kèm thông tin node. Transporter gửi
thẳng giữa các node (như `Http2Pubsub`) **cần** thông tin này để biết gửi event tới đâu. Mỗi process
nên có đúng một `EventBus` cho mỗi `SpiderMesh`.

## Tự viết transporter

Transporter chỉ cần một tên cố định, `publish()` và `listen()`. Ví dụ transporter trong bộ nhớ, hữu ích
khi test:

```ts
import { EventBus, type EventTransporter } from '@spider-mesh/events'
import { Observable, Subject, filter, map } from 'rxjs'

class InMemoryEventTransporter implements EventTransporter {
  // Tên cố định; dùng cho tuỳ chọn `transporter` khi link.
  public readonly name = 'in-memory'
  readonly #bus = new Subject<{ topic: string; data: unknown }>()

  async publish<T>(topic: string, data: T) {
    this.#bus.next({ topic, data })
  }

  listen<T>(topic: string): Observable<T> {
    return this.#bus.pipe(
      filter(message => message.topic === topic),
      map(message => message.data as T),
    )
  }
}

class UserCreated {
  constructor(public id = '') {}
}

const events = new EventBus()
events.registerTransporter(new InMemoryEventTransporter())

const userCreated = events.link(UserCreated)
userCreated.listen().subscribe(event => console.log('created', event.id))
await userCreated.publish(new UserCreated('42'))
```

`registerTransporter()` trả về một subscription; `unsubscribe()` để gỡ transporter.

## NestJS

```ts
import { Inject, Injectable, Module } from '@nestjs/common'
import { EventBus, NestJSLinkEvent } from '@spider-mesh/events'

class OrderPaid {
  constructor(public orderId = '') {}
}

@Injectable()
class ReceiptService {
  // Inject kiểu trả về của `events.link(OrderPaid)`.
  constructor(@Inject(OrderPaid) private readonly orderPaid: ReturnType<EventBus['link']>) {}

  onModuleInit() {
    this.orderPaid.listen().subscribe(event => console.log('send receipt', event))
  }
}

@Module({
  providers: [
    { provide: EventBus, useValue: new EventBus() },
    NestJSLinkEvent(OrderPaid),
    ReceiptService,
  ],
})
export class AppModule {}
```

## API

| Export | Ý nghĩa |
| --- | --- |
| `EventBus` | `registerTransporter()`, `link(Class, options?)`, `linkTopic(name, options?)` |
| `EventTransporter` | Kiểu cho transporter tự viết |
| `NestJSLinkEvent(Class)` | Provider NestJS trả về `events.link(Class)` |

| Tuỳ chọn | Ở đâu | Ý nghĩa |
| --- | --- | --- |
| `mesh` | `EventBus` | Công bố topic đang nghe cho transporter gửi thẳng giữa node. |
| `mode` | `EventBus`, `link` | `'single'` (mặc định) hoặc `'fanout'`. |
| `transporter` | `link` | Chỉ dùng transporter có tên này. |
