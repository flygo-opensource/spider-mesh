# Kiến trúc `@spider-mesh/ws`

## Một transporter, hai vai trò optional

`BaseWebsocketTransporter` luôn là RPC transporter (`name = 'websocket'`). Khi được truyền vào
`new Topology({ discovery: transporter })`, nó đồng thời implement discovery contract hai chiều.

```text
Không Topology:
SpiderMesh -> WebsocketTransporter -> Relay -> provider

Có Topology:
SpiderMesh -> Topology <-> WebsocketTransporter discovery frames
          -> WebsocketTransporter RPC -> Relay -> node đích
```

## Relay ownership

Relay sở hữu:

- socket/node directory;
- service index;
- round-robin state cho request chưa pin node;
- caller/provider correlation cho cancel và cho việc đóng stream khi một đầu rớt;
- event topic subscriptions.

Core không cần copy các state này khi không có nhu cầu enumerate node.

## Đóng RPC đang bay khi một đầu rớt

Một RPC trả `Observable` có thể sống rất lâu, nên mất kết nối giữa chừng phải thành một sự kiện
terminal thay vì một stream im lặng. Hai lớp cùng lo việc này và cố ý dư thừa:

1. **Relay** giữ `{ caller, provider, service }` cho mỗi request đang bay. Provider rớt →
   caller nhận `MICROSERVICE_OFFLINE`; caller rớt → provider nhận `cancel`. Với request
   round-robin, relay là nơi duy nhất biết chắc cặp này.
2. **Core** lưu `destination_node_id` trong `#rpc.pending` (từ `node_id` được pin, từ
   `destination_node_id` mà transporter trả về, hoặc từ `sender_node_id` của response đầu
   tiên) và `error()` mọi stream trỏ tới node vừa offline. Lớp này phủ trường hợp relay chết
   cùng lúc và các transporter không có relay.

## Discovery-only variant

`WebsocketDiscoveryServer` và `WebsocketDiscoveryClient` chỉ đồng bộ hello/offline, không truyền
RPC. Variant này dùng khi data plane và discovery plane cần tách riêng.

## Protocol identity

Mọi RPC endpoint dùng metadata key `websocket`. Tên class Node/browser/React Native có thể khác
nhưng wire name không đổi.
