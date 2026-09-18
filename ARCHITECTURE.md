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
terminal thay vì một stream im lặng. Ba lớp cùng lo việc này, mỗi lớp phủ một kiểu đứt khác nhau:

1. **Relay** giữ `{ caller, provider, service }` cho mỗi request đang bay. Provider rớt →
   caller nhận `MICROSERVICE_OFFLINE`; caller rớt → provider nhận `cancel`. Với request
   round-robin, relay là nơi duy nhất biết chắc cặp này.
2. **Core** lưu `destination_node_id` trong `#rpc.pending` (từ `node_id` được pin, từ
   `destination_node_id` mà transporter trả về, hoặc từ `sender_node_id` của response đầu
   tiên) và `error()` mọi stream trỏ tới node vừa offline. Lớp này phủ các transporter không có
   relay.
3. **Transporter** giữ `request_id → socket` cho mọi request đã gửi mà chưa có response kết thúc.
   Khi socket tới relay mất (close, error hay `close()` chủ động), nó tự phát response
   `MICROSERVICE_OFFLINE` cho các request trên socket đó. Đây là lớp duy nhất phủ được ca **chính
   relay chết**: relay không còn để báo, còn transporter cố ý không phát `offline` cho node vì mất
   relay không có nghĩa là node chết. Membership giữ nguyên, và request không được tự gửi lại qua
   relay khác vì có thể đã chạy ở provider.

## Discovery-only variant

`WebsocketDiscoveryServer` và `WebsocketDiscoveryClient` chỉ đồng bộ hello/offline, không truyền
RPC. Variant này dùng khi data plane và discovery plane cần tách riêng.

## Protocol identity

Mọi RPC endpoint dùng metadata key `websocket`. Tên class Node/browser/React Native có thể khác
nhưng wire name không đổi.
