# TODO

## ✅ RPC stream đang mở không được kết thúc khi provider offline — ĐÃ FIX (17/09/2026)

Phát hiện khi thiết kế composer hướng sự kiện cho `tiktok/views` (caller subscribe một method trả
`Observable` dài hạn qua `WebsocketTransporter`, provider là worker chạy qua relay).

### Hiện tượng (trước khi sửa)

Provider node rớt kết nối khỏi relay khi đang stream một RPC → `Observable` phía caller **treo im
vô hạn**: không `error`, không `complete`. Nếu caller không đặt `timeout` (là idle-timeout giữa hai
emission) thì không bao giờ biết provider đã chết. Request/response thường thì chờ hết `timeout`
thay vì báo `MICROSERVICE_OFFLINE` ngay, trái với ý định ghi ở `core/src/SpiderMesh.ts`.

Nguyên nhân: relay `on_close` chỉ xoá `#pendingRequests` của provider vừa chết mà không báo cho
caller, còn core chỉ cập nhật membership khi nhận `offline` chứ không duyệt `#rpc.pending`.

### Đã sửa

**1. Relay** (`src/WebsocketRelayServer.ts`) — `#pendingRequests` giờ lưu
`{ caller, provider, service }`. `#failPendingRequests(socket)` chạy đầu `on_close` và đóng cả
hai chiều:

- provider rớt → caller nhận `{ error: MICROSERVICE_OFFLINE, completed: true }`;
- caller rớt → provider nhận `cancel`, không còn rò rỉ stream đang chạy.

Request route tới một node vừa đóng socket cũng trả `MICROSERVICE_OFFLINE` ngay thay vì bị bỏ im.

**2. Core** (`core/src/SpiderMesh.ts`) — `PendingRpcStream` có thêm `destination_node_id`, lấy từ
`options.node_id`, từ `destination_node_id` mà `transporter.send()` trả về, hoặc từ
`sender_node_id` của response đầu tiên. Sự kiện `offline` gọi `#failPendingRpcForNode()` để
`error()` mọi stream trỏ tới node đó. Phủ trường hợp relay chết cùng lúc và transporter không relay.

Kèm theo: `RpcResponsePacket.sender_node_id` (optional) và `RpcTransporter.send()` trả thêm
`destination_node_id` (optional) — cả hai đều backward-compatible.

**3. Cancel race** (`core/src/SpiderMesh.ts`) — `cancelSend` được gán trong `.then()` của
`transporter.send(...)`; unsubscribe trước khi promise resolve thì `cancel` không bao giờ được gửi.
Giờ giữ cờ `cancelled` và gửi bù ngay khi `send` resolve.

### Test

- `tests/websocket-stream-lifecycle.e2e.test.ts` (3 test, đã nối vào `bun run test:e2e`):
  provider chết giữa stream → caller nhận `MICROSERVICE_OFFLINE` trong < 1 s mà **không** đặt
  `timeout`; unsubscribe thắng race vẫn gửi `cancel`; caller chết thì provider dừng stream.
- `core/tests/mock-e2e.test.ts` (3 test): cô lập lớp core, không cần relay.
