# @spider-mesh/ws — Test Coverage / Danh sách bài kiểm thử

All tests are end-to-end (E2E) — each test spawns real child processes connected through a WebSocket relay server.

Tất cả các bài kiểm thử đều là end-to-end (E2E) — mỗi bài chạy các tiến trình thực kết nối qua WebSocket relay server.

---

## `websocket-transporter.e2e.test.ts`

| Test | Input / Đầu vào | Output / Đầu ra |
|------|-----------------|-----------------|
| **websocket transporter e2e** | Gửi RPC request/cancel frame và kiểm tra relay định tuyến đúng | Binary frames truyền đúng định dạng msgpack |
| **websocket transporter exposes connection statuses** | Kết nối tới server hợp lệ và server không tồn tại | Status: `"connected"` và `"error"` tương ứng; `close()` xóa URL khỏi `status$` |

## `websocket-spidermesh.e2e.test.ts`

| Test | Input / Đầu vào | Output / Đầu ra |
|------|-----------------|-----------------|
| **websocket spidermesh rpc e2e** | Client gọi `GreetingService.hello()` qua relay | `"hello websocket e2e from provider"` |

## `websocket-spidermesh-reverse.e2e.test.ts`

| Test | Input / Đầu vào | Output / Đầu ra |
|------|-----------------|-----------------|
| **websocket spidermesh reverse rpc e2e** | Server expose service, Client gọi ngược lại | `"hello from server from client"` |

## `websocket-spidermesh-round-robin.e2e.test.ts`

| Test | Input / Đầu vào | Output / Đầu ra |
|------|-----------------|-----------------|
| **websocket spidermesh round-robin e2e** | 1 Client, 2 Providers (`provider-a`, `provider-b`), 4 RPC calls | 4 kết quả; cả 2 providers đều được gọi (round-robin) |

## `websocket-spidermesh-matrix.e2e.test.ts`

| Test | Input / Đầu vào | Output / Đầu ra |
|------|-----------------|-----------------|
| **websocket spidermesh rpc matrix e2e** | Gọi sync value, async value, sync Observable, async Observable, sync error, async error, observable error | 4 return types đúng giá trị; 3 error messages đúng |

## `websocket-missing.e2e.test.ts`

| Test | Input / Đầu vào | Output / Đầu ra |
|------|-----------------|-----------------|
| **websocket rpc timeout e2e** | Client gọi method không bao giờ resolve với `timeout: 2000ms` | `code: "MICROSERVICE_RPC_TIMEOUT"` sau ~2s |
| **websocket rpc fallback e2e** | Không có provider; client gọi với `fallback: "fallback-response"`, `retry: 0` | Nhận `"fallback-response"` thay vì throw |
| **websocket provider offline detection e2e** | Provider kết nối relay → client gọi thành công → provider SIGTERM → relay broadcast offline → client gọi lại | Lần 1: success; lần 2: `code: "MICROSERVICE_OFFLINE"` |
| **websocket concurrent rpc e2e** | 10 RPC calls song song qua relay đến cùng provider | 10 responses đúng, `allCorrect: true` |
| **websocket provider reconnect e2e** | `provider-v1` online → client gọi → `provider-v1` SIGTERM → relay offline broadcast → `provider-v2` kết nối → client gọi lại | `firstCallOk`, `providerOffline`, `reconnectOk` đều `true` |
| **websocket failover: 3 nodes then 1 offline e2e** | 3 providers online; 6 requests (phase 1); kill `provider-c`; 4 requests (phase 2) | Phase 1: cả 3 provider nhận request; Phase 2: chỉ `provider-a` và `provider-b`, không lỗi |

---

## Coverage Summary / Tóm tắt độ phủ

| Scenario | Covered |
|----------|---------|
| Basic RPC | ✅ |
| Reverse RPC | ✅ |
| Round-robin load balancing | ✅ |
| Sync / async / Observable return types | ✅ |
| Error propagation | ✅ |
| WebSocket connection status tracking | ✅ |
| RPC timeout | ✅ |
| Fallback value | ✅ |
| Provider disconnect / offline detection | ✅ |
| Concurrent RPC | ✅ |
| Provider reconnect | ✅ |
| Failover (kill 1 of 3 nodes) | ✅ |
| Cancel stream on unsubscribe | ✅ (via `send()` returning `{ cancel }`) |
