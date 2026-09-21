# Tài liệu E2E Tests — `@spider-mesh/ws`

Tất cả các test đều là **end-to-end (e2e)**: mỗi test khởi chạy các tiến trình con thực tế (bằng `bun run`), để chúng giao tiếp qua WebSocket thật, rồi kiểm tra stdout để xác nhận kết quả.

---

## Kiến trúc giao tiếp WebSocket

```
┌──────────────┐   WebSocket   ┌─────────────────────┐   WebSocket   ┌──────────────┐
│   Client     │ ────────────► │   Relay Server      │ ◄──────────── │   Provider   │
│ (WsTransporter)│ ◄─────────── │ (WebsocketRelayServer)│ ──────────── │ (WsTransporter)│
└──────────────┘               └─────────────────────┘               └──────────────┘
```

- Tất cả traffic đều đi qua **relay server trung tâm** — các node không kết nối trực tiếp với nhau.
- Relay server đóng vai trò: router (định tuyến gói tin), registry (quản lý danh sách node), broadcaster (phát sự kiện offline đến tất cả).
- Khi provider ngắt kết nối, relay **tự động broadcast** sự kiện offline đến tất cả client đang kết nối — không cần warmup connection như TCP.

---

## Cách chạy test

```bash
# Chạy toàn bộ test
cd /path/to/ws
bun test

# Chạy một file test cụ thể
bun test tests/websocket-spidermesh.e2e.test.ts

# Chạy một harness riêng lẻ để debug
bun run examples/websocket-e2e-test.ts
```

---

## Nhóm 1 — Tests cơ bản (Core RPC)

### `websocket-spidermesh.e2e.test.ts`
**Harness:** `examples/websocket-e2e-test.ts`

**Mục đích:** Kiểm tra luồng RPC đơn giản nhất — client gọi một method trên provider qua relay và nhận kết quả.

**Kịch bản:**
1. Relay server khởi động, lắng nghe kết nối WebSocket
2. Client kết nối đến relay (`client connected`)
3. Provider kết nối đến relay, đăng ký service `GreetingService` (`provider ready`)
4. Relay thông báo provider online đến client
5. Client gọi `hello()` → relay định tuyến đến provider → provider trả về kết quả → relay chuyển về client

**Kiểm tra:**
```
stdout chứa: "hello websocket e2e from provider"
```

**Tiến trình tham gia:**
| Vai trò | File |
|---------|------|
| Relay | `examples/websocket-server.ts` |
| Client | `examples/websocket-e2e-client.ts` |
| Provider | `examples/websocket-e2e-provider.ts` |

---

## Nhóm 2 — Tests kiểm tra transporter (Transporter Tests)

### `websocket-transporter.e2e.test.ts`

Gồm **2 test case**: một e2e và một unit test trực tiếp trên `WebsocketTransporter`.

---

#### Test 1: `websocket transporter e2e`
**Harness:** `examples/websocket-smoke-test.ts`

**Mục đích:** Kiểm tra giao thức binary của WebSocket transporter — gửi và nhận các loại packet khác nhau qua relay, đảm bảo encode/decode đúng.

**Kịch bản:**
1. Relay server khởi động
2. Hai node (`nodeA`, `nodeB`) kết nối đến relay với vai trò khác nhau (server/client)
3. Gửi nhiều loại packet:
   - RPC `request` packet từ A đến B
   - RPC `cancel` packet (cancel được phép)
   - RPC `request` packet từ B đến A
   - RPC `cancel` packet (cancel bị chặn — kiểm tra quyền)
4. Kiểm tra relay định tuyến đúng và các node nhận đúng packet

**Kiểm tra:**
```
stdout chứa: "WebSocket binary smoke test passed"
```

---

#### Test 2: `websocket transporter exposes connection statuses`
**Test loại:** Unit test (không spawn tiến trình con)

**Mục đích:** Kiểm tra trực tiếp API `status$` của `WebsocketTransporter` — theo dõi trạng thái kết nối theo thời gian thực.

**Kịch bản:**
1. Tạo `WebsocketTransporter` trong bộ nhớ (không spawn process)
2. Kết nối đến một server thật → trạng thái: `"connecting"` → `"connected"`
3. Kết nối đến một port không tồn tại → trạng thái: `"connecting"` → `"error"`
4. Đóng cả hai kết nối → trạng thái biến mất khỏi `status$`

**Các trạng thái kết nối:**
| Trạng thái | Ý nghĩa |
|-----------|---------|
| `"connecting"` | Đang thiết lập kết nối |
| `"connected"` | Kết nối thành công |
| `"error"` | Kết nối thất bại |
| *(không có)* | Kết nối đã đóng, đã xóa |

---

## Nhóm 3 — Tests phân phối tải (Load Distribution)

### `websocket-spidermesh-round-robin.e2e.test.ts`
**Harness:** `examples/websocket-e2e-round-robin-test.ts`

**Mục đích:** Kiểm tra rằng khi có nhiều provider, relay phân phối request theo thuật toán **round-robin** (lần lượt từng node).

**Kịch bản:**
1. Relay khởi động
2. **2 provider** (`provider-a`, `provider-b`) cùng kết nối và đăng ký cùng service
3. Client gửi **4 request** liên tiếp
4. Relay định tuyến lần lượt đến từng provider

**Kiểm tra:**
```typescript
// 4 kết quả, 2 provider khác nhau → round-robin phân đều
results.length === 4
new Set(providers).size === 2
providers[0].includes('provider-')
```

**Tiến trình tham gia:**
| Vai trò | File | Biến môi trường |
|---------|------|-----------------|
| Relay | `examples/websocket-server.ts` | `WS_PORT` |
| Client | `examples/websocket-e2e-round-robin-client.ts` | `WS_URL` |
| Provider A | `examples/websocket-e2e-provider.ts` | `PROVIDER_ID=provider-a` |
| Provider B | `examples/websocket-e2e-provider.ts` | `PROVIDER_ID=provider-b` |

---

### `websocket-spidermesh-reverse.e2e.test.ts`
**Harness:** `examples/websocket-e2e-reverse-test.ts`

**Mục đích:** Kiểm tra **reverse RPC** — trong mô hình WS, "server" (node được gọi) thực chất là một WebSocket client kết nối đến relay; "client" (bên gọi) cũng là một WebSocket client. Bài test đảo vai trò: "server" gọi ngược "client".

**Kịch bản:**
1. Server kết nối đến relay, đăng ký service của mình
2. Client kết nối đến relay, **đăng ký service của mình** với server
3. Server gọi ngược lại method trên client qua relay
4. Server nhận và in kết quả

**Kiểm tra:**
```
stdout chứa: "hello from server from client"
```

**Tiến trình tham gia:**
| Vai trò | File |
|---------|------|
| Relay | `examples/websocket-server.ts` |
| Server | `examples/websocket-e2e-reverse-server.ts` |
| Client | `examples/websocket-e2e-reverse-client.ts` |

---

### `websocket-spidermesh-matrix.e2e.test.ts`
**Harness:** `examples/websocket-e2e-matrix-test.ts`

**Mục đích:** Kiểm tra **4 dạng return value** của RPC method qua WebSocket.

**Ma trận test (2×2):**

| | Sync | Async |
|---|---|---|
| **Giá trị đơn** | `syncValue` | `asyncValue` |
| **Observable stream** | `syncObservable` | `asyncObservable` |

Ngoài ra còn test **lỗi** từ cả 3 dạng: sync error, async error, observable error.

**Kiểm tra:**
```
'"syncValue":"sync:case-sync"'
'"asyncValue":"async:case-async"'
'"syncObservable":["sync-observable:case-sync-observable:1","sync-observable:case-sync-observable:2"]'
'"asyncObservable":["async-observable:case-async-observable:1","async-observable:case-async-observable:2"]'
'"errors":["sync-error","async-error","observable-error"]'
```

**Tiến trình tham gia:**
| Vai trò | File |
|---------|------|
| Relay | `examples/websocket-server.ts` |
| Client | `examples/websocket-e2e-matrix-client.ts` |
| Provider | `examples/websocket-e2e-matrix-provider.ts` |

---

## Nhóm 4 — Tests tính bền vững (Resilience)

### `websocket-missing.e2e.test.ts`

Gồm **6 test case** kiểm tra các tình huống lỗi và phục hồi.

---

#### Test 1: `websocket rpc timeout e2e`
**Harness:** `examples/websocket-timeout-test.ts`

**Mục đích:** Kiểm tra rằng khi provider không trả lời trong thời hạn quy định, client nhận đúng lỗi timeout.

**Kịch bản:**
1. Relay khởi động
2. Provider khởi động với một method **không bao giờ resolve** (treo mãi)
3. Client gọi method đó với `timeout: 2000ms`
4. Sau 2 giây, client phải nhận lỗi `MICROSERVICE_RPC_TIMEOUT`

**Kiểm tra:**
```
'"timeoutDetected":true'
'"code":"MICROSERVICE_RPC_TIMEOUT"'
```

**Tiến trình tham gia:**
| Vai trò | File |
|---------|------|
| Relay | `examples/websocket-server.ts` |
| Provider | `examples/websocket-timeout-provider.ts` |
| Client | `examples/websocket-timeout-client.ts` |

---

#### Test 2: `websocket rpc fallback e2e`
**Harness:** `examples/websocket-fallback-test.ts`

**Mục đích:** Kiểm tra rằng khi gọi service không tồn tại, client nhận được **giá trị fallback** thay vì throw lỗi.

**Kịch bản:**
1. Relay khởi động
2. **Không khởi động provider nào** — service không tồn tại trên mạng
3. Client gọi với cấu hình: `fallback: "fallback-response"`, `retry: 0`
4. Relay trả về lỗi `MICROSERVICE_OFFLINE` → client trả về giá trị fallback

**Kiểm tra:**
```
'"fallbackReceived":true'
'"value":"fallback-response"'
```

**Tiến trình tham gia:**
| Vai trò | File |
|---------|------|
| Relay | `examples/websocket-server.ts` |
| Client | `examples/websocket-fallback-client.ts` |

---

#### Test 3: `websocket provider offline detection e2e`
**Harness:** `examples/websocket-offline-test.ts`

**Mục đích:** Kiểm tra rằng khi provider ngắt kết nối, relay phát hiện và **broadcast sự kiện offline** đến tất cả client.

**Cơ chế hoạt động (WS-specific):**
- Khi provider ngắt kết nối WebSocket, relay server nhận sự kiện `close` của socket.
- Relay broadcast message `{ type: "offline", node_id: "..." }` đến **tất cả client đang kết nối**.
- Client cập nhật Registry cục bộ, xóa node khỏi danh sách → call tiếp theo nhận `MICROSERVICE_OFFLINE` ngay lập tức.
- **Không cần warmup connection** như TCP — relay xử lý hoàn toàn tự động.

**Kịch bản:**
1. Relay khởi động
2. Provider kết nối, đăng ký service
3. Client kết nối, **gọi thành công lần 1** → ghi nhận `firstCallOk: true`
4. Harness gửi `SIGTERM` đến provider
5. Relay phát hiện disconnect → broadcast offline
6. Client cập nhật Registry → gọi lại nhận `MICROSERVICE_OFFLINE` → ghi nhận `offlineDetected: true`

**Kiểm tra:**
```
'"firstCallOk":true'
'"offlineDetected":true'
'"code":"MICROSERVICE_OFFLINE"'
```

**Tiến trình tham gia:**
| Vai trò | File |
|---------|------|
| Relay | `examples/websocket-server.ts` |
| Provider | `examples/websocket-e2e-provider.ts` |
| Client | `examples/websocket-offline-client.ts` |

---

#### Test 4: `websocket concurrent rpc e2e`
**Harness:** `examples/websocket-concurrent-test.ts`

**Mục đích:** Kiểm tra rằng nhiều RPC call gửi **đồng thời** qua cùng một WebSocket connection đến relay đều nhận đủ kết quả, không bị trộn lẫn response.

**Kịch bản:**
1. Relay và provider khởi động
2. Client gửi **10 request song song** bằng `Promise.all()`
3. Relay định tuyến đến provider, provider xử lý đồng thời
4. Tất cả 10 kết quả phải đúng và đầy đủ

**Kiểm tra:**
```
'"concurrentOk":true'
'"count":10'
'"allCorrect":true'
```

**Tiến trình tham gia:**
| Vai trò | File |
|---------|------|
| Relay | `examples/websocket-server.ts` |
| Provider | `examples/websocket-e2e-provider.ts` |
| Client | `examples/websocket-concurrent-client.ts` |

---

#### Test 5: `websocket provider reconnect e2e`
**Harness:** `examples/websocket-reconnect-test.ts`

**Mục đích:** Kiểm tra rằng khi provider cũ bị kill và provider mới kết nối lại (cùng service), client có thể **tự động phát hiện và tiếp tục gọi** provider mới mà không cần restart.

**Đây là tính năng đặc trưng của WS** — nhờ relay broadcast cả sự kiện online lẫn offline, client luôn có danh sách node cập nhật.

**Kịch bản:**
```
provider-v1 kết nối ──► client gọi OK ──► provider-v1 bị kill
                                              ↓ relay broadcast offline
                                           client phát hiện offline
                                              ↓ provider-v2 kết nối lại
                                           client gọi OK với provider-v2
```

1. Relay khởi động
2. `provider-v1` kết nối, đăng ký `GreetingService`
3. Client kết nối, **gọi thành công lần 1** → `firstCallOk: true`
4. Harness kill `provider-v1`
5. Client nhận offline event → `providerOffline: true`
6. `provider-v2` kết nối (cùng service, ID khác)
7. Client phát hiện `provider-v2` → **gọi thành công lần 2** → `reconnectOk: true`

**Kiểm tra:**
```
'"firstCallOk":true'
'"providerOffline":true'
'"reconnectOk":true'
```

**Tiến trình tham gia:**
| Vai trò | File | Biến môi trường |
|---------|------|-----------------|
| Relay | `examples/websocket-server.ts` | `WS_PORT` |
| Provider V1 | `examples/websocket-e2e-provider.ts` | `PROVIDER_ID=provider-v1` |
| Provider V2 | `examples/websocket-e2e-provider.ts` | `PROVIDER_ID=provider-v2` |
| Client | `examples/websocket-reconnect-client.ts` | `WS_URL` |

---

#### Test 6: `websocket failover: 3 nodes then 1 offline e2e`
**Harness:** `examples/websocket-failover-test.ts`

**Mục đích:** Kiểm tra rằng sau khi một node offline, các request tiếp theo tự động **chuyển sang các node còn lại** mà không bị gián đoạn.

**Kịch bản:**
```
Phase 1:  provider-a ─┐
          provider-b ─┼─ Relay (round-robin) ← 6 requests → client
          provider-c ─┘

                ↓ providerC.kill('SIGTERM')
                ↓ Relay broadcast offline → client cập nhật Registry

Phase 2:  provider-a ─┐
          provider-b ─┴─ Relay (round-robin) ← 4 requests → client
          provider-c ✗ (offline, đã xóa khỏi Registry)
```

1. Relay khởi động
2. 3 provider (`provider-a`, `provider-b`, `provider-c`) cùng kết nối
3. Client chờ đủ 3 node trong Registry
4. **Phase 1:** Client gửi 6 request — cả 3 provider đều được gọi
5. Harness kill `provider-c`
6. Relay broadcast offline → client cập nhật Registry
7. **Phase 2:** Client gửi 4 request — chỉ 2 provider còn lại phục vụ, không có lỗi

**Kiểm tra:**
```typescript
// Phase 1
phase1.length === 6
phase1UniqueProviders === 3          // cả 3 đều nhận request

// Phase 2
phase2.length === 4
phase2UniqueProviders === 2          // chỉ 2 còn lại
phase2.every(r => !r.includes('provider-c'))  // provider-c không xuất hiện
```

**Tiến trình tham gia:**
| Vai trò | File | Biến môi trường |
|---------|------|-----------------|
| Relay | `examples/websocket-server.ts` | `WS_PORT` |
| Provider A | `examples/websocket-e2e-provider.ts` | `PROVIDER_ID=provider-a` |
| Provider B | `examples/websocket-e2e-provider.ts` | `PROVIDER_ID=provider-b` |
| Provider C | `examples/websocket-e2e-provider.ts` | `PROVIDER_ID=provider-c` |
| Client | `examples/websocket-failover-client.ts` | `WS_URL` |

---

## Test đang phát triển

### Discovery / Phát hiện node online-offline
**Harness:** `examples/websocket-discovery-test.ts`
**Observer:** `examples/websocket-discovery-observer.ts`

**Mục đích:** Kiểm tra rằng khi có nhiều tiến trình mỗi tiến trình cung cấp các service khác nhau, một observer có thể theo dõi **số lượng node online cho từng service** theo thời gian thực.

**Kịch bản:**
```
Provider 1 (SERVICES=a):    ServiceA ─────────────────────── SIGTERM ─►
Provider 2 (SERVICES=a,b):  ServiceA + ServiceB ──────────────────── SIGTERM ─►
Observer:     watchService('ServiceA') + watchService('ServiceB')
```

**3 Milestone:**
| Milestone | Trạng thái | Output mong đợi |
|-----------|-----------|-----------------|
| 1 | Cả 2 provider online | `{"serviceA":2,"serviceB":1}` |
| 2 | Provider 1 bị kill | `{"serviceA":1,"serviceB":1}` |
| 3 | Provider 2 bị kill | `{"serviceA":0,"serviceB":0}` |

**Lưu ý kỹ thuật (WS-specific):** Trong WS, relay tự động broadcast offline event khi provider ngắt kết nối — **không cần health check** hay warmup connection. Observer chỉ cần subscribe `watchService()` là đủ.

**Biến môi trường provider:**
- `SERVICES=a` → chỉ khởi tạo `ServiceA`
- `SERVICES=a,b` → khởi tạo cả `ServiceA` và `ServiceB`

---

## So sánh TCP vs WebSocket

| Đặc điểm | TCP (`Http2Rpc`) | WebSocket (`WsTransporter`) |
|----------|-----------------|------------------------------|
| Topology | Peer-to-peer (direct) | Hub-and-spoke (qua relay) |
| Offline detection | Connection close → Http2Rpc phát event | Relay broadcast offline đến tất cả |
| Warmup cần thiết | Có — phải có connection mở trước | Không — relay tự xử lý |
| Reconnect | Client tự kết nối lại | Relay phát hiện, client nhận event |
| Discovery | mDNS hoặc Registry ngoài | Relay server là registry trung tâm |
| Heartbeat | Không (TCP keep-alive) | Có — heartbeat qua WebSocket |

---

## Tổng quan các biến môi trường

| Biến | Dùng bởi | Ý nghĩa |
|------|----------|---------|
| `WS_PORT` | relay server | Cổng lắng nghe của relay |
| `WS_URL` | tất cả client/provider | URL kết nối đến relay (`ws://host:port`) |
| `PROVIDER_ID` | round-robin, failover, reconnect | Định danh provider trong kết quả |
| `SERVICES` | discovery tests | Danh sách service cần khởi tạo (`a`, `a,b`) |

---

## Cơ chế E2E Harness

Tất cả harness sử dụng các helper trong `examples/helpers/e2eHarness.ts`:

- **`randomPort(base)`** — Chọn port ngẫu nhiên trong khoảng `[base, base+200)` để tránh conflict giữa các test
- **`createWsTestEnv(port)`** — Tạo environment variables `WS_PORT` và `WS_URL`
- **`start(name, file, env)`** — Spawn tiến trình con `bun run <file>` với env
- **`waitForOutput(child, regex, timeoutMs, name)`** — Chờ stdout khớp regex, timeout → throw
- **`stopAll()`** — Gửi SIGTERM đến tất cả tiến trình con, đợi chúng thoát

**Lưu ý về listener gap:** `waitForOutput` chỉ lắng nghe output **từ lúc gọi đến khi regex khớp**. Nếu output được emit giữa hai lần gọi `waitForOutput`, output đó có thể bị mất. Các harness xử lý điều này bằng cách **in ra từng milestone ngay khi nhận được** thay vì chờ đến cuối.
