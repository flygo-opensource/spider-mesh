# Tài liệu E2E Tests — `@spider-mesh/tcp`

Tất cả các test đều là **end-to-end (e2e)**: mỗi test khởi chạy các tiến trình con thực tế (bằng `bun run`), để chúng giao tiếp qua giao thức HTTP/2 thật, rồi kiểm tra stdout để xác nhận kết quả.

---

## Kiến trúc giao tiếp TCP

```
┌──────────────┐   HTTP/2 request   ┌──────────────┐
│   Client     │ ─────────────────► │   Provider   │
│  (Http2Rpc)  │ ◄───────────────── │  (Http2Rpc)  │
└──────────────┘   HTTP/2 response  └──────────────┘
```

- Mỗi node lắng nghe trên một cổng TCP được phân bổ ngẫu nhiên.
- **Không có relay trung gian** — các node kết nối trực tiếp với nhau (peer-to-peer).
- Phát hiện node online/offline dựa vào sự kiện đóng kết nối HTTP/2.
- Discovery (tìm kiếm node) được thực hiện qua mDNS hoặc Registry nội bộ.

---

## Cách chạy test

```bash
# Chạy toàn bộ test
cd /path/to/tcp
bun test

# Chạy một file test cụ thể
bun test tests/tcp-spidermesh.e2e.test.ts

# Chạy một harness riêng lẻ để debug
bun run examples/tcp-e2e-test.ts
```

---

## Nhóm 1 — Tests cơ bản (Core RPC)

### `tcp-spidermesh.e2e.test.ts`
**Harness:** `examples/tcp-e2e-test.ts`

**Mục đích:** Kiểm tra luồng RPC đơn giản nhất — client gọi một method trên provider và nhận kết quả.

**Kịch bản:**
1. Client khởi động, kết nối vào mạng TCP (`TCP e2e client connected`)
2. Provider khởi động, đăng ký service `GreetingService`
3. Client phát hiện provider qua Registry, gọi `hello()`
4. Provider trả về chuỗi kết quả

**Kiểm tra:**
```
stdout chứa: "hello tcp e2e from provider"
```

**Tiến trình tham gia:**
| Vai trò | File |
|---------|------|
| Client  | `examples/tcp-e2e-client.ts` |
| Provider | `examples/tcp-e2e-provider.ts` |

---

### `tcp-transporters.e2e.test.ts`
**Harness:** `examples/tcp-smoke-test.ts`

**Mục đích:** Kiểm tra cùng lúc cả **RPC** và **PubSub** trên TCP transporter.

**Kịch bản:**
1. Provider khởi động, lắng nghe cả RPC lẫn PubSub topic
2. Client kết nối, thực hiện:
   - Gọi RPC `hello()` → nhận chuỗi trả về
   - Publish một message lên topic → provider nhận được
3. Cả hai đường truyền đều hoạt động đồng thời

**Kiểm tra:**
```
stdout chứa: "TCP transporter smoke test passed"
stdout chứa: "hello tcp smoke from smoke provider"
stdout chứa: '"message":"pubsub-ok"'
```

**Tiến trình tham gia:**
| Vai trò | File |
|---------|------|
| Provider | `examples/tcp-smoke-provider.ts` |
| Client  | `examples/tcp-smoke-client.ts` |

---

## Nhóm 2 — Tests kiểm tra hợp đồng transporter (Contracts)

### `tcp-contracts.e2e.test.ts`

Gồm **2 test case** kiểm tra cấu trúc dữ liệu của các sự kiện phát ra từ transporter.

---

#### Test 1: `tcp rpc transporter contract e2e`
**Harness:** `examples/tcp-rpc-contract-test.ts`

**Mục đích:** Xác nhận rằng khi `Http2Rpc` nhận một RPC packet, nó phát ra sự kiện có đúng cấu trúc — không bọc thêm lớp wrapper nào.

**Kịch bản:**
1. Tạo 2 instance `Http2Rpc`: `server` và `client`
2. Tạo `Registry`, thêm node server vào với địa chỉ cổng thực
3. Client gửi packet kiểu `request` trực tiếp đến server:
   ```json
   { "kind": "request", "sender_node_id": "rpc-contract-client", "destination_node_id": "rpc-contract-server", ... }
   ```
4. Lắng nghe sự kiện phát ra từ server, kiểm tra cấu trúc

**Kiểm tra:**
```
stdout chứa: '"hasRpc":true'         — sự kiện có trường "rpc"
stdout chứa: '"hasMessage":false'    — không có trường "message" dư thừa
stdout chứa: '"packetKind":"request"' — packet là loại request
stdout chứa: "TCP RPC contract test passed"
```

> **Lưu ý cấu trúc API:** `RpcEvent` phát ra packet thô — truy cập bằng `event.rpc.kind`, `event.rpc.sender_node_id`, v.v. Không có wrapper `event.rpc.packet.*`.

---

#### Test 2: `tcp discovery transporter contract e2e`
**Harness:** `examples/tcp-discovery-contract-test.ts`

**Mục đích:** Xác nhận cấu trúc của sự kiện Discovery (tìm kiếm node).

**Kịch bản:**
1. Listener khởi động, lắng nghe các sự kiện discovery
2. Sender phát broadcast thông tin node của mình
3. Listener nhận được sự kiện, kiểm tra cấu trúc

**Kiểm tra:**
```
stdout chứa: '"hasDiscovered":true'
stdout chứa: '"hasRawNodeId":false'              — không có trường raw "node_id" dư thừa
stdout chứa: '"discoveredNodeId":"discovery-contract-sender"'
stdout chứa: "TCP discovery contract test passed"
```

---

## Nhóm 3 — Tests phân phối tải (Load Distribution)

### `tcp-spidermesh-round-robin.e2e.test.ts`
**Harness:** `examples/tcp-e2e-round-robin-test.ts`

**Mục đích:** Kiểm tra rằng khi có nhiều provider, Registry phân phối các request theo thuật toán **round-robin** (lần lượt từng node).

**Kịch bản:**
1. Client khởi động, đăng ký với Registry
2. **2 provider** (`provider-a`, `provider-b`) cùng khởi động, đăng ký cùng service `GreetingService`
3. Client gửi **4 request** liên tiếp
4. Kết quả được thu thập, kiểm tra mỗi provider đều được gọi

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
| Client | `examples/tcp-e2e-round-robin-client.ts` | — |
| Provider A | `examples/tcp-e2e-provider.ts` | `PROVIDER_ID=provider-a` |
| Provider B | `examples/tcp-e2e-provider.ts` | `PROVIDER_ID=provider-b` |

---

### `tcp-spidermesh-reverse.e2e.test.ts`
**Harness:** `examples/tcp-e2e-reverse-test.ts`

**Mục đích:** Kiểm tra **reverse RPC** — server gọi ngược lại client. Trong mô hình thông thường, client là bên gọi; trong bài test này, vai trò bị đảo ngược.

**Kịch bản:**
1. Server khởi động, kết nối vào mạng
2. Client kết nối đến server, **đăng ký service của mình** với server
3. Server gọi ngược lại method trên client
4. Server nhận và in kết quả

**Kiểm tra:**
```
stdout chứa: "hello from server from client"
```

**Tiến trình tham gia:**
| Vai trò | File |
|---------|------|
| Server | `examples/tcp-e2e-reverse-server.ts` |
| Client | `examples/tcp-e2e-reverse-client.ts` |

---

### `tcp-spidermesh-matrix.e2e.test.ts`
**Harness:** `examples/tcp-e2e-matrix-test.ts`

**Mục đích:** Kiểm tra **4 dạng return value** của RPC method — SpiderMesh hỗ trợ cả sync/async và cả giá trị đơn lẫn Observable stream.

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
| Client | `examples/tcp-e2e-matrix-client.ts` |
| Provider | `examples/tcp-e2e-matrix-provider.ts` |

---

## Nhóm 4 — Tests tính bền vững (Resilience)

### `tcp-missing.e2e.test.ts`

Gồm **5 test case** kiểm tra các tình huống lỗi và phục hồi.

---

#### Test 1: `tcp rpc timeout e2e`
**Harness:** `examples/tcp-timeout-test.ts`

**Mục đích:** Kiểm tra rằng khi provider không trả lời trong thời hạn quy định, client nhận đúng lỗi timeout.

**Kịch bản:**
1. Provider khởi động với một method **không bao giờ resolve** (treo mãi)
2. Client gọi method đó với `timeout: 2000ms`
3. Sau 2 giây, client phải nhận lỗi `MICROSERVICE_RPC_TIMEOUT`

**Kiểm tra:**
```
'"timeoutDetected":true'
'"code":"MICROSERVICE_RPC_TIMEOUT"'
```

**Tiến trình tham gia:**
| Vai trò | File |
|---------|------|
| Provider | `examples/tcp-timeout-provider.ts` |
| Client | `examples/tcp-timeout-client.ts` |

---

#### Test 2: `tcp rpc fallback e2e`
**Harness:** `examples/tcp-fallback-test.ts`

**Mục đích:** Kiểm tra rằng khi gọi service không tồn tại, client nhận được **giá trị fallback** thay vì throw lỗi.

**Kịch bản:**
1. **Không khởi động provider nào** — service không tồn tại trên mạng
2. Client gọi với cấu hình: `fallback: "fallback-response"`, `retry: 0`
3. Client nhận được giá trị fallback mà không crash

**Kiểm tra:**
```
'"fallbackReceived":true'
'"value":"fallback-response"'
```

**Tiến trình tham gia:**
| Vai trò | File |
|---------|------|
| Client | `examples/tcp-fallback-client.ts` |

> **Lưu ý:** Đây là test duy nhất không khởi động provider.

---

#### Test 3: `tcp provider offline detection e2e`
**Harness:** `examples/tcp-offline-test.ts`

**Mục đích:** Kiểm tra rằng khi provider bị kill đột ngột, client phát hiện được sự kiện offline.

**Cơ chế hoạt động:**
- `Http2Rpc` giữ một HTTP/2 session (kết nối liên tục) đến provider sau lần gọi đầu tiên.
- Khi provider chết, kết nối TCP đóng lại → `Http2Rpc` nhận sự kiện `close` → phát ra event `{ offline: node_id }` → Registry xóa node → client nhận lỗi `MICROSERVICE_OFFLINE`.

**Kịch bản:**
1. Provider khởi động
2. Client kết nối và **gọi thành công lần 1** → ghi nhận `firstCallOk: true`
3. Harness gửi `SIGTERM` đến provider (giả lập crash)
4. Client phát hiện provider offline → ghi nhận `offlineDetected: true`

**Kiểm tra:**
```
'"firstCallOk":true'
'"offlineDetected":true'
'"code":"MICROSERVICE_OFFLINE"'
```

**Tiến trình tham gia:**
| Vai trò | File |
|---------|------|
| Provider | `examples/tcp-e2e-provider.ts` |
| Client | `examples/tcp-offline-client.ts` |

---

#### Test 4: `tcp concurrent rpc e2e`
**Harness:** `examples/tcp-concurrent-test.ts`

**Mục đích:** Kiểm tra rằng nhiều RPC call gửi **đồng thời** đến cùng một provider đều nhận đủ kết quả, không bị mất response.

**Kịch bản:**
1. Provider khởi động
2. Client gửi **10 request song song** bằng `Promise.all()`
3. Tất cả 10 kết quả phải đúng và đầy đủ

**Kiểm tra:**
```
'"concurrentOk":true'
'"count":10'
'"allCorrect":true'
```

**Tiến trình tham gia:**
| Vai trò | File |
|---------|------|
| Provider | `examples/tcp-e2e-provider.ts` |
| Client | `examples/tcp-concurrent-client.ts` |

---

#### Test 5: `tcp failover: 3 nodes then 1 offline e2e`
**Harness:** `examples/tcp-failover-test.ts`

**Mục đích:** Kiểm tra rằng sau khi một node offline, các request tiếp theo tự động **chuyển sang các node còn lại** mà không bị gián đoạn.

**Kịch bản:**
```
Phase 1:  provider-a ─┐
          provider-b ─┼─ Registry (round-robin) ← 6 requests → client
          provider-c ─┘

                ↓ providerC.kill('SIGTERM')

Phase 2:  provider-a ─┐
          provider-b ─┴─ Registry (round-robin) ← 4 requests → client
          provider-c ✗ (offline, đã xóa khỏi Registry)
```

1. Khởi động 3 provider (`provider-a`, `provider-b`, `provider-c`)
2. Client chờ đủ 3 node trong Registry
3. **Phase 1:** Client gửi 6 request — cả 3 provider đều được gọi
4. Harness kill `provider-c`
5. **Phase 2:** Client gửi 4 request — chỉ 2 provider còn lại phục vụ, không có lỗi

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
| Provider A | `examples/tcp-e2e-provider.ts` | `PROVIDER_ID=provider-a` |
| Provider B | `examples/tcp-e2e-provider.ts` | `PROVIDER_ID=provider-b` |
| Provider C | `examples/tcp-e2e-provider.ts` | `PROVIDER_ID=provider-c` |
| Client | `examples/tcp-failover-client.ts` | — |

---

## Test đang phát triển

### Discovery / Phát hiện node online-offline
**Harness:** `examples/tcp-discovery-test.ts`
**Observer:** `examples/tcp-discovery-observer.ts`

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

**Lưu ý kỹ thuật (TCP-specific):** Trong TCP, offline detection yêu cầu **có ít nhất một HTTP/2 connection đang mở** đến provider. Khi connection đóng (do provider chết), `Http2Rpc` mới phát ra sự kiện `offline`. Observer cần thực hiện các health check call định kỳ để duy trì kết nối này.

**Biến môi trường provider:**
- `SERVICES=a` → chỉ khởi tạo `ServiceA`
- `SERVICES=a,b` → khởi tạo cả `ServiceA` và `ServiceB`

---

## Tổng quan các biến môi trường

| Biến | Dùng bởi | Ý nghĩa |
|------|----------|---------|
| `REGISTRY_HOST` | tất cả TCP tests | Host của discovery registry |
| `REGISTRY_PORT` | tất cả TCP tests | Cổng của discovery registry |
| `PROVIDER_ID` | round-robin, failover | Định danh provider trong kết quả |
| `SERVICES` | discovery tests | Danh sách service cần khởi tạo (`a`, `a,b`) |

---

## Cơ chế E2E Harness

Tất cả harness sử dụng các helper trong `examples/helpers/e2eHarness.ts`:

- **`createTcpTestEnv()`** — Tạo environment variables với port ngẫu nhiên cho registry
- **`start(name, file, env)`** — Spawn tiến trình con `bun run <file>` với env
- **`waitForOutput(child, regex, timeoutMs, name)`** — Chờ stdout khớp regex, timeout → throw
- **`stopAll()`** — Gửi SIGTERM đến tất cả tiến trình con, đợi chúng thoát

**Lưu ý về listener gap:** `waitForOutput` chỉ lắng nghe output **từ lúc gọi đến khi regex khớp**. Nếu output được emit giữa hai lần gọi `waitForOutput`, output đó có thể bị mất. Các harness xử lý điều này bằng cách **in ra từng milestone ngay khi nhận được** thay vì chờ đến cuối.
