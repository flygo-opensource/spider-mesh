# @spider-mesh/tcp — Test Coverage / Danh sách bài kiểm thử

All tests are end-to-end (E2E) — each test spawns real child processes using actual TCP/UDP sockets.

Tất cả các bài kiểm thử đều là end-to-end (E2E) — mỗi bài chạy các tiến trình thực sự với TCP/UDP sockets thực.

---

## `tcp-transporters.e2e.test.ts`

| Test | Input / Đầu vào | Output / Đầu ra |
|------|-----------------|-----------------|
| **tcp transporters smoke e2e** | Provider + Client cùng namespace, gọi RPC và publish pubsub event | RPC response đúng; event được nhận bởi provider |

## `tcp-contracts.e2e.test.ts`

| Test | Input / Đầu vào | Output / Đầu ra |
|------|-----------------|-----------------|
| **tcp rpc transporter contract e2e** | Gửi RPC request packet trực tiếp qua `Http2Rpc` | `RpcEvent.rpc` là raw packet, `packet.kind = "request"`, có `sender_node_id` |
| **tcp discovery transporter contract e2e** | `UdpDiscovery` sender broadcast node metadata | Receiver phát hiện đúng node, `discovered.node_id` đúng, không có raw wrapper |

## `tcp-spidermesh.e2e.test.ts`

| Test | Input / Đầu vào | Output / Đầu ra |
|------|-----------------|-----------------|
| **tcp spidermesh rpc e2e** | Client chờ provider qua UdpDiscovery, gọi `GreetingService.hello()` | `"hello tcp e2e from provider"` |

## `tcp-spidermesh-reverse.e2e.test.ts`

| Test | Input / Đầu vào | Output / Đầu ra |
|------|-----------------|-----------------|
| **tcp spidermesh reverse rpc e2e** | Server expose service, Client gọi ngược lại từ phía server | `"hello from server from client"` |

## `tcp-spidermesh-round-robin.e2e.test.ts`

| Test | Input / Đầu vào | Output / Đầu ra |
|------|-----------------|-----------------|
| **tcp spidermesh round-robin e2e** | 1 Client, 2 Providers (`provider-a`, `provider-b`), 4 RPC calls | 4 kết quả; cả 2 providers đều được gọi (round-robin) |

## `tcp-spidermesh-matrix.e2e.test.ts`

| Test | Input / Đầu vào | Output / Đầu ra |
|------|-----------------|-----------------|
| **tcp spidermesh rpc matrix e2e** | Gọi sync value, async value, sync Observable, async Observable, sync error, async error, observable error | 4 return types đúng giá trị; 3 error messages đúng |

## `tcp-missing.e2e.test.ts`

| Test | Input / Đầu vào | Output / Đầu ra |
|------|-----------------|-----------------|
| **tcp rpc timeout e2e** | Client gọi method không bao giờ resolve với `timeout: 2000ms` | `code: "MICROSERVICE_RPC_TIMEOUT"` sau ~2s |
| **tcp rpc fallback e2e** | Không có provider; client gọi với `fallback: "fallback-response"`, `retry: 0` | Nhận `"fallback-response"` thay vì throw |
| **tcp provider offline detection e2e** | Provider chạy → client gọi thành công → provider bị SIGTERM → client gọi lại | Lần 1: success; lần 2: `code: "MICROSERVICE_OFFLINE"` |
| **tcp concurrent rpc e2e** | 10 RPC calls song song đến cùng provider | 10 responses đúng, `allCorrect: true` |
| **tcp failover: 3 nodes then 1 offline e2e** | 3 providers cùng online; 6 requests (phase 1); kill `provider-c`; 4 requests (phase 2) | Phase 1: cả 3 provider nhận request; Phase 2: chỉ `provider-a` và `provider-b`, không lỗi |

---

## Coverage Summary / Tóm tắt độ phủ

| Scenario | Covered |
|----------|---------|
| Basic RPC | ✅ |
| Reverse RPC | ✅ |
| Round-robin load balancing | ✅ |
| Sync / async / Observable return types | ✅ |
| Error propagation | ✅ |
| PubSub events | ✅ |
| RPC transporter contract (`send()` packet structure) | ✅ |
| Discovery transporter contract | ✅ |
| RPC timeout | ✅ |
| Fallback value | ✅ |
| Provider crash / offline detection | ✅ |
| Concurrent RPC | ✅ |
| Failover (kill 1 of 3 nodes) | ✅ |
| Cancel stream on unsubscribe | ✅ (via `send()` returning `{ cancel }`) |
