# Tài liệu kiểm thử `@spider-mesh/tcp`

Các suite local dùng Bun và giao tiếp qua HTTP/2 socket thật. Những test cần discovery dùng generic
`@simple-discovery/udp`; package TCP không còn chứa `UdpDiscovery`.

## Kiến trúc được kiểm tra

```text
@simple-discovery/udp ──► Topology ◄── SpiderMesh localNode$
                   │
          ┌────────┴────────┐
          ▼                 ▼
       Http2Rpc        Http2Pubsub
          │
          └── reachability / bounded reconnect
```

- `TopologyDiscoveryAdapter` replace toàn bộ remote snapshot mới hơn theo `version`.
- Ohayo UDP trả lời one-shot khi thấy announcement mới; không heartbeat định kỳ.
- HTTP/2 session hoặc TCP socket `close`/`error` kích hoạt bounded retry; quá ngưỡng endpoint bị
  đánh dấu unreachable nhưng membership vẫn thuộc Topology.
- Snapshot có host/port mới mở lại connection cycle.

## Cách chạy

```bash
cd /path/to/spider-mesh/tcp
bun run build
bun run test:e2e
bun run test:resilience
```

Chạy riêng một file:

```bash
bun test tests/tcp-session-recovery.e2e.test.ts
```

Không có script `bun test` tổng hợp trong `package.json`; hai lệnh chuẩn là `test:e2e` và
`test:resilience`.

## Suite E2E chuẩn — 16 test

| File | Số test | Nội dung |
| --- | ---: | --- |
| `tcp-transporters.e2e.test.ts` | 1 | Smoke RPC và pubsub thật. |
| `tcp-contracts.e2e.test.ts` | 2 | RPC packet contract và generic Ohayo discovery envelope. |
| `tcp-session-recovery.e2e.test.ts` | 1 | Topology TTL loại snapshot cũ → restart endpoint mới → rediscovery. |
| `tcp-spidermesh.e2e.test.ts` | 1 | RPC qua `SpiderMesh` giữa các process. |
| `tcp-spidermesh-reverse.e2e.test.ts` | 1 | Provider/client gọi RPC theo chiều ngược lại. |
| `tcp-spidermesh-matrix.e2e.test.ts` | 1 | Sync, async, Observable và các đường lỗi. |
| `tcp-spidermesh-round-robin.e2e.test.ts` | 1 | Hai provider cùng service đều nhận request. |
| `tcp-getting-started.e2e.test.ts` | 1 | Ví dụ Getting Started chạy được giữa process thật. |
| `tcp-registry-free-wait.e2e.test.ts` | 1 | Client khởi động trước provider: `wait()` block rồi thành công. |
| `tcp-missing.e2e.test.ts` | 5 | Timeout, fallback, offline, concurrent RPC và failover 3→2 node. |
| `tcp-multi-provider-noretry.e2e.test.ts` | 1 | Không route vào peer đang ở trạng thái metadata nửa hoàn chỉnh. |

## Suite resilience — 6 test

Chạy bằng:

```bash
bun run test:resilience
```

| Kịch bản | Điều kiện pass |
| --- | --- |
| Full metadata replacement | Service và endpoint cũ biến mất; snapshot không bị merge thành peer giả. |
| Restart soak | Số peer ổn định qua nhiều lần provider restart. |
| Bounded reconnect | Giữ Topology membership, dừng retry target cũ và nối target mới khi Discovery cập nhật. |
| SIGKILL lifecycle | Endpoint thành unreachable, membership được giữ, sau đó endpoint mới được rediscover. |
| Duplicate test ID | Hai process không bị merge thành một synthetic peer. |
| Interrupted stream | Nhận phần dữ liệu đã phát và đúng một terminal `MICROSERVICE_OFFLINE`. |

UDP packet signing, namespace/tag filtering, anti-replay, duplicate delivery, socket relay và
`close()` được kiểm tra trong package `@simple-discovery/udp`, không lặp lại trong TCP resilience.

## Cross-host tests

Hai harness dưới đây dùng package tarball và Bun trong `/tmp`:

- [`remote-multiservice/README.md`](remote-multiservice/README.md): A trên máy thứ ba phải thấy đủ
  B và C trên hai server, gọi trực tiếp đủ bốn provider và round-robin tới cả hai bản sao.
- [`remote-lifecycle/README.md`](remote-lifecycle/README.md): stop/SIGKILL provider phải offline;
  restart cùng test label nhưng port mới phải online lần hai.

Đã xác minh trên 3 host `192.168.1.10`, `192.168.1.20`, `192.168.1.30` ngày 2026-07-12 bằng Bun.

## Cấu hình test quan trọng

| Biến | Vai trò |
| --- | --- |
| `SPIDERMESH_NAMESPACE` | Cô lập mesh/test run. |
| `SIMPLE_DISCOVERY_KEY` | HMAC key dùng chung. |
| `SIMPLE_DISCOVERY_PORT` | UDP discovery port. |
| `SIMPLE_DISCOVERY_UDP_WHITELIST_ADDRESS` | Danh sách explicit peers khi multicast không dùng được. |
| `SPIDERMESH_HTTP2_RECONNECT_ATTEMPTS` | Số lỗi liên tiếp trước khi đánh dấu endpoint unreachable. |
| `SPIDERMESH_HTTP2_RECONNECT_DELAY_MS` | Base reconnect backoff. |
| `SPIDERMESH_HTTP2_CONNECT_TIMEOUT_MS` | Timeout tối đa cho mỗi lần mở HTTP/2 connection. |

`SPIDERMESH_NODE_ID` xuất hiện trong một số harness cấp thấp chỉ để tạo nhãn deterministic cho node
snapshot được dựng thủ công. Runtime `SpiderMesh` không đọc biến này và luôn sinh ID ngẫu nhiên.
