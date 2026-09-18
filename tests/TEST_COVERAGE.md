# `@spider-mesh/tcp` test coverage

Chi tiết lệnh chạy, từng file và cross-host harness nằm tại [TESTS.md](TESTS.md).

| Nhóm | Độ phủ | Trạng thái gần nhất |
| --- | --- | --- |
| Build TypeScript | TCP và contract re-export từ core | Pass |
| Standard E2E | 16 test: RPC, pubsub, discovery contract, wait, matrix, round-robin, timeout, fallback, concurrent, failover | 16/16 pass |
| Resilience | 6 tests: replace snapshot, bounded retry stop, soak restart, SIGKILL recovery, duplicate ID, interrupted stream | 6/6 pass |
| Ohayo UDP package | 9 test: generic contract, explicit peers, one-shot reply, HMAC/expiry, filtering, malformed packet, duplicates, outbound validation, lifecycle | 9/9 pass |
| Remote 3-host topology | A thấy đủ B/C trên hai server; direct RPC và round-robin đủ 4 provider | Pass |
| Remote lifecycle | Online → SIGKILL/offline → restart port mới/rediscovered | Pass trên cả hai remote server |

Kết quả local mới nhất dùng optional Core Topology: Discovery giữ membership, còn HTTP/2 giữ
reachability và không xóa node khỏi Topology. UDP chỉ announcement/one-shot convergence.
