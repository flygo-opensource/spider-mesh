# Kiến trúc `@spider-mesh/tcp`

## `Http2Rpc`

```text
SpiderMesh request
   -> Http2Rpc
      -> Topology route -> node.host + transporters.http2.port
      hoặc
      -> resolveService -> infrastructure endpoint
```

Transporter sở hữu HTTP/2 server, client session, packet framing, response stream và reconnect.
Nó không sở hữu Discovery và không xóa node khỏi Topology.

State endpoint gồm:

- `connections`: session đang hoạt động theo node ID.
- `connecting`: promise dùng chung để tránh mở trùng session.
- `exhaustedTargets`: chặn vòng reconnect vô hạn cho tới khi có tín hiệu retry mới.
- `responseStreams`: server stream để gửi response về đúng request.

Reachability không còn là state private dùng để route. `Http2Rpc` báo trạng thái vào Topology để
mọi consumer (`route`, `watch`, diagnostics) dùng cùng một nguồn state; membership vẫn thuộc
Discovery.

## Phân chia trách nhiệm

| Việc | Chủ sở hữu |
| --- | --- |
| Node tồn tại/offline | Discovery -> Topology |
| Chọn node theo strategy | Topology, khi request yêu cầu |
| Kết nối node cụ thể | `Http2Rpc` |
| Endpoint suspect/unreachable/recovered | `Http2Rpc` -> Topology |
| Xác minh lại membership | Topology -> Discovery |
| Route qua DNS/load balancer | `resolveService` + hạ tầng |
| Retry/timeout RPC | SpiderMesh Core |
| Reconnect HTTP/2 session | `Http2Rpc` |

## `Http2Pubsub`

Đây là event transporter riêng có name `http2-pubsub`. Nó đọc topic membership từ Topology và
không tham gia RPC registration. Ứng dụng không dùng event thì không cần import class này.

## Wire identity

- RPC metadata: `node.transporters.http2`.
- Event metadata: `node.transporters['http2-pubsub']`.
- Không có constructor-name fallback.
