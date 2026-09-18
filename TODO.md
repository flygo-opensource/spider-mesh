# TODO

## Ohayo discovery protocol

- [x] Giữ `@spider-mesh/core` runtime-agnostic: không đưa HTTP, UDP, socket hoặc wire-level code vào package core.
- [x] Discovery transporter contract: `Observable<DiscoveryMessage<T>>` + `broadcast(message)`. Gói `@spider-mesh/discovery` đã bỏ; adapter nằm trong `@spider-mesh/tcp` (`TopologyDiscoveryAdapter`), `@spider-mesh/ws` giữ bản sao type của envelope.
- [x] Chuẩn hóa `DiscoveryMessage<T>` gồm `node_id`, `namespace`, `tags`, `version`, `created_at`, `seq`, `data`.
- [x] Chuẩn hóa discovery constructor options: `namespace` và `tags` bắt buộc, `node_id?` tùy chọn.
- [x] `TopologyDiscoveryAdapter` (trong `@spider-mesh/tcp`) map `SpiderMeshNode` vào `DiscoveryMessage<SpiderMeshNode>` qua `data`.
- [x] Dùng `tags` để phân biệt message của Spider Mesh, ví dụ `["spider-mesh", "node"]`, và để transport có thể lọc contains-all trước khi emit.
- [x] Tách UDP discovery khỏi `@spider-mesh/tcp`; dùng generic `@ohayo/udp` implement protocol ở `https://github.com/flygo-opensource/ohayo/blob/main/udp/README.md`.
- [ ] Thiết kế HTTP discovery companion transport theo `https://github.com/flygo-opensource/ohayo/blob/main/http/README.md` cho môi trường không dùng multicast.
- [x] Tất cả env của UDP discovery bắt đầu bằng `OHAYO_` và dùng `OHAYO_DISCOVERY_PORT`.
- [x] Xác định cách đồng bộ `seq` và `version` với `SpiderMeshNode.version` hiện tại để consumer bỏ qua message cũ/trùng.
- [x] Cập nhật README/ARCHITECTURE sau khi contract ổn định để nhấn mạnh core chỉ biết abstraction, transport cụ thể nằm ngoài core.

## Tests

- [x] Contract test cho discovery message envelope và constructor options.
- [x] Test namespace/tags filtering theo rule contains-all.
- [x] Test adapter trong `@spider-mesh/tcp` từ `SpiderMeshNode` sang `DiscoveryMessage<SpiderMeshNode>`.
- [x] Test Registry bỏ qua full snapshot có `version` cũ hơn.
- [x] Giữ Registry là helper cho transporter; `SpiderMesh` không sở hữu hoặc ingest Registry.
- [x] Test stop node → TCP emit trạng thái offline qua Registry → restart endpoint mới → rediscovery.
- [x] Test ba máy: Service A nhìn thấy đủ hai provider B và hai provider C chạy trên hai server.
