# Changelog

## 3.0.0 — Kubernetes discovery

- `KubernetesDiscovery`: membership từ EndpointSlice của một headless Service (list + watch, tự nối
  lại, list lại khi 410), thông tin node kéo trực tiếp từ `GET /node` của từng pod.
- Pod hết ready thì node rời `Topology` ngay; host của node là IP của pod, không cần
  `SPIDERMESH_NODE_HOSTNAME`.
- Không có quyền watch EndpointSlice, không có API, hoặc không chạy trong pod: cảnh báo rồi rơi về phân
  giải DNS của headless Service (`mode: 'auto'`). `mode: 'api'` và `mode: 'dns'` để chọn cố định.
- Export `kubernetesApiMembership()`, `dnsMembership()` để dùng lại phần membership.
- Đã thử trên k3s v1.36 (1 node, 3 provider + 1 client): pod bị xoá rời Topology sau khoảng 80ms,
  rolling restart không lỗi lời gọi nào; ở chế độ DNS pod rời sau khoảng 5s.

### Breaking so với 2.x

- Viết lại từ đầu, không tương thích với `@spider-mesh/k8s` 2.x: bỏ `K8sRpcTransporter` và WebSocket
  discovery server/client. RPC giờ đi qua `Http2Rpc` của `@spider-mesh/tcp`; gói này chỉ còn là
  discovery cắm vào `Topology`.
- Cần `@spider-mesh/core` 3.x; mọi node trong mesh phải cùng dòng 3.x.
