/** Cổng mà mỗi pod mở để các pod khác kéo `SpiderMeshNode` của nó (`GET /node`). */
export const SPIDERMESH_K8S_DISCOVERY_PORT = Number(process.env.SPIDERMESH_K8S_DISCOVERY_PORT || 7001)
/** Chu kỳ phân giải DNS khi phải dùng DNS thay cho watch EndpointSlice. */
export const SPIDERMESH_K8S_DNS_INTERVAL_MS = Number(process.env.SPIDERMESH_K8S_DNS_INTERVAL_MS || 5000)
/** Không nhận byte nào (kể cả heartbeat) quá lâu thì coi stream `/node` đã chết và nối lại. */
export const SPIDERMESH_K8S_NODE_IDLE_TIMEOUT_MS = Number(process.env.SPIDERMESH_K8S_NODE_IDLE_TIMEOUT_MS || 45_000)
/** Server `/node` gửi một dòng trống theo chu kỳ này để phía kéo phân biệt được stream rảnh với stream chết. */
export const SPIDERMESH_K8S_NODE_HEARTBEAT_MS = 15_000

/** Thư mục ServiceAccount mà kubelet mount vào mọi pod. */
export const SERVICE_ACCOUNT_DIR = '/var/run/secrets/kubernetes.io/serviceaccount'
