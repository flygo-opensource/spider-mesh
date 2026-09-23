# @spider-mesh/k8s

Discovery cho Spider Mesh trên Kubernetes: các pod tìm thấy nhau qua EndpointSlice của một headless
Service, không cần UDP hay broker. Dùng cùng `Topology` và `Http2Rpc` của `@spider-mesh/tcp`.

Hai việc tách riêng:

- **Membership**: pod nào đang ready. Lấy từ EndpointSlice, tức là từ chính `readinessProbe` của pod.
  Pod hết ready (crash, rolling update, node chết) thì node bị xoá khỏi `Topology` **ngay**, không đợi
  `removeUnreachableAfterMs`.
- **Thông tin node**: service, topic, cổng transporter. Mỗi pod phục vụ `SpiderMeshNode` của mình ở
  `GET /node` (cổng 7001) dưới dạng stream NDJSON; các pod khác kéo trực tiếp và nhận mọi thay đổi.

```bash
bun add @spider-mesh/core @spider-mesh/tcp @spider-mesh/k8s rxjs
```

```ts
// mesh.ts
import { SpiderMesh, Topology } from '@spider-mesh/core'
import { Http2Rpc } from '@spider-mesh/tcp'
import { KubernetesDiscovery } from '@spider-mesh/k8s'

const topology = new Topology({
  discovery: new KubernetesDiscovery({ service: 'spider-mesh' }),
})

export const mesh = new SpiderMesh({ topology, transporters: [new Http2Rpc({ port: 7000 })] })
export { topology }
```

Khai báo và gọi service không đổi, xem `@spider-mesh/core`.

Đo trên k3s v1.36 (3 provider, 1 client gọi mỗi giây):

| Tình huống | Watch EndpointSlice | Rơi về DNS |
| --- | --- | --- |
| Xoá một pod: client gỡ node sau | ~80ms | ~5s |
| Scale 1 → 3: node mới vào sau (tính cả lúc pod khởi động) | ~1.3s | chưa đo |
| Rolling restart: lời gọi lỗi | 0 | 0 |

> Bản 3.x viết lại hoàn toàn so với 2.x (`K8sRpcTransporter` + WebSocket discovery đã bỏ). RPC đi qua
> `Http2Rpc` của `@spider-mesh/tcp`; gói này chỉ là discovery.

**Không cần `SPIDERMESH_NODE_HOSTNAME`**: host của một node là địa chỉ mà pod khác đã kéo được `/node`
của nó, tức IP của pod.

## Manifest

Một headless Service chọn **mọi** pod của mesh (không phải mỗi service một cái), cùng quyền đọc
EndpointSlice:

```yaml
apiVersion: v1
kind: Service
metadata: { name: spider-mesh, namespace: apps }
spec:
  clusterIP: None
  selector: { spider-mesh/namespace: default }   # label chung của mọi pod trong mesh
  ports:
    - { name: discovery, port: 7001 }
---
apiVersion: v1
kind: ServiceAccount
metadata: { name: spider-mesh, namespace: apps }
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata: { name: spider-mesh-discovery, namespace: apps }
rules:
  - apiGroups: [discovery.k8s.io]
    resources: [endpointslices]
    verbs: [list, watch]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata: { name: spider-mesh-discovery, namespace: apps }
subjects: [{ kind: ServiceAccount, name: spider-mesh, namespace: apps }]
roleRef: { kind: Role, name: spider-mesh-discovery, apiGroup: rbac.authorization.k8s.io }
```

Trong mỗi Deployment dùng mesh:

```yaml
spec:
  template:
    metadata:
      labels: { spider-mesh/namespace: default }
    spec:
      serviceAccountName: spider-mesh
      terminationGracePeriodSeconds: 30
      containers:
        - name: app
          ports:
            - { name: discovery, containerPort: 7001 }
            - { name: rpc, containerPort: 7000 }
          readinessProbe:
            httpGet: { path: /health, port: 8080 }   # health của chính app
          lifecycle:
            # Pod bị gỡ khỏi EndpointSlice trước; chờ chút để lời gọi đang chạy kịp xong.
            preStop: { exec: { command: ['sleep', '5'] } }
```

Pod chỉ vào membership khi `readinessProbe` đạt, nên probe nên phản ánh app đã sẵn sàng phục vụ thật.
`GET /healthz` của discovery chỉ cho biết server `/node` đã có node local, không thay được health của app.

## Không có quyền watch: rơi về DNS

Mặc định (`mode: 'auto'`) discovery watch EndpointSlice. Nếu không làm được thì nó **in một cảnh báo**
rồi chuyển sang phân giải DNS của headless Service theo chu kỳ:

| Tình huống | Cảnh báo |
| --- | --- |
| ServiceAccount thiếu quyền (401/403) | `Cannot watch EndpointSlices of Service 'spider-mesh' (HTTP 403 Forbidden). Grant the pod's ServiceAccount list and watch on endpointslices.discovery.k8s.io …` |
| Không có EndpointSlice API (404) | `Cannot watch EndpointSlices … (HTTP 404 …)` |
| Không chạy trong pod (không có ServiceAccount mount vào) | `No Kubernetes service account in this process (not running in a pod?)` |

Mọi cảnh báo kết thúc bằng `Falling back to DNS: resolving spider-mesh.apps.svc.cluster.local every 5000ms …`.

Chạy bằng DNS vẫn đúng, nhưng:

- pod vào hoặc ra được nhận ra **chậm vài giây** (chu kỳ phân giải cộng với cache của CoreDNS);
- DNS lỗi tạm thời thì membership được giữ nguyên, không xoá node nào;
- sửa RBAC xong thì phải khởi động lại pod để quay về watch.

| `mode` | Hành vi |
| --- | --- |
| `auto` (mặc định) | Watch EndpointSlice; không được thì cảnh báo và dùng DNS |
| `api` | Chỉ watch; lỗi quyền thì cảnh báo và thử lại mỗi 30s, không dùng DNS |
| `dns` | Chỉ DNS, không cần quyền gì |

`onWarning` nhận cảnh báo thay cho `console.warn`, ví dụ để đẩy vào logger.

## Khi mất kết nối

| Chuyện gì | Discovery làm gì |
| --- | --- |
| Pod hết ready hoặc bị xoá | `removeRemote` ngay; `Http2Rpc` ngừng route và ngừng nối lại tới node đó |
| Stream `/node` đứt nhưng pod vẫn ready | Nối lại với thời gian chờ tăng dần (tối đa 10s), **không** xoá node |
| Stream `/node` im lặng quá 45s (không cả heartbeat 15s) | Coi là chết, nối lại |
| Watch bị API server đóng (định kỳ) | Watch tiếp từ `resourceVersion` cuối |
| Watch hết hạn (410) | List lại rồi watch tiếp |
| Mất API server | Giữ nguyên membership, thử lại (tối đa 30s); `verify()` trả `unknown` |
| Pod mới được cấp lại IP của pod cũ | Node cũ bị xoá, node mới được thêm |

`verify(node_id)` trả `alive` khi node thuộc một pod đang ready, còn lại `unknown`: pod rời membership
đã bị xoá ngay lúc rời, nên không có trường hợp nào cần trả `dead`.

## Tuỳ chọn

| Tuỳ chọn | Mặc định | Ý nghĩa |
| --- | --- | --- |
| `service` | *(bắt buộc)* | Tên headless Service |
| `namespace` | namespace của pod, rồi `POD_NAMESPACE`, rồi `default` | Namespace Kubernetes của Service |
| `port` | `SPIDERMESH_K8S_DISCOVERY_PORT` hoặc `7001` | Cổng của `GET /node`, giống nhau ở mọi pod |
| `host` | mọi interface | Địa chỉ lắng nghe của `GET /node` |
| `portName` | `discovery` | Tên cổng trong Service; cổng của từng EndpointSlice được đọc theo tên này |
| `mode` | `auto` | Xem trên |
| `clusterDomain` | `cluster.local` | Dùng để dựng tên DNS khi rơi về DNS |
| `dnsIntervalMs` | `SPIDERMESH_K8S_DNS_INTERVAL_MS` hoặc `5000` | Chu kỳ phân giải DNS |
| `onWarning` | `console.warn` | Nhận cảnh báo |
| `api` | từ ServiceAccount | `{ server, token?, ca? }`, để chạy ngoài pod hoặc trong test |

| Biến | Mặc định | Ý nghĩa |
| --- | --- | --- |
| `SPIDERMESH_K8S_DISCOVERY_PORT` | `7001` | Cổng `GET /node` |
| `SPIDERMESH_K8S_DNS_INTERVAL_MS` | `5000` | Chu kỳ phân giải DNS |
| `SPIDERMESH_K8S_NODE_IDLE_TIMEOUT_MS` | `45000` | Stream `/node` im lặng quá mức này thì nối lại |

## Dùng lại phần membership

`kubernetesApiMembership()` và `dnsMembership()` được export riêng: chúng phát tập `host:port` của các
pod đang ready (hoặc `null` khi tạm thời không biết), dùng được cho việc khác ngoài Spider Mesh.

```ts
import { inClusterConfig, kubernetesApiMembership } from '@spider-mesh/k8s'

const api = inClusterConfig()!
kubernetesApiMembership({ ...api, namespace: 'apps', service: 'tasks', portName: 'http', defaultPort: 8081 })
  .subscribe(targets => console.log(targets))
```

## Phát triển

```bash
bun install && bun run build && bun run test:e2e
```

Test e2e (`tests/mesh.e2e.test.ts`) chạy hai process Spider Mesh thật với `Http2Rpc`; nó import bản
build của `@spider-mesh/tcp`, nên cần build `core` và `tcp` trước.

Thử trên cluster thật: `tests/cluster/` có app demo và manifest, cách chạy trong [AGENTS.md](AGENTS.md).
