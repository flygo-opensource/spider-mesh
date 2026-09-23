# Kiến trúc `@spider-mesh/k8s`

## Hai việc, hai nguồn

```text
                ┌──────────── pod A ─────────────────────────────────────┐
API server ───▶ │ membership$  (EndpointSlice watch, hoặc DNS)           │
 (EndpointSlice)│    tập host:port của pod ready                         │
                │       │ mới  → NodeStream(target) ── GET /node ──▶ pod B
                │       │ mất  → close + removeRemote(node_id)           │
                │ server :7001  GET /node ◀── NodeStream của pod khác    │
                │    stream NDJSON của localNode$                        │
                │ Http2Rpc :7000  (không biết gì về discovery)           │
                └────────────────────────────────────────────────────────┘
```

- **Membership** (pod nào ready) đến từ Kubernetes. Đây là nguồn duy nhất quyết định xoá node.
- **Thông tin node** (`SpiderMeshNode`: service, topic, cổng transporter) đến từ chính pod đó qua
  `GET /node`. Kubernetes không biết những thứ này, nên phải kéo trực tiếp.

Discovery implement `TopologyDiscovery` của core và chỉ dùng `TopologyDiscoveryContext`
(`localNode$`, `upsertRemote`, `removeRemote`). `Http2Rpc` nhận node mới qua `topology.nodes$` như với
mọi discovery khác; package này không import, không sửa gì trong `@spider-mesh/tcp`.

## Module

| File | Trách nhiệm |
| --- | --- |
| `src/KubernetesDiscovery.ts` | Server `/node`, chọn nguồn membership (`auto`/`api`/`dns`), áp snapshot lên các `NodeStream`, `verify()` |
| `src/membership.ts` | `kubernetesApiMembership()` (list + watch EndpointSlice), `dnsMembership()` (poll headless Service), cấu hình in-cluster |
| `src/NodeStream.ts` | Kéo `GET /node` của một pod, giữ stream, nối lại khi đứt |
| `src/const.ts` | Biến môi trường |

Runtime chỉ import **type** từ `@spider-mesh/core`: package không phụ thuộc bản core cụ thể lúc chạy.

## Membership

Cả hai nguồn phát `ReadonlySet<string> | null`:

- tập `host:port` của pod ready (IPv6 trong ngoặc vuông);
- `null` khi tạm thời không biết (mất API server, DNS lỗi). Bên nhận **giữ nguyên** membership và đặt
  `synced = false`; không bao giờ suy ra "không còn pod nào" từ một lỗi.

### EndpointSlice (`kubernetesApiMembership`)

1. `list` EndpointSlice theo label `kubernetes.io/service-name=<service>`, phát snapshot.
2. `watch` từ `resourceVersion` của list, có bookmark, `timeoutSeconds=290`. Mỗi event cập nhật map
   `slice name → targets` rồi phát lại toàn bộ tập.
3. Watch đóng bình thường → watch tiếp từ `resourceVersion` cuối. Đóng ngay liên tục → nghỉ 1s.
4. 410 (HTTP status hoặc event `ERROR`) → list lại.
5. Lỗi mạng → phát `null`, thử lại với backoff 0.5s → 30s.
6. 401/403/404 → lỗi `KubernetesApiUnavailableError`: lỗi cấu hình, thử lại không giúp được, nên đẩy
   lên cho `KubernetesDiscovery` quyết định.

Endpoint được coi là ready khi `conditions.ready !== false` (rỗng = ready, theo quy ước Kubernetes).
Cổng lấy theo tên (`portName`, mặc định `discovery`) trong `ports` của từng slice, không có thì dùng
`port` của discovery.

Token ServiceAccount được **đọc lại mỗi request**: kubelet xoay vòng token projected định kỳ, giữ bản
cũ trong bộ nhớ sẽ bị 401 sau khoảng một giờ.

Dùng `node:http`/`node:https` thay cho `fetch` vì `fetch` của Node không nhận CA riêng.

### DNS (`dnsMembership`)

Phân giải A và AAAA của `<service>.<namespace>.svc.<clusterDomain>` theo chu kỳ. Dùng
`resolve4`/`resolve6`, **không** dùng `dns.lookup`: lookup chỉ trả một địa chỉ, còn headless Service trả
một bản ghi cho mỗi pod. `ENOTFOUND`/`ENODATA` nghĩa là chưa có pod ready (tập rỗng); lỗi khác phát `null`.

## Chọn nguồn và rơi về DNS

| `mode` | Không có ServiceAccount | API từ chối (401/403/404) |
| --- | --- | --- |
| `auto` | cảnh báo, dùng DNS | cảnh báo, dùng DNS |
| `api` | cảnh báo, không discovery gì | cảnh báo, thử lại mỗi 30s |
| `dns` | dùng DNS | không gọi API |

Rơi về DNS là một chiều trong vòng đời của `bind()`: sửa RBAC xong phải khởi động lại pod.

## Áp snapshot (`#apply`)

- Target mới (không phải chính mình) → tạo `NodeStream`.
- Target biến mất → `close()` stream, `removeRemote(node_id)` nếu đã học được node id.
- `null` → chỉ đặt `synced = false`.

Lọc chính mình theo `localNode.host:port`. `SPIDERMESH_NODE_HOSTNAME` thường không đặt trong
Kubernetes nên host rỗng; khi đó stream tới chính mình trả về node id của mình, `#onNode` nhận ra,
đánh dấu `self` và đóng stream nhưng giữ entry để không mở lại.

## `NodeStream`

- `GET http://<target>/node`, đọc NDJSON; dòng rỗng là heartbeat (server gửi mỗi 15s).
- Không nhận byte nào trong `SPIDERMESH_K8S_NODE_IDLE_TIMEOUT_MS` (45s) → huỷ, nối lại.
- Kết nối không lên trong 3s → huỷ, nối lại.
- Đứt → nối lại với backoff 250ms → 10s; reset khi nhận được snapshot. **Không bao giờ tự xoá node**:
  pod còn trong membership nghĩa là Kubernetes vẫn coi nó ready.

## `#onNode`

| Snapshot | Xử lý |
| --- | --- |
| node id = local | `self`, đóng stream |
| namespace mesh khác local | cảnh báo một lần cho target đó, bỏ qua |
| node id khác node id đã học của target | pod mới nhận lại IP của pod cũ: `removeRemote(cũ)` |
| còn lại | `upsertRemote({ ...node, host: <host của target> })` |

Host luôn bị ghi đè bằng địa chỉ đã kéo được `/node`: đó là địa chỉ chắc chắn tới được, và nhờ vậy
không cần `SPIDERMESH_NODE_HOSTNAME`. Snapshot cũ hơn bị Topology bỏ qua (so `version`), nên stream nối
lại không ghi đè dữ liệu mới.

Trường `nodes` của `SpiderMeshNode` được chuyển nguyên: không package nào trong repo ghi dữ liệu vào đó.

## `verify()`

`alive` khi node id thuộc một target đang có trong membership; còn lại `unknown`, kể cả khi chưa
`synced`. Không trả `dead`: pod rời membership đã bị `removeRemote` ngay lúc rời, nên node còn trong
Topology mà không thuộc target nào là do discovery khác đưa vào, và discovery này không có thẩm quyền.

## Phân chia trách nhiệm

| Việc | Chủ sở hữu |
| --- | --- |
| Pod ready hay không | Kubernetes (readinessProbe → EndpointSlice) |
| Node vào/ra Topology | `KubernetesDiscovery` |
| Thông tin node | Chính node, qua `GET /node` |
| Kết nối RPC, reachability | `Http2Rpc` |
| Chọn node cho lời gọi | Topology |
