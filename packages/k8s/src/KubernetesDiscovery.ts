import { createServer, type ServerResponse } from 'node:http'
import { catchError, defer, EMPTY, retry, Subscription, timer, type Observable } from 'rxjs'
import type { SpiderMeshNode, TopologyDiscovery, TopologyDiscoveryContext, TopologyNodeVerificationResult } from '@spider-mesh/core'
import {
    SPIDERMESH_K8S_DISCOVERY_PORT,
    SPIDERMESH_K8S_DNS_INTERVAL_MS,
    SPIDERMESH_K8S_NODE_HEARTBEAT_MS,
    SPIDERMESH_K8S_NODE_IDLE_TIMEOUT_MS,
} from './const.js'
import {
    dnsMembership,
    formatTarget,
    inClusterConfig,
    KubernetesApiUnavailableError,
    kubernetesApiMembership,
    readServiceAccountNamespace,
    targetHost,
    type KubernetesApiConfig,
    type MembershipSnapshot,
    type Resolver,
} from './membership.js'
import { NodeStream } from './NodeStream.js'

/**
 * - `auto`: watch EndpointSlice; không có quyền hoặc không chạy trong pod thì cảnh báo và rơi về DNS.
 * - `api`: chỉ watch EndpointSlice; lỗi quyền thì cảnh báo và thử lại, không rơi về DNS.
 * - `dns`: chỉ phân giải headless Service theo chu kỳ, không cần quyền gì.
 */
export type KubernetesDiscoveryMode = 'auto' | 'api' | 'dns'

export type KubernetesDiscoveryOptions = {
    /** Headless Service chọn mọi pod của mesh. */
    service: string
    /** Mặc định: namespace của pod (ServiceAccount), rồi `POD_NAMESPACE`, rồi `default`. */
    namespace?: string
    /** Cổng của `GET /node`, giống nhau ở mọi pod. Mặc định `SPIDERMESH_K8S_DISCOVERY_PORT` hoặc 7001. */
    port?: number
    /** Địa chỉ lắng nghe của `/node`. Mặc định mọi interface. */
    host?: string
    /** Tên cổng trong Service; cổng của từng EndpointSlice được đọc theo tên này. Mặc định `discovery`. */
    portName?: string
    /** Mặc định `auto`. */
    mode?: KubernetesDiscoveryMode
    /** Mặc định `cluster.local`. */
    clusterDomain?: string
    /** Chu kỳ phân giải DNS. Mặc định `SPIDERMESH_K8S_DNS_INTERVAL_MS` hoặc 5000. */
    dnsIntervalMs?: number
    /** Nhận cảnh báo (rơi về DNS, không mở được cổng...). Mặc định `console.warn`. */
    onWarning?: (message: string) => void
    /** Thay cấu hình API server lấy từ ServiceAccount; dùng khi chạy ngoài pod hoặc trong test. */
    api?: KubernetesApiConfig
    /** Thay bộ phân giải DNS; dùng trong test. */
    resolve?: Resolver
}

const PREFIX = '[spider-mesh/k8s]'

/**
 * Discovery cho Kubernetes. Membership (pod nào đang ready) lấy từ EndpointSlice của một headless
 * Service; thông tin node (service, topic, cổng transporter) thì mỗi pod tự phục vụ ở `GET /node` và các
 * pod khác kéo trực tiếp. Không cần UDP hay broker.
 *
 * Pod rời membership thì node bị xoá khỏi Topology ngay, không đợi `removeUnreachableAfterMs`. Host của
 * node là địa chỉ đã kéo được `/node`, nên không cần đặt `SPIDERMESH_NODE_HOSTNAME`.
 */
export class KubernetesDiscovery implements TopologyDiscovery {

    readonly namespace: string
    readonly port: number

    #options: KubernetesDiscoveryOptions
    #binding = Subscription.EMPTY
    #peers = new Map<string, NodeStream>()
    #synced = false
    #activeMode?: 'api' | 'dns'
    #warnedNamespace = new Set<string>()

    constructor(options: KubernetesDiscoveryOptions) {
        if (!options.service) throw new Error(`${PREFIX} options.service is required`)
        this.#options = options
        this.namespace = options.namespace
            ?? readServiceAccountNamespace()
            ?? process.env.POD_NAMESPACE
            ?? 'default'
        this.port = options.port ?? SPIDERMESH_K8S_DISCOVERY_PORT
    }

    /** Nguồn membership đang dùng; `undefined` trước khi bind. */
    get mode() {
        return this.#activeMode
    }

    /** Tên DNS của headless Service, dùng khi rơi về DNS. */
    get hostname() {
        return `${this.#options.service}.${this.namespace}.svc.${this.#options.clusterDomain ?? 'cluster.local'}`
    }

    bind(context: TopologyDiscoveryContext) {
        this.#binding.unsubscribe()
        const binding = new Subscription()
        let local: SpiderMeshNode | undefined
        const listeners = new Set<ServerResponse>()
        const write = (response: ServerResponse, node: SpiderMeshNode) => response.write(`${JSON.stringify(node)}\n`)

        binding.add(context.localNode$.subscribe(node => {
            local = node
            for (const response of listeners) write(response, node)
        }))

        const server = createServer((request, response) => {
            const path = (request.url ?? '').split('?')[0]
            if (request.method === 'GET' && path === '/node') {
                response.writeHead(200, { 'content-type': 'application/x-ndjson', 'cache-control': 'no-store' })
                if (local) write(response, local)
                listeners.add(response)
                response.once('close', () => listeners.delete(response))
                return
            }
            if (request.method === 'GET' && path === '/healthz') {
                response.writeHead(local ? 200 : 503).end()
                return
            }
            response.writeHead(404).end()
        })
        server.on('error', error => {
            this.#warn(`Cannot listen on port ${this.port} (${error.message}): other pods will not learn this node.`)
        })
        server.listen(this.port, this.#options.host)
        // Dòng trống: phía kéo biết stream còn sống dù node không đổi.
        const heartbeat = setInterval(() => {
            for (const response of listeners) response.write('\n')
        }, SPIDERMESH_K8S_NODE_HEARTBEAT_MS)
        heartbeat.unref?.()
        binding.add(() => {
            clearInterval(heartbeat)
            for (const response of listeners) response.destroy()
            listeners.clear()
            server.close()
            server.closeAllConnections?.()
        })

        binding.add(this.#membership$().subscribe(snapshot => this.#apply(snapshot, context, () => local)))
        binding.add(() => {
            for (const peer of this.#peers.values()) peer.close()
            this.#peers.clear()
            this.#synced = false
        })

        this.#binding = binding
        return binding
    }

    /**
     * Node thuộc một pod đang ready thì `alive`. Còn lại là `unknown`: pod rời membership đã bị xoá ngay
     * khi rời, nên node nào còn trong Topology mà không thuộc pod nào là do discovery khác đưa vào.
     */
    async verify(node_id: string): Promise<TopologyNodeVerificationResult> {
        if (!this.#synced) return 'unknown'
        for (const peer of this.#peers.values()) {
            if (peer.node_id === node_id) return 'alive'
        }
        return 'unknown'
    }

    close() {
        this.#binding.unsubscribe()
        this.#binding = Subscription.EMPTY
    }

    #membership$(): Observable<MembershipSnapshot> {
        const mode = this.#options.mode ?? 'auto'
        const dns$ = defer(() => {
            this.#activeMode = 'dns'
            return dnsMembership({
                hostname: this.hostname,
                port: this.port,
                intervalMs: this.#options.dnsIntervalMs ?? SPIDERMESH_K8S_DNS_INTERVAL_MS,
                resolve: this.#options.resolve,
            })
        })
        if (mode === 'dns') return dns$

        const api = this.#options.api ?? inClusterConfig()
        if (!api) {
            if (mode === 'api') {
                this.#warn(`No Kubernetes service account in this process and no options.api: mode 'api' cannot discover any node.`)
                return EMPTY
            }
            this.#warn(`No Kubernetes service account in this process (not running in a pod?). ${this.#dnsFallbackNote()}`)
            return dns$
        }

        const api$ = defer(() => {
            this.#activeMode = 'api'
            return kubernetesApiMembership({
                ...api,
                namespace: this.namespace,
                service: this.#options.service,
                portName: this.#options.portName ?? 'discovery',
                defaultPort: this.port,
            })
        })

        if (mode === 'api') {
            return api$.pipe(retry({
                delay: error => {
                    this.#warn(`${this.#apiFailure(error)} Retrying in 30s; mode 'api' does not fall back to DNS.`)
                    return timer(30_000)
                },
            }))
        }

        return api$.pipe(catchError(error => {
            if (!(error instanceof KubernetesApiUnavailableError)) throw error
            this.#warn(`${this.#apiFailure(error)} ${this.#dnsFallbackNote()}`)
            return dns$
        }))
    }

    #apiFailure(error: unknown) {
        const status = error instanceof KubernetesApiUnavailableError ? error.status : undefined
        const reason = error instanceof Error ? error.message : String(error)
        const fix = status === 401 || status === 403
            ? ` Grant the pod's ServiceAccount list and watch on endpointslices.discovery.k8s.io in namespace `
                + `'${this.namespace}' (see the RBAC manifest in the @spider-mesh/k8s README).`
            : ''
        return `Cannot watch EndpointSlices of Service '${this.#options.service}' (${reason}).${fix}`
    }

    #dnsFallbackNote() {
        const interval = this.#options.dnsIntervalMs ?? SPIDERMESH_K8S_DNS_INTERVAL_MS
        return `Falling back to DNS: resolving ${this.hostname} every ${interval}ms, so pods joining or leaving `
            + `are noticed several seconds late.`
    }

    #apply(snapshot: MembershipSnapshot, context: TopologyDiscoveryContext, getLocal: () => SpiderMeshNode | undefined) {
        // Không biết thì giữ nguyên: mất API server hay DNS lỗi không có nghĩa là mọi pod đã chết.
        if (snapshot === null) {
            this.#synced = false
            return
        }
        this.#synced = true

        const local = getLocal()
        const selfTarget = local?.host ? formatTarget(local.host, this.port) : undefined
        for (const target of snapshot) {
            if (target === selfTarget || this.#peers.has(target)) continue
            const peer: NodeStream = new NodeStream(target, {
                onNode: node => this.#onNode(peer, node, context, getLocal()),
            }, SPIDERMESH_K8S_NODE_IDLE_TIMEOUT_MS)
            this.#peers.set(target, peer)
        }

        for (const [target, peer] of this.#peers) {
            if (snapshot.has(target)) continue
            peer.close()
            this.#peers.delete(target)
            if (peer.node_id) context.removeRemote(peer.node_id)
        }
    }

    #onNode(peer: NodeStream, node: SpiderMeshNode, context: TopologyDiscoveryContext, local: SpiderMeshNode | undefined) {
        if (local && node.node_id === local.node_id) {
            // Chính mình (host chưa đặt nên không lọc được theo địa chỉ): ngừng kéo, giữ entry để không mở lại.
            peer.self = true
            peer.close()
            return
        }
        if (local && node.namespace !== local.namespace) {
            if (!this.#warnedNamespace.has(peer.target)) {
                this.#warnedNamespace.add(peer.target)
                this.#warn(`Ignoring ${peer.target}: its mesh namespace '${node.namespace}' differs from '${local.namespace}'.`)
            }
            return
        }
        // Cùng IP nhưng node khác: pod cũ đã đi, pod mới được cấp lại IP đó.
        if (peer.node_id && peer.node_id !== node.node_id) context.removeRemote(peer.node_id)
        peer.node_id = node.node_id
        context.upsertRemote({ ...node, host: targetHost(peer.target) })
    }

    #warn(message: string) {
        const onWarning = this.#options.onWarning ?? (text => console.warn(`${PREFIX} ${text}`))
        onWarning(message)
    }
}
