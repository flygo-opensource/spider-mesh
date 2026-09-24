import { Subscription, type Observable } from 'rxjs'
import type { SpiderMeshNode, TopologyDiscovery, TopologyDiscoveryContext } from '@spider-mesh/core'

/**
 * Envelope mà discovery trao đổi, ví dụ `DiscoveryMessage<T>` của `@simple-discovery/udp`. Hai phía khớp nhau
 * theo cấu trúc, nên `tcp` không phụ thuộc gói discovery cụ thể nào.
 */
export type DiscoveryMessage<T> = {
    node_id: string
    namespace: string
    tags: string[]
    version: string
    created_at: number
    seq: number
    data: T
    remote_host?: string
}

/** Kết quả xác minh một node còn sống hay không. */
export type DiscoveryNodeVerificationResult = 'alive' | 'dead' | 'unknown'

/** Discovery mà adapter nhận: luồng message nhận được, cộng `broadcast()`. */
export type DiscoveryTransporter<T> = Observable<DiscoveryMessage<T>> & {
    broadcast(message: DiscoveryMessage<T>): Promise<void>
    verify?(node_id: string): Promise<DiscoveryNodeVerificationResult>
    close?: () => void
}

/** Tuỳ chọn của `TopologyDiscoveryAdapter`. */
export type TopologyDiscoveryAdapterOptions = {
    /** Tag gắn vào message gửi đi. Tag của discovery phải nằm trong danh sách này. */
    tags?: string[]
    /** Nhận lỗi khi broadcast; không đặt thì lỗi bị bỏ qua. */
    onError?: (error: unknown) => void
    /** Đóng discovery bên dưới khi Topology đóng. Mặc định `true`. */
    closeTransporter?: boolean
    /** Phát lại snapshot local định kỳ để Topology TTL không xoá nhầm node còn sống. */
    heartbeatIntervalMs?: number
}

/** Tag mặc định để các node Spider Mesh nhận ra announcement của nhau. */
export const SPIDER_MESH_DISCOVERY_TAGS = ['spider-mesh', 'node'] as const

const toDiscoveryMessage = (node: SpiderMeshNode, tags: readonly string[]): DiscoveryMessage<SpiderMeshNode> => ({
    node_id: node.node_id,
    namespace: node.namespace,
    tags: [...tags],
    version: String(node.version),
    created_at: Date.now(),
    seq: node.version,
    data: node,
})

/**
 * Node không đặt `SPIDERMESH_NODE_HOSTNAME` công bố `host` rỗng; khi đó dùng địa chỉ nguồn của gói
 * discovery (`remote_host`) để các node khác vẫn kết nối tới được.
 */
const withRemoteHost = (message: DiscoveryMessage<SpiderMeshNode>): SpiderMeshNode => {
    const node = message.data
    if (node.host || !message.remote_host) return node
    return { ...node, host: message.remote_host }
}

/**
 * Nối một discovery generic (ví dụ `UdpDiscovery` của `@simple-discovery/udp`) vào `Topology`: node local được
 * broadcast ra ngoài, message nhận được thì đưa node vào Topology.
 */
export class TopologyDiscoveryAdapter implements TopologyDiscovery {

    #binding = Subscription.EMPTY

    constructor(
        public readonly transporter: DiscoveryTransporter<SpiderMeshNode>,
        private readonly options: TopologyDiscoveryAdapterOptions = {},
    ) {}

    bind(context: TopologyDiscoveryContext) {
        this.#binding.unsubscribe()
        const binding = new Subscription()
        const tags = this.options.tags ?? SPIDER_MESH_DISCOVERY_TAGS
        let latestLocalNode: SpiderMeshNode | undefined

        const broadcast = (node: SpiderMeshNode) => {
            void this.transporter.broadcast(toDiscoveryMessage(node, tags)).catch(error => {
                this.options.onError?.(error)
            })
        }

        binding.add(context.localNode$.subscribe(node => {
            latestLocalNode = node
            broadcast(node)
        }))

        if (this.options.heartbeatIntervalMs && this.options.heartbeatIntervalMs > 0) {
            const heartbeat = setInterval(() => {
                if (latestLocalNode) broadcast(latestLocalNode)
            }, this.options.heartbeatIntervalMs)
            heartbeat.unref?.()
            binding.add(() => clearInterval(heartbeat))
        }

        binding.add(this.transporter.subscribe(message => {
            context.upsertRemote(withRemoteHost(message))
        }))

        this.#binding = binding
        return binding
    }

    /** Chuyển yêu cầu xác minh của Topology xuống discovery nếu nó hỗ trợ. */
    async verify(node_id: string) {
        return await this.transporter.verify?.(node_id) ?? 'unknown'
    }

    async close() {
        this.#binding.unsubscribe()
        if (this.options.closeTransporter !== false) {
            await this.transporter.close?.()
        }
    }
}
