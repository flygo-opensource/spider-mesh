import {
    BehaviorSubject,
    distinctUntilChanged,
    map,
    ReplaySubject,
    Subject,
    Subscription,
    type Observable,
} from 'rxjs'
import type {
    RpcRoutingOptions,
    SpiderMeshNode,
    TopologyDiscovery,
    TopologyEvent,
    TopologyNodeVerificationResult,
    TopologyReachabilityReport,
    TopologyReachabilityStatus,
    TopologyRoute,
    TopologyRouteRequest,
    TopologyRouteResult,
} from './types.js'

/** Cấu hình khởi tạo kho topology dùng chung của một SpiderMesh node. */
export type TopologyOptions = {
    /** Discovery được Topology sở hữu và kết nối khi nhận local node từ SpiderMesh. */
    discovery?: TopologyDiscovery
    /** Thời gian tối đa giữ remote node nếu Discovery không phát sự kiện offline. */
    staleAfterMs?: number
    /**
     * Xoá remote node khi mọi transporter đã báo nó `unreachable` liên tục trong khoảng này.
     * Dùng khi discovery chỉ để các node tìm thấy nhau (như UDP) và kết nối của transporter mới là
     * bằng chứng node còn sống: không cần heartbeat hay `staleAfterMs`.
     */
    removeUnreachableAfterMs?: number
}

type ReachabilityEntry = Required<Pick<TopologyReachabilityReport, 'status' | 'observed_at'>>
    & Pick<TopologyReachabilityReport, 'reason'>
    & {
        /** Thời điểm endpoint chuyển sang `status` hiện tại; không đổi khi báo lặp lại cùng status. */
        since: number
    }

/** Tùy chọn tương thích cho API chọn node trực tiếp trước đây của Registry. */
export type TopologyPickRpcTargetOptions = {
    node_id?: string
    filter?: (node: SpiderMeshNode) => boolean
}

/** Patch metadata của một node đã tồn tại trong Topology. */
export type TopologyNodePatch = Partial<Omit<SpiderMeshNode, 'node_id'>>

type RouteState = {
    index?: number
    activeByNode?: Map<string, number>
}

/**
 * Kho trạng thái node dùng chung cho mọi môi trường.
 *
 * Topology không tự khám phá hạ tầng; Discovery trong constructor sẽ đưa remote
 * node vào đây. Khi được gắn vào SpiderMesh, local node cũng được thêm vào cùng
 * một danh sách để local service có thể được route giống remote service.
 */
export class Topology {
    public readonly nodes$ = new BehaviorSubject<Map<string, SpiderMeshNode>>(new Map())
    readonly #events = new Subject<TopologyEvent>()
    /** Stream event node/endpoint dùng cho monitoring và availability realtime. */
    public readonly events$ = this.#events.asObservable()

    readonly #discovery?: TopologyDiscovery
    readonly #staleAfterMs?: number
    readonly #lastSeen = new Map<string, number>()
    readonly #routingStates = new Map<string, RouteState>()
    readonly #reachability = new Map<string, Map<string, ReachabilityEntry>>()
    readonly #verifyingNodes = new Map<string, Promise<TopologyNodeVerificationResult>>()
    readonly #localNode$ = new ReplaySubject<SpiderMeshNode>(1)
    #localNodeId?: string
    #localBinding = Subscription.EMPTY
    #discoveryBinding?: { unsubscribe(): void }
    #staleTimer?: ReturnType<typeof setInterval>
    readonly #removeUnreachableAfterMs?: number
    #unreachableTimer?: ReturnType<typeof setInterval>

    constructor(options: TopologyOptions = {}) {
        this.#discovery = options.discovery
        this.#staleAfterMs = options.staleAfterMs
        this.#removeUnreachableAfterMs = options.removeUnreachableAfterMs

        if (this.#staleAfterMs && this.#staleAfterMs > 0) {
            this.#staleTimer = setInterval(() => this.#removeStaleNodes(), Math.max(100, this.#staleAfterMs / 2))
            this.#staleTimer.unref?.()
        }

        if (this.#removeUnreachableAfterMs && this.#removeUnreachableAfterMs > 0) {
            this.#unreachableTimer = setInterval(
                () => this.#removeUnreachableNodes(),
                Math.max(100, Math.min(this.#removeUnreachableAfterMs / 2, 30_000)),
            )
            this.#unreachableTimer.unref?.()
        }
    }

    /** Node hiện tại của process, nếu Topology đã được bind vào SpiderMesh. */
    get localNode() {
        return this.#localNodeId ? this.nodes$.value.get(this.#localNodeId) : undefined
    }

    /** ID của node hiện tại, dùng để bỏ qua announcement vọng lại từ Discovery. */
    get localNodeId() {
        return this.#localNodeId
    }

    /**
     * Kết nối local node stream của SpiderMesh với Topology và Discovery.
     * Mỗi Topology chỉ thuộc về một SpiderMesh; bind lần hai sẽ thay binding cũ.
     */
    bindLocalNode(source: Observable<SpiderMeshNode>) {
        this.#localBinding.unsubscribe()
        this.#discoveryBinding?.unsubscribe()

        const localBinding = source.subscribe(node => {
            const previousLocalId = this.#localNodeId
            this.#localNodeId = node.node_id

            const nodes = new Map(this.nodes$.value)
            if (previousLocalId && previousLocalId !== node.node_id) nodes.delete(previousLocalId)
            nodes.set(node.node_id, this.#cloneNode(node))
            this.nodes$.next(nodes)
            this.#localNode$.next(this.#cloneNode(node))
        })

        this.#localBinding = localBinding

        const discoveryBinding = this.#discovery?.bind({
            localNode$: this.#localNode$.asObservable(),
            upsertRemote: node => this.upsertRemote(node),
            removeRemote: nodeId => this.removeRemote(nodeId),
        })
        this.#discoveryBinding = discoveryBinding || undefined

        return new Subscription(() => {
            localBinding.unsubscribe()
            discoveryBinding?.unsubscribe()
        })
    }

    /** Thêm hoặc thay thế snapshot đầy đủ của một remote node. */
    upsertRemote(node: SpiderMeshNode) {
        if (node.node_id === this.#localNodeId) return this.localNode

        const nodes = new Map(this.nodes$.value)
        const current = nodes.get(node.node_id)
        if (current && node.version < current.version) return current

        const cloned = this.#cloneNode(node)
        nodes.set(node.node_id, cloned)
        this.#lastSeen.set(node.node_id, Date.now())
        this.nodes$.next(nodes)
        if (!current) {
            this.#events.next({
                type: 'node-online',
                node: cloned,
                observed_at: Date.now(),
            })
        }
        return cloned
    }

    /** Alias chuyển tiếp của API Registry cũ. */
    upsertPeer(node: SpiderMeshNode) {
        return this.upsertRemote(node)
    }

    /** Cập nhật một phần metadata của node đã tồn tại. */
    patchPeer(nodeId: string, patch: TopologyNodePatch) {
        const nodes = new Map(this.nodes$.value)
        const current = nodes.get(nodeId)
        if (!current) return null

        const updated = this.#cloneNode({
            ...current,
            ...patch,
            node_id: nodeId,
            topics: patch.topics ?? current.topics,
            services: patch.services ? { ...current.services, ...patch.services } : current.services,
            nodes: patch.nodes ? { ...current.nodes, ...patch.nodes } : current.nodes,
            transporters: patch.transporters
                ? { ...current.transporters, ...patch.transporters }
                : current.transporters,
            build: patch.build === undefined ? current.build : patch.build,
        })
        nodes.set(nodeId, updated)
        if (nodeId !== this.#localNodeId) this.#lastSeen.set(nodeId, Date.now())
        this.nodes$.next(nodes)
        return updated
    }

    /** Xóa remote node; local node không thể bị Discovery xóa. */
    removeRemote(nodeId: string, reason: 'discovery' | 'verification' | 'stale' | 'unreachable' = 'discovery') {
        if (nodeId === this.#localNodeId) return false
        const nodes = new Map(this.nodes$.value)
        const node = nodes.get(nodeId)
        const deleted = nodes.delete(nodeId)
        if (deleted) {
            this.#lastSeen.delete(nodeId)
            this.#reachability.delete(nodeId)
            this.nodes$.next(nodes)
            this.#events.next({
                type: 'node-offline',
                node: node!,
                reason,
                observed_at: Date.now(),
            })
        }
        return deleted
    }

    /**
     * Nhận reachability từ transporter mà không thay đổi membership.
     * `suspect` và `unreachable` bị loại khỏi routing ngay; chỉ Discovery mới xóa node.
     */
    reportReachability(report: TopologyReachabilityReport) {
        if (!this.nodes$.value.has(report.node_id)) return false

        const observedAt = report.observed_at ?? Date.now()
        const byTransporter = this.#reachability.get(report.node_id) ?? new Map()
        const previous = byTransporter.get(report.transporter)
        byTransporter.set(report.transporter, {
            status: report.status,
            reason: report.reason,
            observed_at: observedAt,
            since: previous?.status === report.status ? previous.since : observedAt,
        })
        this.#reachability.set(report.node_id, byTransporter)

        // `recovered` chỉ có nghĩa khi endpoint đã từng bị suspect/unreachable.
        // Connection thành công lần đầu đã được biểu diễn bởi node-online, không phát recovery giả.
        if (previous?.status !== report.status && (report.status !== 'reachable' || previous != undefined)) {
            const type = report.status === 'reachable'
                ? 'endpoint-recovered'
                : report.status === 'suspect'
                    ? 'endpoint-suspect'
                    : 'endpoint-unreachable'
            this.#events.next({
                type,
                node_id: report.node_id,
                transporter: report.transporter,
                reason: report.reason,
                observed_at: observedAt,
            })
        }

        if (report.status === 'unreachable' && previous?.status !== 'unreachable') {
            void this.verifyNode(report.node_id)
        }
        return true
    }

    /** Trạng thái endpoint hiện tại; endpoint chưa có báo cáo được xem là reachable để tương thích. */
    getReachability(nodeId: string, transporter: string): TopologyReachabilityStatus {
        return this.#reachability.get(nodeId)?.get(transporter)?.status ?? 'reachable'
    }

    /** Kiểm tra endpoint có được phép tham gia routing hay không. */
    isReachable(nodeId: string, transporter: string) {
        return this.getReachability(nodeId, transporter) === 'reachable'
    }

    /**
     * Nhờ Discovery xác minh membership; các lời gọi đồng thời cho cùng node dùng chung Promise.
     * Chỉ kết quả `dead` mới xóa node khỏi Topology.
     */
    verifyNode(nodeId: string): Promise<TopologyNodeVerificationResult> {
        const current = this.#verifyingNodes.get(nodeId)
        if (current) return current
        if (!this.nodes$.value.has(nodeId) || !this.#discovery?.verify) {
            return Promise.resolve('unknown')
        }

        const lastSeen = this.#lastSeen.get(nodeId)
        const verification = Promise.resolve(this.#discovery.verify(nodeId))
            .catch(() => 'unknown' as const)
            .then(result => {
                // Snapshot mới đến trong lúc verify có quyền vô hiệu hóa kết quả dead cũ.
                if (result === 'dead' && this.#lastSeen.get(nodeId) === lastSeen) {
                    this.removeRemote(nodeId, 'verification')
                }
                return result
            })
            .finally(() => {
                if (this.#verifyingNodes.get(nodeId) === verification) {
                    this.#verifyingNodes.delete(nodeId)
                }
            })
        this.#verifyingNodes.set(nodeId, verification)
        return verification
    }

    /** Alias chuyển tiếp của API Registry cũ. */
    removePeer(nodeId: string) {
        return this.removeRemote(nodeId)
    }

    /** Lấy một node theo ID, bao gồm cả local node. */
    getPeer(nodeId: string) {
        return this.nodes$.value.get(nodeId)
    }

    /** Liệt kê node đang cung cấp service; không truyền service sẽ trả toàn bộ node. */
    list(service?: string) {
        return [...this.nodes$.value.values()].filter(node => {
            return !service || node.services[service] != undefined
        })
    }

    /** Alias chuyển tiếp của API Registry cũ. */
    listPeers(service?: string) {
        return this.list(service)
    }

    /** Theo dõi danh sách node và chỉ emit khi membership hoặc version thay đổi. */
    watch(service?: string) {
        return this.nodes$.pipe(
            map(() => this.list(service)),
            distinctUntilChanged((prev, curr) => {
                if (prev.length !== curr.length) return false
                const previousVersions = new Map(prev.map(node => [node.node_id, node.version]))
                return curr.every(node => previousVersions.get(node.node_id) === node.version)
            }),
        )
    }

    /** Alias availability để giảm chi phí migration. */
    listNodes(service: string) {
        return this.list(service)
    }

    /** Alias availability để giảm chi phí migration. */
    watchService(service: string) {
        return this.watch(service)
    }

    /**
     * Resolve một request thành node cụ thể.
     * Không có `node_id` và không có `routing` nghĩa là hạ tầng/transporter tự route.
     */
    route(request: TopologyRouteRequest): TopologyRoute | undefined {
        const excluded = new Set(request.exclude_node_ids ?? [])
        const candidates = this.list(request.service).filter(node => {
            const endpoint = node.transporters?.[request.transporter]
            return !excluded.has(node.node_id)
                && this.isReachable(node.node_id, request.transporter)
                && endpoint !== undefined
                && endpoint !== null
                && endpoint !== false
        })

        if (request.node_id) {
            const node = candidates.find(candidate => candidate.node_id === request.node_id)
            return node ? this.#toRoute(node, request.transporter) : undefined
        }

        if (!request.routing || candidates.length === 0) return undefined

        const stateKey = this.#routingStateKey(request)
        const node = this.#selectNode(candidates, request.routing, stateKey)
        if (!node) return undefined

        return {
            ...this.#toRoute(node, request.transporter),
            state_key: stateKey,
        }
    }

    /** Cập nhật state least-active/latency sau khi transporter hoàn tất request. */
    report(route: TopologyRoute, _result: TopologyRouteResult) {
        if (!route.state_key) return
        const state = this.#routingStates.get(route.state_key)
        const activeByNode = state?.activeByNode
        if (!activeByNode) return

        const current = activeByNode.get(route.node.node_id) ?? 0
        activeByNode.set(route.node.node_id, Math.max(0, current - 1))
    }

    /** API round-robin cũ, giữ lại cho package chưa migrate xong. */
    pickRpcNode(service: string, options: TopologyPickRpcTargetOptions = {}) {
        if (options.node_id) {
            const node = this.getPeer(options.node_id)
            if (!node || node.services[service] == undefined || (options.filter && !options.filter(node))) return null
            return node.node_id
        }

        const candidates = options.filter ? this.list(service).filter(options.filter) : this.list(service)
        if (candidates.length === 0) return null
        const key = `legacy:${service}`
        return this.#selectNode(candidates, { strategy: 'round-robin' }, key)?.node_id ?? null
    }

    /** API lookup metadata cũ, không nên dùng cho routing mới. */
    getRpcTransporterName(service: string) {
        for (const node of this.list(service)) {
            if (typeof node.transporters?.rpc === 'string') return node.transporters.rpc
        }
        return undefined
    }

    /** Liệt kê node đã quảng bá một event topic. */
    listTopicNodes(topic: string) {
        return this.list().filter(node => node.topics.includes(topic))
    }

    /** Dừng Discovery, timer và toàn bộ subscription do Topology sở hữu. */
    async close() {
        this.#localBinding.unsubscribe()
        this.#discoveryBinding?.unsubscribe()
        if (this.#staleTimer) clearInterval(this.#staleTimer)
        if (this.#unreachableTimer) clearInterval(this.#unreachableTimer)
        await this.#discovery?.close?.()
        this.#localNode$.complete()
        this.#events.complete()
        this.nodes$.complete()
    }

    #selectNode(nodes: SpiderMeshNode[], routing: RpcRoutingOptions, stateKey: string) {
        if (routing.strategy === 'random') {
            return nodes[Math.floor(Math.random() * nodes.length)]
        }

        if (routing.strategy === 'consistent-hash') {
            // Sắp xếp để cùng một topology set cho cùng kết quả dù Discovery đến khác thứ tự.
            const stableNodes = [...nodes].sort((left, right) => left.node_id.localeCompare(right.node_id))
            return stableNodes[this.#hash(routing.key) % stableNodes.length]
        }

        const state = this.#routingStates.get(stateKey) ?? {}
        this.#routingStates.set(stateKey, state)

        if (routing.strategy === 'least-active') {
            const activeByNode = state.activeByNode ?? new Map<string, number>()
            state.activeByNode = activeByNode
            const selected = [...nodes].sort((a, b) => {
                return (activeByNode.get(a.node_id) ?? 0) - (activeByNode.get(b.node_id) ?? 0)
            })[0]
            if (selected) activeByNode.set(selected.node_id, (activeByNode.get(selected.node_id) ?? 0) + 1)
            return selected
        }

        const index = state.index ?? 0
        const selected = nodes[index % nodes.length]
        state.index = (index + 1) % nodes.length
        return selected
    }

    #routingStateKey(request: TopologyRouteRequest) {
        const routing = request.routing!
        const customStateKey = 'state_key' in routing ? routing.state_key : undefined
        return `${request.transporter}:${request.service}:${routing.strategy}:${customStateKey ?? 'default'}`
    }

    #toRoute(node: SpiderMeshNode, transporter: string): TopologyRoute {
        return {
            node,
            endpoint: node.transporters[transporter],
        }
    }

    #hash(value: string) {
        let hash = 2166136261
        for (let index = 0; index < value.length; index++) {
            hash ^= value.charCodeAt(index)
            hash = Math.imul(hash, 16777619)
        }
        return hash >>> 0
    }

    /** Xoá node mà mọi transporter đã báo `unreachable` liên tục quá `removeUnreachableAfterMs`. */
    #removeUnreachableNodes() {
        if (!this.#removeUnreachableAfterMs) return
        const now = Date.now()
        for (const [nodeId, byTransporter] of this.#reachability) {
            if (nodeId === this.#localNodeId || byTransporter.size === 0) continue
            const entries = [...byTransporter.values()]
            if (!entries.every(entry => entry.status === 'unreachable')) continue
            // Tính từ lần chuyển sang unreachable gần nhất: mọi endpoint đều phải đứt đủ lâu.
            const unreachableSince = Math.max(...entries.map(entry => entry.since))
            if (now - unreachableSince >= this.#removeUnreachableAfterMs) {
                this.removeRemote(nodeId, 'unreachable')
            }
        }
    }

    #removeStaleNodes() {
        if (!this.#staleAfterMs) return
        const threshold = Date.now() - this.#staleAfterMs
        for (const [nodeId, lastSeen] of this.#lastSeen) {
            if (lastSeen < threshold) this.removeRemote(nodeId, 'stale')
        }
    }

    #cloneNode(node: SpiderMeshNode): SpiderMeshNode {
        return {
            ...node,
            topics: [...(node.topics || [])],
            services: { ...(node.services || {}) },
            nodes: { ...(node.nodes || {}) },
            transporters: { ...(node.transporters || {}) },
            build: node.build ? {
                ...node.build,
                tags: node.build.tags ? { ...node.build.tags } : undefined,
            } : undefined,
        }
    }
}
