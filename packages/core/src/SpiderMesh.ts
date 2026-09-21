import { BehaviorSubject, catchError, distinctUntilChanged, EMPTY, filter, finalize, firstValueFrom, map, merge, mergeMap, Observable, of, retry, Subject, Subscription, switchMap, take, takeUntil, tap, throwError, timeout, timer } from "rxjs"
import { listBeforeMicroserviceOnlineMethods } from "./decorators/BeforeMicroserviceOnline.js";
import { LOCAL_SERVICES$ } from "./decorators/Microservice.js";
import { SPIDERMESH_NAMESPACE, SPIDERMESH_NODE_HOSTNAME } from "../const.js";
import { SpiderMeshNode, RpcTransporter, RpcOptions, RpcEvent, RpcResponsePacket, RpcCancelPacket, NodeRef, RpcProbeResult } from './types.js'
import type { Topology } from './Topology.js'
import { createRandomNodeId } from './helpers/createRandomNodeId.js'
import { isSubscribable } from './helpers/isSubscribable.js'
import { normalizeRpcError } from './helpers/normalizeRpcError.js'
import { readBuildInfo } from './helpers/readBuildInfo.js'

/** Snapshot node tương thích với hello event của protocol cũ. */
export type HelloEvent = SpiderMeshNode & { back?: boolean }
/** Hàm quyết định danh sách node hiện tại đã thỏa điều kiện chờ hay chưa. */
export type ServiceChecker = (nodes: NodeRef[]) => Promise<boolean> | boolean
/** Map node tương thích với public API cũ của SpiderMesh. */
export type NodesMap = {
    nodes: Map<string, SpiderMeshNode>,
    last_updated_node_id: string
}

type PendingRpcStream = {
    stream: Subject<any>
    finished: boolean
    /**
     * Node đang phục vụ request. Biết từ đầu khi caller pin `node_id` hoặc transporter tự
     * route; với round-robin thì chỉ biết sau packet response đầu tiên.
     */
    destination_node_id?: string
}

type RpcTransportTarget = {
    node_id?: string
    transporter: RpcTransporter
}

/** Cấu hình khởi tạo runtime SpiderMesh và các dependency RPC của node. */
export type SpiderMeshOptions = {
    node_id?: string
    topology?: Topology
    transporters?: RpcTransporter[]
}

/**
 * Runtime RPC chính: quản lý service local, request lifecycle và transporter.
 * Topology là optional; khi không có, availability tối thiểu được kiểm tra bằng probe.
 */
export class SpiderMesh {

    public readonly node_id: string
    public readonly namespace = SPIDERMESH_NAMESPACE
    public readonly topology?: Topology
    #transporters = {
        rpcs: new BehaviorSubject(new Map<string, RpcTransporter>()),
    }
    #localServices = new Map<string, any>()
    #me$: BehaviorSubject<SpiderMeshNode>
    public readonly localNode$: Observable<SpiderMeshNode>

    get localNode() {
        return this.#me$.value
    }

    #rpc = {
        pending: new Map<string, PendingRpcStream>(),
        running: new Map<string, Subscription>()
    }

    constructor(options: SpiderMeshOptions = {}) {
        this.node_id = options.node_id ?? createRandomNodeId()
        this.topology = options.topology
        this.#me$ = new BehaviorSubject<SpiderMeshNode>({
            host: SPIDERMESH_NODE_HOSTNAME,
            namespace: SPIDERMESH_NAMESPACE,
            node_id: this.node_id,
            topics: [],
            services: {},
            transporters: {},
            nodes: {},
            version: 0,
            build: readBuildInfo(),
        })
        this.localNode$ = this.#me$.asObservable()
        this.topology?.bindLocalNode(this.localNode$)

        LOCAL_SERVICES$.pipe(
            mergeMap(async service => {
                const list = listBeforeMicroserviceOnlineMethods(service.instance)
                for (const method of list) {
                    await service.instance[method]()
                }

                this.#localServices.set(service.name, service.instance)
                this.#refresh({
                    services: {
                        [service.name]: service.metadata
                    }
                })
            }, 1),
            catchError(() => EMPTY)
        ).subscribe()

        for (const transporter of options.transporters ?? []) {
            this.registerTransporter(transporter)
        }
    }


    /** Đăng ký một transporter bằng tên ổn định do chính transporter khai báo. */
    registerTransporter(rpcTransporter: RpcTransporter) {
        const resolvedName = rpcTransporter.name
        if (!resolvedName) throw new Error('RpcTransporter.name is required')
        if (this.#transporters.rpcs.value.has(resolvedName)) {
            throw new Error(`RPC transporter "${resolvedName}" is already registered`)
        }
        const subscription = new Subscription()

        subscription.add(this.#linkRpcTransporter(resolvedName, rpcTransporter).subscribe())
        void Promise.resolve(rpcTransporter.start?.({
            topology: this.topology,
            localNode$: this.localNode$,
            hasLocalService: service => this.#localServices.has(service),
        })).catch(() => undefined)

        subscription.add(() => {
            if (this.#transporters.rpcs.value.get(resolvedName) === rpcTransporter) {
                const transporters = new Map(this.#transporters.rpcs.value)
                transporters.delete(resolvedName)
                this.#transporters.rpcs.next(transporters)
                this.#refresh({ transporters: { [resolvedName]: false } })
            }
            void rpcTransporter.stop?.()
        })
        return subscription
    }

    listRpcNodes(service: string): NodeRef[] {
        return this.#listRoutableNodes(service)
    }

    watchService(service: string): Observable<NodeRef[]> {
        if (!this.topology) return of([])
        return merge(this.topology.watch(service), this.topology.events$, timer(0, 250)).pipe(
            map(() => this.#listRoutableNodes(service)),
            distinctUntilChanged((previous, current) => {
                if (previous.length !== current.length) return false
                const versions = new Map(previous.map(node => [node.node_id, (node as SpiderMeshNode).version]))
                return current.every(node => versions.get(node.node_id) === (node as SpiderMeshNode).version)
            }),
        )
    }

    /**
     * Availability RPC đòi hỏi cả service lẫn endpoint transporter đã sẵn sàng.
     * Discovery có thể phát hai snapshot này ở hai thời điểm khác nhau khi node khởi động.
     */
    #listRoutableNodes(service: string): NodeRef[] {
        const nodes = this.topology?.list(service) ?? []
        const transporters = [...this.#transporters.rpcs.value.values()]
        return nodes.filter(node => transporters.some(transporter => {
            return transporter.canRoute
                ? transporter.canRoute(service, node.node_id)
                : ![undefined, null, false].includes(node.transporters?.[transporter.name])
        }))
    }

    #selectRpcTransport(filters: Partial<Pick<RpcOptions<any>, 'node_id' | 'service' | 'transporter'>> = {}): RpcTransporter | undefined {
        if (!filters.service) return undefined
        // Explicit binding always wins.
        if (filters.transporter) {
            const transporter = this.#transporters.rpcs.value.get(filters.transporter)
            if (!transporter) return undefined
            if (transporter.canRoute && !transporter.canRoute(filters.service, filters.node_id)) {
                return undefined
            }
            return transporter
        }

        const rpcs = this.#transporters.rpcs.value
        // Nothing to choose from — probe canRoute only when at least one RPC transporter exists.
        if (rpcs.size === 0) return undefined

        // Reachability-first: the first-registered transporter may not actually be able to
        // reach the provider (e.g. a LAN Http2Rpc has no route to a relay-only peer that the
        // WS transporter sees). Prefer a transporter that declares it can route to this
        // service/node, so routing follows the same source of truth as wait()/watch()/nodes.
        for (const transporter of rpcs.values()) {
            if (transporter.canRoute?.(filters.service, filters.node_id)) {
                return transporter
            }
        }

        // Transporter cũ không có canRoute vẫn được phép tự resolve trong send().
        // Nếu transporter đã triển khai canRoute và tất cả đều trả false thì phải báo
        // offline ngay, tránh biến trạng thái offline rõ ràng thành RPC timeout.
        for (const transporter of rpcs.values()) {
            if (!transporter.canRoute) return transporter
        }
        return undefined
    }

    #completePendingRpc(request_id: string) {
        this.#rpc.pending.delete(request_id)
    }

    /**
     * Node offline trong lúc còn RPC đang bay: stream phải `error` ngay thay vì treo im
     * tới khi `timeout` (là idle-timeout giữa hai emission) kích hoạt — hoặc treo vô hạn
     * nếu caller không đặt timeout. Xem thêm chú thích ở `#selectRpcTransport`.
     */
    #failPendingRpcForNode(node_id: string) {
        for (const [request_id, pending] of this.#rpc.pending) {
            if (pending.finished) continue
            if (pending.destination_node_id !== node_id) continue

            pending.finished = true
            this.#completePendingRpc(request_id)
            pending.stream.error({
                code: 'MICROSERVICE_OFFLINE',
                message: `Node ${node_id} went offline while the RPC was in flight`,
            })
        }
    }

    #completeRunningRpc(request_id: string) {
        this.#rpc.running.delete(request_id)
    }

    #index = 1
    callRemoteService<R, T>(options: RpcOptions<T>) {
        return of(1).pipe(
            mergeMap(() => {
                const transporter = this.#selectRpcTransport(options)
                if (!transporter) throw { code: 'MICROSERVICE_OFFLINE', message: `No transporter available for service ${options.service}` }
                return new Observable<R>(subscriber => {
                    const request_id = `${this.node_id}:${Date.now().toString(36)}:${(this.#index++).toString(36)}`
                    const pending: PendingRpcStream & { stream: Subject<R> } = {
                        stream: new Subject<R>(),
                        finished: false,
                        destination_node_id: options.node_id,
                    }
                    this.#rpc.pending.set(request_id, pending)

                    const subscription = pending.stream.subscribe({
                        next: value => subscriber.next(value),
                        error: error => {
                            pending.finished = true
                            subscriber.error(error)
                        },
                        complete: () => {
                            pending.finished = true
                            subscriber.complete()
                        }
                    })

                    let cancelSend: (() => void) | undefined
                    let cancelled = false
                    transporter.send({
                        kind: 'request',
                        request_id,
                        sender_node_id: this.node_id,
                        destination_node_id: options.node_id,
                        service: options.service,
                        method: options.method,
                        args: options.args,
                        routing: options.routing,
                    }).then(({ cancel, destination_node_id }) => {
                        if (destination_node_id) pending.destination_node_id = destination_node_id
                        cancelSend = cancel
                        // Unsubscribe có thể xảy ra trước khi promise này resolve; nếu không
                        // gửi bù ở đây thì provider giữ stream chạy mãi.
                        if (cancelled) cancel()
                    }).catch((error: any) => {
                        pending.finished = true
                        pending.stream.error(normalizeRpcError(error))
                        this.#completePendingRpc(request_id)
                    })

                    return () => {
                        subscription.unsubscribe()
                        if (!pending.finished) {
                            cancelled = true
                            cancelSend?.()
                        }
                        this.#completePendingRpc(request_id)
                    }
                })
            }),
            options.timeout ? timeout({
                each: options.timeout,
                with: () => throwError(() => ({ code: 'MICROSERVICE_RPC_TIMEOUT', message: 'RPC timeout' }))
            }) : tap(),
            retry({
                delay: (e: { code: string }, count: number) => {
                    if (e.code === 'MICROSERVICE_OFFLINE') {
                        // RxJS đếm lượt thử lại từ 1, nên `retry: N` phải cho phép đủ N lượt.
                        if (options.retry && count <= options.retry) return timer(1000)
                    }
                    throw e
                }
            }),
            catchError(e => {
                if (options.fallback != undefined) return of(options.fallback as any as T)
                throw e
            })
        )
    }

    /**
     * Kiểm tra service bằng Topology nếu có, nếu không sẽ gọi probe của transporter.
     * Kết quả probe không được dùng để giả lập danh sách node.
     */
    async probeService(service: string, node_id?: string): Promise<RpcProbeResult> {
        if (this.topology) {
            const nodes = this.#listRoutableNodes(service)
            const reachable = node_id
                ? nodes.some(node => node.node_id === node_id)
                : nodes.length > 0
            return { reachable, node_id: node_id ?? nodes[0]?.node_id }
        }

        for (const transporter of this.#transporters.rpcs.value.values()) {
            if (transporter.probe) {
                try {
                    const result = await transporter.probe({ service, node_id })
                    if (result.reachable) return result
                } catch {
                    // Probe lỗi chỉ làm transporter này unavailable; transporter khác vẫn được thử.
                }
                continue
            }
            if (transporter.canRoute?.(service, node_id)) {
                return { reachable: true, node_id }
            }
        }
        return { reachable: false }
    }

    /**
     * Chờ service sẵn sàng. Có Topology thì chờ membership; không có sẽ poll probe.
     */
    waitForService(
        service: string,
        checker: ServiceChecker = nodes => nodes.length > 0,
        stop$: Observable<any> = EMPTY,
    ) {
        if (this.topology) {
            return firstValueFrom(this.watchService(service).pipe(
                takeUntil(stop$),
                mergeMap(async nodes => ({ nodes, ready: await checker(nodes) })),
                filter(result => result.ready),
                map(result => result.nodes),
            ), { defaultValue: null })
        }

        return firstValueFrom(timer(0, 500).pipe(
            takeUntil(stop$),
            mergeMap(() => this.probeService(service)),
            filter(result => result.reachable),
            map(() => [] as NodeRef[]),
            take(1),
        ), { defaultValue: null })
    }

    #linkRpcTransporter(name: string, transporter: RpcTransporter) {
        this.#transporters.rpcs.next(new Map(this.#transporters.rpcs.value).set(name, transporter))
        this.#ensureLocalTransporterPresence(name)

        const initialEndpoints = (transporter as RpcTransporter & { metadata?: RpcEvent['endpoints'] }).metadata
        if (initialEndpoints) {
            this.#refresh({
                transporters: {
                    [name]: initialEndpoints
                }
            })
        }

        return transporter.pipe(
            map(({ rpc, offline, endpoints }) => {
                if (rpc) {
                    const packet = rpc

                    if (packet.kind == 'request') {
                        const reply = async (response: Omit<RpcResponsePacket, 'kind' | 'request_id' | 'destination_node_id' | 'sender_node_id'>) => {
                            await transporter.send({
                                kind: 'response',
                                request_id: packet.request_id,
                                destination_node_id: packet.sender_node_id,
                                sender_node_id: this.node_id,
                                ...response
                            })
                        }

                        const handleResponse = (response: any) => {
                            if (isSubscribable(response)) {
                                let queue = Promise.resolve()
                                const running = response.subscribe({
                                    next: data => {
                                        queue = queue.then(() => reply({ data }))
                                    },
                                    error: error => {
                                        queue = queue.then(() => reply({
                                            error: normalizeRpcError(error),
                                            completed: true
                                        })).finally(() => this.#completeRunningRpc(packet.request_id))
                                    },
                                    complete: () => {
                                        queue = queue.then(() => reply({ completed: true })).finally(() => this.#completeRunningRpc(packet.request_id))
                                    }
                                })

                                this.#rpc.running.set(packet.request_id, running)
                                return
                            }

                            void reply({ data: response, completed: true })
                        }

                        const service = this.#localServices.get(packet.service)
                        if (!service || typeof service[packet.method] !== 'function') {
                            void reply({
                                error: {
                                    code: 'MICROSERVICE_NOT_FOUND',
                                    message: `Service ${packet.service}.${packet.method} not found`
                                },
                                completed: true
                            })
                        } else {
                            try {
                                const response = service[packet.method].apply(service, packet.args)

                                Promise.resolve(response)
                                    .then(handleResponse)
                                    .catch(error => reply({
                                        error: normalizeRpcError(error),
                                        completed: true
                                    }))
                            } catch (error) {
                                void reply({
                                    error: normalizeRpcError(error),
                                    completed: true
                                })
                            }
                        }
                    }

                    if (packet?.kind === 'response') {
                        const pending = this.#rpc.pending.get(packet.request_id)
                        if (pending) {
                            // Round-robin: chỉ tới packet đầu tiên caller mới biết provider nào
                            // đang phục vụ, và từ đó mới đóng được stream khi node đó offline.
                            if (packet.sender_node_id) pending.destination_node_id = packet.sender_node_id

                            // Đánh dấu terminal trước khi phát data. `await` dùng firstValueFrom()
                            // sẽ unsubscribe ngay trong `next`; nếu đánh dấu sau, teardown gửi
                            // cancel thừa và có thể phá response stream vừa hoàn tất.
                            if (packet.completed || packet.error != undefined) {
                                pending.finished = true
                            }

                            if ('data' in packet) {
                                pending.stream.next(packet.data)
                            }

                            if ('error' in packet && packet.error != undefined) {
                                pending.stream.error(packet.error)
                                this.#completePendingRpc(packet.request_id)
                            } else if (packet.completed) {
                                pending.stream.complete()
                                this.#completePendingRpc(packet.request_id)
                            }
                        }
                    }

                    if (packet?.kind === 'cancel') {
                        const stream = this.#rpc.running.get(packet.request_id)
                        if (stream) {
                            stream.unsubscribe()
                            this.#completeRunningRpc(packet.request_id)
                        }
                    }
                }

                if (offline) {
                    this.#failPendingRpcForNode(offline)
                }

                if (endpoints) {
                    this.#refresh({
                        transporters: {
                            [name]: endpoints
                        }
                    })
                }
            }),
            finalize(() => undefined)
        )
    }

    #refresh(patch: Partial<Omit<SpiderMeshNode, 'version'>>) {
        const current = this.#me$.value
        this.#me$.next({
            ...current,
            ...patch,
            version: current.version + 1,
            topics: patch.topics || current.topics,
            services: {
                ...current.services,
                ...(patch.services || {})
            },
            nodes: {
                ...current.nodes,
                ...(patch.nodes || {})
            },
            transporters: {
                ...current.transporters,
                ...(patch.transporters || {})
            }
        })
    }

    #ensureLocalTransporterPresence(name: string) {
        if (Object.hasOwn(this.#me$.value.transporters, name)) return

        this.#refresh({
            transporters: {
                [name]: true
            }
        })
    }

    /**
     * Update metadata owned by an external capability package (for example
     * `@spider-mesh/events`). Core only stores and announces the value.
     */
    setLocalTransporterMetadata(name: string, metadata: unknown = true) {
        this.#refresh({
            transporters: {
                [name]: metadata,
            },
        })
    }

    /**
     * Replace the local topic snapshot observed by external bindings.
     * Topic ownership lives in `@spider-mesh/events`; this bridge remains so
     * brokerless transports can discover subscribers without coupling core to
     * the event API.
     */
    setLocalTopics(topics: string[]) {
        const next = [...new Set(topics)].sort()
        const current = this.#me$.value.topics
        if (next.length === current.length && next.every((topic, index) => topic === current[index])) return
        this.#refresh({ topics: next })
    }
}
