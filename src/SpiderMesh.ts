import { BehaviorSubject, catchError, EMPTY, filter, finalize, from, lastValueFrom, map, mergeMap, NEVER, Observable, of, retry, share, Subject, Subscription, take, tap, throwError, timeout, timer } from "rxjs"
import { listBeforeMicroserviceOnlineMethods } from "./decorators/BeforeMicroserviceOnline.js";
import { LOCAL_SERVICES$ } from "./decorators/Microservice.js";
import { SPIDERMESH_NAMESPACE, SPIDERMESH_NODE_HOSTNAME } from "../const.js";
import { SpiderMeshNode, RpcTransporter, PubsubTransporter, DiscoveryTransporter, RpcOptions, SpiderMeshError, RpcEvent, RpcResponsePacket, DiscoveryEvent, MeshTransporter, RpcCancelPacket } from './types.js'
import { Registry } from './Registry.js'

export type HelloEvent = SpiderMeshNode & { back?: boolean }
export type ServiceChecker = (nodes: SpiderMeshNode[]) => Promise<boolean> | boolean
export type NodesMap = {
    nodes: Map<string, SpiderMeshNode>,
    last_updated_node_id: string
}

type PendingRpcStream = {
    stream: Subject<any>
    finished: boolean
}

type RpcTransportTarget = {
    node_id?: string
    transporter: RpcTransporter
}

const isSubscribable = (value: unknown): value is Observable<unknown> => {
    return !!value && typeof value === 'object' && typeof (value as { subscribe?: unknown }).subscribe === 'function'
}

const isNamedTransporter = (value: unknown): value is { constructor?: { name?: string } } => {
    return !!value && typeof value === 'object'
}

export class SpiderMesh {

    public readonly node_id = `${Date.now().toString(36).toUpperCase()}|${Math.random().toString(36).slice(10).toUpperCase()}`
    public readonly namespace = SPIDERMESH_NAMESPACE
    #transporters = {
        rpcs: new Map<string, RpcTransporter>(),
        pubsubs: new Map<string, PubsubTransporter>(),
        discoveries: new Map<string, DiscoveryTransporter>()
    }
    #localServices = new Map<string, any>()
    #me$ = new BehaviorSubject<SpiderMeshNode>({
        host: SPIDERMESH_NODE_HOSTNAME,
        namespace: SPIDERMESH_NAMESPACE,
        node_id: this.node_id,
        topics: [],
        services: {},
        transporters: {},
        nodes: {},
        version: 0,
    })

    #rpc = {
        pending: new Map<string, PendingRpcStream>(),
        running: new Map<string, Subscription>()
    }

    constructor(readonly registry?: Registry) {
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
    }


    registerTransporter(meshTransporter: MeshTransporter, name?: string) {
        const resolvedName = name
            || (isNamedTransporter(meshTransporter) ? meshTransporter.constructor?.name : undefined)
            || 'AnonymousTransporter'
        const subscription = new Subscription()

        if (this.registry && typeof meshTransporter.linkRegistry === 'function') {
            meshTransporter.linkRegistry(this.registry)
        }

        if (this.#isRpcTransporter(meshTransporter)) {
            subscription.add(this.#linkRpcTransporter(resolvedName, meshTransporter).subscribe())
        }
        if (this.#isPubsubTransporter(meshTransporter)) {
            subscription.add(this.#linkPubsubTransporter(resolvedName, meshTransporter).subscribe())
        }
        if (this.#isDiscoveryTransporter(meshTransporter)) {
            subscription.add(this.#linkDiscoveryTransporter(resolvedName, meshTransporter).subscribe())
        }

        return subscription
    }

    listRpcNodes(service: string) {
        return (this.registry?.listPeers({ service }) || []).filter(node => {
            return typeof node.transporters?.rpc === 'string'
        })
    }

    watchService(service: string) {
        if (!this.registry) return EMPTY
        return this.registry.watch(service).pipe(
            map(() => this.listRpcNodes(service))
        )
    }

    #selectRpcTransport(filters: Partial<Pick<RpcOptions<any>, 'node_id' | 'service' | 'transporter'>> = {}): RpcTransportTarget | null {
        if (!filters.service) return null

        const transporterName = typeof filters.transporter === 'string'
            ? filters.transporter
            : filters.transporter?.name

        if (!this.registry) {
            const transporter = transporterName
                ? this.#transporters.rpcs.get(transporterName)
                : this.#transporters.rpcs.values().next().value

            if (!transporter) return null

            return {
                node_id: filters.node_id,
                transporter
            }
        }

        const nodeId = this.registry.pickRpcNode(filters.service, {
            node_id: filters.node_id
        })
        const resolvedTransporterName = filters.node_id || nodeId
            ? this.registry.getRpcTransporterName(filters.service, {
                node_id: nodeId || filters.node_id
            })
            : transporterName
        if (!resolvedTransporterName) return null

        const transporter = this.#transporters.rpcs.get(resolvedTransporterName)
        if (!transporter) return null
        return {
            node_id: nodeId || filters.node_id,
            transporter
        }
    }

    #normalizeRpcError(error: any): SpiderMeshError | { code?: string, message: string } {
        if (error && typeof error === 'object') {
            return {
                code: typeof error.code === 'string' ? error.code : undefined,
                message: typeof error.message === 'string' ? error.message : 'Unknown RPC error'
            }
        }

        return { message: typeof error === 'string' ? error : 'Unknown RPC error' }
    }

    #completePendingRpc(request_id: string) {
        this.#rpc.pending.delete(request_id)
    }

    #completeRunningRpc(request_id: string) {
        this.#rpc.running.delete(request_id)
    }

    callRemoteService<R, T>(options: RpcOptions<T>) {
        const source$: Observable<unknown> = this.registry
            ? this.registry.watch(options.service)
            : of(undefined)

        return source$.pipe(
            map(() => this.#selectRpcTransport(options)),
            filter((target): target is RpcTransportTarget => !!target),
            take(1),
            mergeMap(target => {
                return new Observable<R>(subscriber => {
                    const request_id = `${this.node_id}:${Date.now().toString(36)}:${Math.random().toString(36).slice(2)}`
                    const pending = {
                        stream: new Subject<R>(),
                        finished: false
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

                    target.transporter.send({
                        kind: 'request',
                        request_id,
                        source_node_id: this.node_id,
                        target_node_id: target.node_id || '',
                        service: options.service,
                        method: options.method,
                        args: options.args
                    }, target.node_id).catch((error: any) => {
                        pending.finished = true
                        pending.stream.error(this.#normalizeRpcError(error))
                        this.#completePendingRpc(request_id)
                    })

                    return () => {
                        subscription.unsubscribe()
                        if (!pending.finished && this.#rpc.pending.has(request_id) && target.node_id) {
                            void target.transporter.send({
                                kind: 'cancel',
                                request_id,
                                source_node_id: this.node_id,
                                target_node_id: target.node_id,
                            } satisfies RpcCancelPacket, target.node_id).catch(() => undefined)
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
                        if (options.retry && count < options.retry) return timer(1000)
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

    #linkRpcTransporter(name: string, transporter: RpcTransporter) {
        this.#transporters.rpcs.set(name, transporter)
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
                    const packet = rpc.packet

                    if (packet?.kind === 'request' && packet.target_node_id === this.node_id) {
                        const reply = async (response: Omit<RpcResponsePacket, 'kind' | 'request_id' | 'source_node_id' | 'target_node_id'>) => {
                            await transporter.send({
                                kind: 'response',
                                request_id: packet.request_id,
                                source_node_id: this.node_id,
                                target_node_id: rpc.node_id,
                                ...response
                            }, rpc.node_id)
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
                                            error: this.#normalizeRpcError(error),
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
                                        error: this.#normalizeRpcError(error),
                                        completed: true
                                    }))
                            } catch (error) {
                                void reply({
                                    error: this.#normalizeRpcError(error),
                                    completed: true
                                })
                            }
                        }
                    }

                    if (packet?.kind === 'response') {
                        const pending = this.#rpc.pending.get(packet.request_id)
                        if (pending) {
                            if ('data' in packet) {
                                pending.stream.next(packet.data)
                            }

                            if ('error' in packet && packet.error != undefined) {
                                pending.finished = true
                                pending.stream.error(packet.error)
                                this.#completePendingRpc(packet.request_id)
                            } else if (packet.completed) {
                                pending.finished = true
                                pending.stream.complete()
                                this.#completePendingRpc(packet.request_id)
                            }
                        }
                    }

                    if (packet?.kind === 'cancel' && packet.target_node_id === this.node_id) {
                        const stream = this.#rpc.running.get(packet.request_id)
                        if (stream) {
                            stream.unsubscribe()
                            this.#completeRunningRpc(packet.request_id)
                        }
                    }
                }

                if (offline) {
                    this.registry?.removePeer(offline)
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

    #linkPubsubTransporter(name: string, transporter: PubsubTransporter) {
        this.#transporters.pubsubs.set(name, transporter)
        this.#ensureLocalTransporterPresence(name)

        return transporter.pipe(
            tap(({ endpoints }) => {
                this.#refresh({
                    transporters: {
                        [name]: endpoints
                    }
                })
            }),
            finalize(() => undefined)
        )
    }

    #linkDiscoveryTransporter(name: string, transporter: DiscoveryTransporter) {
        this.#transporters.discoveries.set(name, transporter)
        this.#ensureLocalTransporterPresence(name)

        const announce = this.#me$.subscribe(metadata => {
            void transporter.broadcast({
                hi: true,
                node: metadata,
                sender_id: this.node_id,
            }).catch(() => undefined)
        })

        return transporter.pipe(
            tap(({ discovered: node }: DiscoveryEvent) => {
                if (!node || typeof node !== 'object' || !('node_id' in node) || !('services' in node)) return
                const rpcTransporterName = this.#resolveDiscoveredRpcTransporterName(node)
                const peer = this.registry?.upsertPeer(
                    rpcTransporterName ? {
                        ...node,
                        transporters: {
                            ...(node.transporters || {}),
                            rpc: rpcTransporterName
                        }
                    } : node
                )

                if (!this.registry || !peer) return
            }),
            finalize(() => {
                announce.unsubscribe()
            })
        )
    }

    #isRpcTransporter(transporter: MeshTransporter): transporter is RpcTransporter {
        return 'send' in transporter
    }

    #isPubsubTransporter(transporter: MeshTransporter): transporter is PubsubTransporter {
        return 'publish' in transporter
    }

    #isDiscoveryTransporter(transporter: MeshTransporter): transporter is DiscoveryTransporter {
        return 'broadcast' in transporter
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

    #resolveDiscoveredRpcTransporterName(node: SpiderMeshNode) {
        for (const [name] of this.#transporters.rpcs.entries()) {
            if (node.transporters && Object.hasOwn(node.transporters, name)) return name
        }
        return null
    }

    #registerTopic(topic: string) {
        if (this.#me$.value.topics.includes(topic)) return

        this.#refresh({
            topics: [...this.#me$.value.topics, topic]
        })
    }

    #unregisterTopic(topic: string) {
        if (!this.#me$.value.topics.includes(topic)) return

        this.#refresh({
            topics: this.#me$.value.topics.filter(item => item !== topic)
        })
    }

    linkEvent<T>(factory: { new(...args: any[]): T }) {
        const topic = factory.name
        const listen$ = new Observable<T>(subscriber => {
            this.#registerTopic(topic)

            const subscription = from(this.#transporters.pubsubs.values()).pipe(
                mergeMap(t => t.listen<T>(topic))
            ).subscribe(subscriber)

            return () => {
                subscription.unsubscribe()
                this.#unregisterTopic(topic)
            }
        }).pipe(share())

        return {
            publish: (data: T) => lastValueFrom(
                from(this.#transporters.pubsubs.values()).pipe(
                    mergeMap(t => t.publish(topic, data))
                ), { defaultValue: undefined as any as void }
            ),
            listen: () => listen$
        }
    }
}