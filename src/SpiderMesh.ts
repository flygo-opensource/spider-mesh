import { BehaviorSubject, catchError, EMPTY, filter, finalize, from, lastValueFrom, map, mergeAll, mergeMap, NEVER, Observable, of, retry, Subject, Subscription, take, tap, throwError, timeout, timer } from "rxjs"
import { listBeforeMicroserviceOnlineMethods } from "./decorators/BeforeMicroserviceOnline.js";
import { LOCAL_SERVICES$ } from "./decorators/Microservice.js";
import { SPIDERMESH_NAMESPACE, SPIDERMESH_NODE_HOSTNAME } from "../const.js";
import { AllIpAddresses } from "./helpers/GetIps.js";
import { SpiderMeshNode, RpcTransporter, PubsubTransporter, DiscoveryTransporter, RpcOptions, SpiderMeshError, RpcEvent, RpcPacket, RpcRequestPacket, RpcResponsePacket, RpcCancelPacket, DiscoveryEvent } from './types.js'

export type HelloEvent = SpiderMeshNode & { back?: boolean }
export type ServiceChecker = (nodes: SpiderMeshNode[]) => Promise<boolean> | boolean

export type NodesMap = {
    nodes: Map<string, SpiderMeshNode>,
    last_updated_node_id: string
}

export type SpiderMeshOptions = {
    transporters: Array<
        { new(): RpcTransporter | PubsubTransporter | DiscoveryTransporter }
        | RpcTransporter
        | PubsubTransporter
        | DiscoveryTransporter
    >
}

type PendingRpcStream = {
    stream: Subject<any>
    finished: boolean
}

const isSubscribable = (value: unknown): value is Observable<unknown> => {
    return !!value && typeof value === 'object' && typeof (value as { subscribe?: unknown }).subscribe === 'function'
}

export class SpiderMesh {

    public readonly node_id = `${Date.now().toString(36).toUpperCase()}`
    public readonly namespace = SPIDERMESH_NAMESPACE

    #rpcs = new Map<string, RpcTransporter>()
    #pubsubs = new BehaviorSubject(new Map<string, PubsubTransporter>())
    #discovers = new Map<string, DiscoveryTransporter>()

    #metadata$ = new BehaviorSubject<SpiderMeshNode>({
        ips: AllIpAddresses,
        host: SPIDERMESH_NODE_HOSTNAME,
        namespace: SPIDERMESH_NAMESPACE,
        node_id: this.node_id,
        services: {},
        transporters: {},
        nodes: {},
        version: 0,
    })

    #nodes$ = new BehaviorSubject({
        nodes: new Map<string, SpiderMeshNode & { rpc?: string }>(),
        last_updated_node_id: ''
    })
    #local_services = new Map<string, any>()
    #rpc = {
        indexes: new Map<string, number>(),
        pending: new Map<string, PendingRpcStream>(),
        running: new Map<string, Subscription>()
    }

    constructor(private options: SpiderMeshOptions) {
        if (!options) throw new Error(`Missing options for SpiderMesh, please using like that new SpiderMeshOptions({ ... options})`)
        setTimeout(() => {
            LOCAL_SERVICES$.pipe(
                mergeMap(async service => {
                    const list = listBeforeMicroserviceOnlineMethods(service.instance)
                    for (const method of list) {
                        await service.instance[method]()
                    }
                    this.#local_services.set(service.name, service.instance)
                    const metadata = {
                        ... this.#metadata$.value,
                        services: {
                            ... this.#metadata$.value.services,
                            [service.name]: service.metadata
                        },
                        version: this.#metadata$.value.version + 1
                    }
                    this.#metadata$.next(metadata)
                }, 1),
                catchError(e => EMPTY)
            ).subscribe()

            this.#linkTransporters()
        }, 0)

    }

    static asProvider(options: SpiderMeshOptions) {
        return {
            provide: this,
            useFactory: async () => new this(options)
        }
    }

    listRpcNodes(service: string) {
        return [...this.#nodes$.value.nodes.values()].filter(node => {
            return !!node.rpc && node.services[service] != undefined
        })
    }

    watchService(service: string) {
        return this.#nodes$.pipe(
            filter((e, i) => {
                if (i == 0 || !e.last_updated_node_id) return true
                if (e.last_updated_node_id.startsWith('offline:')) {
                    return e.last_updated_node_id.split(':')[1].includes(service)
                }
                const node = e.nodes.get(e.last_updated_node_id)
                return node ? node.services[service] != undefined : false
            }),
            map(e => [...e.nodes.values()].filter(node => node.services[service] != undefined))
        )
    }


    #selectRpcTarget(filters: Partial<Pick<RpcOptions<any>, 'node_id' | 'ip' | 'service'>> = {}) {
        if (!filters.service) return null

        if (filters.node_id) {
            const node = this.#nodes$.value.nodes.get(filters.node_id)
            if (!node) return null
            if (!node.services[filters.service]) return null
            if (!node.rpc) return null
            const transporter = this.#rpcs.get(node.rpc)
            if (!transporter) return null
            return { node, transporter }
        }

        const nodes = this.listRpcNodes(filters.service).map(node => {
            const transporter = node.rpc && this.#rpcs.get(node.rpc)
            if (transporter) {
                if (filters.ip && !node.ips.includes(filters.ip)) return null
                return { node, transporter }
            }
            return null
        }).filter(Boolean).map(node => node!)

        if (nodes.length === 0) return null

        const index = this.#rpc.indexes.get(filters.service) || 0
        this.#rpc.indexes.set(filters.service, (index + 1) % nodes.length)
        return nodes[index % nodes.length]
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

    #sendRpcPacket(transporter: RpcTransporter, node: SpiderMeshNode, packet: RpcPacket) {
        return transporter.send(packet, node)
    }

    #resolveRpcNode(node_id: string): SpiderMeshNode {
        const known = this.#nodes$.value.nodes.get(node_id)
        if (known) {
            return known
        }

        return {
            node_id,
            namespace: this.namespace,
            host: '',
            ips: [],
            version: 0,
            services: {},
            nodes: {},
            transporters: {}
        }
    }

    #completePendingRpc(request_id: string) {
        this.#rpc.pending.delete(request_id)
    }

    #completeRunningRpc(request_id: string) {
        this.#rpc.running.delete(request_id)
    }

    callRemoteService<R, T>(options: RpcOptions<T>) {

        return this.#nodes$.pipe(
            map(() => this.#selectRpcTarget(options)),
            filter(Boolean),
            take(1),
            mergeMap(target => {
                return new Observable<R>(subscriber => {
                    // Each subscription maps to one in-flight remote stream.
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

                    this.#sendRpcPacket(target.transporter, target.node, {
                        kind: 'request',
                        request_id,
                        source_node_id: this.node_id,
                        target_node_id: target.node.node_id,
                        service: options.service,
                        method: options.method,
                        args: options.args
                    }).catch(error => {
                        pending.finished = true
                        pending.stream.error(this.#normalizeRpcError(error))
                        this.#completePendingRpc(request_id)
                    })

                    return () => {
                        subscription.unsubscribe()
                        if (!pending.finished && this.#rpc.pending.has(request_id)) {
                            // Tell the remote side to stop emitting if the caller leaves early.
                            void this.#sendRpcPacket(target.transporter, target.node, {
                                kind: 'cancel',
                                request_id,
                                source_node_id: this.node_id,
                                target_node_id: target.node.node_id,
                            }).catch(() => undefined)
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
        if (this.#rpcs.has(name)) return EMPTY
        this.#rpcs.set(name, transporter)

        const initialEndpoints = (transporter as RpcTransporter & { metadata?: RpcEvent['endpoints'] }).metadata
        if (initialEndpoints) {
            this.#metadata$.next({
                ... this.#metadata$.value,
                transporters: {
                    ... this.#metadata$.value.transporters,
                    [name]: initialEndpoints
                },
                version: this.#metadata$.value.version + 1
            })
        }

        return transporter.pipe(
            map(({ rpc, offline, endpoints }) => {
                if (rpc) {
                    const packet = rpc.packet

                    if (packet?.kind === 'request') {
                        if (packet.target_node_id === this.node_id) {
                            const reply = async (response: Omit<RpcResponsePacket, 'kind' | 'request_id' | 'source_node_id' | 'target_node_id'>) => {
                                await this.#sendRpcPacket(transporter, this.#resolveRpcNode(rpc.node_id), {
                                    kind: 'response',
                                    request_id: packet.request_id,
                                    source_node_id: this.node_id,
                                    target_node_id: rpc.node_id,
                                    ...response
                                })
                            }

                            const handleResponse = (response: any) => {
                                if (isSubscribable(response)) {
                                    let queue = Promise.resolve()
                                    // Preserve event ordering while forwarding a remote observable stream.
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

                            const service = this.#local_services.get(packet.service)
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

                    if (packet?.kind === 'cancel') {
                        if (packet.target_node_id === this.node_id) {
                            const stream = this.#rpc.running.get(packet.request_id)
                            if (stream) {
                                stream.unsubscribe()
                                this.#completeRunningRpc(packet.request_id)
                            }
                        }
                    }
                }

                if (offline) {
                    const nodes = this.#nodes$.value.nodes
                    const node = nodes.get(offline)

                    if (node) {
                        // Update node list
                        nodes.delete(offline)
                        this.#nodes$.next({ nodes, last_updated_node_id: `offline:${Object.keys(node.services).join(',')}` })
                    }
                }

                if (endpoints) {
                    this.#metadata$.next({
                        ... this.#metadata$.value,
                        transporters: {
                            ... this.#metadata$.value.transporters,
                            [name]: endpoints
                        },
                        version: this.#metadata$.value.version + 1
                    })
                }
            }),
            finalize(() => {
                this.#rpcs.delete(name)
            })
        )
    }

    #linkPubsubTransporter(name: string, transporter: PubsubTransporter) {
        const transporters = this.#pubsubs.getValue()
        transporters.set(name, transporter)
        this.#pubsubs.next(transporters)

        return NEVER.pipe(
            finalize(() => {
                transporters.delete(name)
                this.#pubsubs.next(transporters)
            })
        )
    }

    #linkDiscoveryTransporter(name: string, transporter: DiscoveryTransporter) {
        this.#discovers.set(name, transporter)

        const announce = this.#metadata$.subscribe(metadata => {
            void transporter.broadcast({
                hi: true,
                node: metadata,
                sender_id: this.node_id,
            }, metadata.ips).catch(() => undefined)
        })

        return transporter.pipe(
            tap(({ discovered: node }: DiscoveryEvent) => {
                if (!node || typeof node !== 'object' || !('node_id' in node) || !('services' in node)) return
                const nodes = this.#nodes$.value.nodes
                const rpc = Object.keys(node.transporters || {}).find(key => this.#rpcs.has(key));
                nodes.set(node.node_id, {
                    ...nodes.get(node.node_id) || {},
                    ...node,
                    rpc
                })
                this.#nodes$.next({
                    nodes,
                    last_updated_node_id: node.node_id
                })

            }),
            finalize(() => {
                announce.unsubscribe()
                this.#discovers.delete(name)
            })
        )
    }

    #linkTransporters() {


        return from(this.options.transporters).pipe(
            mergeMap(entry => {
                const transporter = typeof entry === 'function' ? new entry() : entry
                const name = typeof entry === 'function'
                    ? entry.name
                    : transporter.constructor?.name || 'AnonymousTransporter'
                const links: Observable<unknown>[] = []

                if ('send' in transporter) {
                    links.push(this.#linkRpcTransporter(name, transporter as RpcTransporter))
                }

                if ('publish' in transporter) {
                    links.push(this.#linkPubsubTransporter(name, transporter as PubsubTransporter))
                }

                if ('broadcast' in transporter) {
                    links.push(this.#linkDiscoveryTransporter(name, transporter as DiscoveryTransporter))
                }

                if (links.length === 0) return EMPTY
                if (links.length === 1) return links[0]
                return from(links).pipe(mergeAll())
            })
        ).subscribe()

    }

    linkEvent<T>(factory: { new(...args: any[]): T }) {
        const topic = factory.name
        return {
            publish: (data: T) => lastValueFrom(
                from(this.#pubsubs.getValue().values()).pipe(
                    mergeMap(t => t.publish(topic, data))
                ), { defaultValue: undefined as any as void }
            ),
            listen: () => {
                return this.#pubsubs.pipe(
                    map(list => [...list.values()]),
                    mergeAll(),
                    mergeMap(t => t.listen<T>(topic))
                )
            }
        }
    }


}  