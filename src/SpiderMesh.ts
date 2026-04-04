import { BehaviorSubject, catchError, EMPTY, filter, finalize, firstValueFrom, from, lastValueFrom, map, merge, mergeAll, mergeMap, Observable, of, retry, tap, throwError, timeout, timer } from "rxjs"
import { listBeforeMicroserviceOnlineMethods } from "./decorators/BeforeMicroserviceOnline.js";
import { MicroserviceException, MicroserviceNotFound, MicroserviceOfflineException, MicroserviceRpcTimeout } from "./helpers/MicroserviceException.js";
import { services$ } from "./decorators/Microservice.js";
import { SPIDERMESH_NAMESPACE, SPIDERMESH_NODE_HOSTNAME } from "../const.js";
import { AllIpAddresses } from "./helpers/GetIps.js";
import { SpiderMeshNode, RpcTransporter, PubsubTransporter, DiscoveryTransporter, RpcOptions } from '@spider-mesh/types'

export type HelloEvent = SpiderMeshNode & { back?: boolean }
export type ServiceChecker = (nodes: SpiderMeshNode[]) => Promise<boolean> | boolean

export type NodesMap = {
    nodes: Map<string, SpiderMeshNode>,
    last_updated_node_id: string
}

export type SpiderMeshOptions = {
    transporters: Array<{ new(): RpcTransporter | PubsubTransporter | DiscoveryTransporter }>
}

export class SpiderMesh {

    public readonly node_id = `${Date.now()}`
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
        topics: [],
        transporters: {},
        nodes: {},
        version: 0
    })

    public readonly nodes$ = new BehaviorSubject({
        nodes: new Map<string, SpiderMeshNode & { rpc?: string }>(),
        last_updated_node_id: ''
    })

    public readonly services$ = new BehaviorSubject({
        services: new Map<string, {
            index: number
            nodes: string[]
        }>(),
        last_updated_services: new Set<string>()
    })

    constructor(private options: SpiderMeshOptions) {
        if (!options) throw new Error(`Missing options for SpiderMesh, please using like that new SpiderMeshOptions({ ... options})`)
        services$.pipe(
            filter(list => Object.keys(list).length > 0),
            mergeMap(async services => {
                for (const { instance } of Object.values(services)) {
                    const list = listBeforeMicroserviceOnlineMethods(instance)
                    for (const method of list) {
                        await instance[method]()
                    }
                }
                const metadata = {
                    ... this.#metadata$.value,
                    services: Object.entries(services).reduce((p, [name, { metadata }]) => {
                        return {
                            ...p,
                            [name]: metadata
                        }
                    }, {} as { [name: string]: any }),
                    version: this.#metadata$.value.version + 1
                }
                this.#metadata$.next(metadata)

                return services
            }),
            catchError(e => EMPTY)
        ).subscribe()

        this.#linkTransporters()

    }

    static asProvider(options: SpiderMeshOptions) {
        return {
            provide: this,
            useFactory: async () => new this(options)
        }
    }


    #selectRpcTarget(filters: Partial<Pick<RpcOptions<any>, 'node_id' | 'ip' | 'service'>> = {}) {
        if (!filters.service) return null

        if (filters.node_id) {
            const node = this.nodes$.value.nodes.get(filters.node_id)
            if (!node) return null
            if (!node.services[filters.service]) return null
            if (!node.rpc) return null
            const transporter = this.#rpcs.get(node.rpc)
            if (!transporter) return null
            return { node, transporter }
        }

        const state = this.services$.value.services.get(filters.service)
        if (!state || state.nodes.length == 0) return null


        const nodes = state.nodes.map(id => {
            const node = this.nodes$.value.nodes.get(id)
            const transporter = node && node.rpc && this.#rpcs.get(node.rpc)
            if (node && transporter) {
                if (filters.ip) {
                    if (!node.ips.includes(filters.ip!)) return null
                }
                return { node, transporter }
            }
        }).filter(Boolean).map(e => e!!)

        return nodes[state.index++ % nodes.length]
    }

    callRemoteService<R, T>(options: RpcOptions<T>) {
        return of(0).pipe(
            mergeMap(async () => {
                await this.waitServiceOnline(options.service, undefined, options.timeout ? timer(options.timeout) : EMPTY)
                const target = this.#selectRpcTarget(options)
                if (!target) throw new MicroserviceOfflineException()
                const force = !!options.node_id || !!options.ip
                return target.transporter.rpc<R, T>(options, target.node, force)
            }),
            mergeMap($ => $),
            retry({
                delay: (e: Error, count: number) => {
                    if (e instanceof MicroserviceOfflineException) {
                        if (options.retry && count < options.retry) return timer(1000)
                    }
                    throw e
                }
            }),
            options.timeout ? timeout({
                each: options.timeout,
                with: () => throwError(() => new MicroserviceRpcTimeout())
            }) : tap(),
            catchError(e => {
                if (options.fallback != undefined) return of(options.fallback as any as T)
                if (e.code) {
                    const error = new MicroserviceException(e)
                    error.stack = e.stack
                    throw error
                }
                throw e
            })
        )
    }



    #linkTransporters() {
        const transporters = this.options.transporters.map(t => {
            const name = t.name
            const transporter = new t()
            const rpc = !!(transporter as RpcTransporter).rpc ? transporter as RpcTransporter : null
            const pubsub = !!(transporter as PubsubTransporter).publish ? transporter as PubsubTransporter : null
            const discovery = !!(transporter as DiscoveryTransporter).broadcast ? transporter as DiscoveryTransporter : null
            return {
                name,
                rpc,
                pubsub,
                discovery
            }
        })
        return merge(
            from(transporters).pipe(
                map(({ name, rpc }) => {
                    if (!rpc) return EMPTY
                    if (this.#rpcs.has(name)) return
                    this.#rpcs.set(name, rpc)
                    return rpc.link(this.#metadata$, this.nodes$).pipe(
                        map(e => {
                            const rpc = e.rpc
                            if (rpc) {
                                try {
                                    const service = services$.value[rpc.service]
                                    const instance = service?.instance
                                    if (!instance) return rpc.callback(throwError(() => new MicroserviceNotFound()))
                                    const response = instance[rpc.method](...rpc.args)
                                    rpc.callback(response)
                                } catch (err) {
                                    rpc.callback(throwError(() => err))
                                }
                            }

                            const online = e.online
                            if (online) {
                                const node = this.nodes$.value.nodes.get(online)
                                if (node) {
                                    node.rpc = name
                                    const services = this.services$.value.services
                                    for (const service of Object.keys(node.services || {})) {
                                        const target = services.get(service) || { index: 0, nodes: [] }
                                        services.set(service, {
                                            index: target.index,
                                            nodes: [
                                                ...target.nodes.filter(id => id != node.node_id),
                                                node.node_id
                                            ]
                                        })
                                    }
                                    this.services$.next({
                                        services,
                                        last_updated_services: new Set(Object.keys(node.services || {}))
                                    })
                                }
                            }

                            const offline = e.offline
                            if (offline) {
                                const nodes = this.nodes$.value.nodes
                                const node = nodes.get(offline)



                                if (node) {
                                    // Update node list
                                    nodes.delete(offline)
                                    this.nodes$.next({ nodes, last_updated_node_id: node.node_id })

                                    // Update services
                                    const services = this.services$.value.services
                                    for (const service of Object.values(node.services)) {
                                        const target = services.get(service) || { index: 0, nodes: [] }
                                        const nodes = target.nodes.filter(id => id != node.node_id)
                                        nodes.length == 0 ? services.delete(service) : services.set(service, { ...target, nodes })
                                    }
                                    this.services$.next({
                                        services,
                                        last_updated_services: new Set(Object.keys(node.services || {}))
                                    })
                                }
                            }

                            const metadata = e.metadata
                            if (metadata) {
                                this.#metadata$.next({
                                    ... this.#metadata$.value,
                                    transporters: {
                                        ... this.#metadata$.value.transporters,
                                        [name]: metadata
                                    },
                                    version: this.#metadata$.value.version + 1
                                })
                            }
                        }
                        ),
                        finalize(() => {
                            this.#rpcs.delete(name)
                        })
                    )
                })
            ),

            from(transporters).pipe(
                mergeMap(({ name, pubsub: transporter }) => {
                    if (!transporter) return EMPTY
                    const transporters = this.#pubsubs.getValue()
                    transporters.set(name, transporter)
                    this.#pubsubs.next(transporters)
                    return transporter.link(this.#metadata$, this.nodes$).pipe(
                        map(a => a.metadata),
                        filter(Boolean),
                        tap(metadata => {
                            this.#metadata$.next({
                                ... this.#metadata$.value,
                                transporters: {
                                    ... this.#metadata$.value.transporters,
                                    [name]: metadata
                                },
                                version: this.#metadata$.value.version + 1
                            })
                        }),
                        finalize(() => {
                            transporters.delete(name)
                            this.#pubsubs.next(transporters)
                        })
                    )
                })
            ),


            from(transporters).pipe(
                mergeMap(({ name, discovery }) => {
                    if (!discovery) return EMPTY
                    this.#discovers.set(name, discovery)
                    return discovery.link(this.#metadata$).pipe(
                        tap(node => {
                            const nodes = this.nodes$.value.nodes
                            nodes.set(node.node_id, {
                                ...nodes.get(node.node_id) || {},
                                ...node
                            })
                            const last_updated_node_id = node.node_id
                            this.nodes$.next({
                                nodes,
                                last_updated_node_id
                            })
                        }),
                        finalize(() => this.#discovers.delete(name))
                    )
                })
            )
        ).subscribe()
    }


    waitServiceOnline(service: string, check: ServiceChecker = (nodes => nodes.length > 0), stop$: Observable<any>) {
        return firstValueFrom(
            merge(
                stop$.pipe(map(() => false)),
                this.services$.pipe(
                    map(() => {
                        const targets = this.services$.value.services.get(service)?.nodes || []
                        const nodes = targets.map(id => this.nodes$.value.nodes.get(id)!).filter(Boolean)
                        return nodes
                    }),
                    mergeMap(async targets => check(targets)),
                    filter(Boolean)
                )
            )
        )
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

    async exposeLocalService(name: string, instance: any, metadata: any) {
        services$.next({
            ...services$.value,
            [name]: {
                instance,
                metadata,
                name
            }
        })
    }

}  