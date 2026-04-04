import { BehaviorSubject, catchError, EMPTY, filter, finalize, firstValueFrom, from, lastValueFrom, map, merge, mergeAll, mergeMap, Observable, of, retry, tap, throwError, timeout, timer } from "rxjs"
import { listBeforeMicroserviceOnlineMethods } from "./decorators/BeforeMicroserviceOnline.js";
import { MicroserviceException, MicroserviceNotFound, MicroserviceOfflineException, MicroserviceRpcTimeout } from "./helpers/MicroserviceException.js";
import { LOCAL_SERVICES$ } from "./decorators/Microservice.js";
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
        topics: [],
        transporters: {},
        nodes: {},
        version: 0,
    })

    #nodes$ = new BehaviorSubject({
        nodes: new Map<string, SpiderMeshNode & { rpc?: string }>(),
        last_updated_node_id: ''
    })
    #local_services = new Map<string, any>()
    #remote_services$ = new BehaviorSubject(new Map<string, {
        index: number
        nodes: string[]
    }>())

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
        const targets = this.#remote_services$.value.get(service)
        if (!targets || targets.nodes.length == 0) return []
        return targets.nodes.map(id => {
            const node = this.#nodes$.value.nodes.get(id)
            if (node && node.rpc) return node
        }).filter(Boolean).map(node => node!)
    }

    watchService(service: string) {
        return this.#nodes$.pipe(
            filter(e => {
                if (!e.last_updated_node_id) return true
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

        const state = this.#remote_services$.value.get(filters.service)
        if (!state || state.nodes.length == 0) return null


        const nodes = state.nodes.map(id => {
            const node = this.#nodes$.value.nodes.get(id)
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
                await firstValueFrom(this.watchService(options.service).pipe(
                    options.timeout ? timeout(options.timeout) : tap(),
                    filter(nodes => nodes.length > 0)
                ), { defaultValue: null })

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


        return from(this.options.transporters).pipe(
            mergeMap(F => {
                const name = F.name
                const transporter = new F()

                if ('rpc' in transporter) {
                    if (this.#rpcs.has(name)) return EMPTY
                    this.#rpcs.set(name, transporter as RpcTransporter)
                    return transporter.link(this.#metadata$, this.#nodes$).pipe(
                        map(({ rpc, online, offline, metadata }) => {
                            if (rpc) {
                                try {
                                    const service = this.#local_services.get(rpc.service)
                                    const instance = service?.instance
                                    if (!instance) return rpc.callback(throwError(() => new MicroserviceNotFound()))
                                    const response = instance[rpc.method](...rpc.args)
                                    rpc.callback(response)
                                } catch (err) {
                                    rpc.callback(throwError(() => err))
                                }
                            }

                            if (offline) {
                                const nodes = this.#nodes$.value.nodes
                                const node = nodes.get(offline)

                                if (node) {
                                    // Update node list
                                    nodes.delete(offline)
                                    this.#nodes$.next({ nodes, last_updated_node_id: `offline:${Object.keys(node.services).join(',')}` })

                                    // Update services
                                    const services = this.#remote_services$.value
                                    for (const service of Object.values(node.services)) {
                                        const target = services.get(service) || { index: 0, nodes: [] }
                                        const nodes = target.nodes.filter(id => id != node.node_id)
                                        nodes.length == 0 ? services.delete(service) : services.set(service, { ...target, nodes })
                                    }
                                    this.#remote_services$.next(services)
                                }
                            }

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
                        }),
                        finalize(() => {
                            this.#rpcs.delete(name)
                        })
                    )
                }

                if ('publish' in transporter) {
                    const transporters = this.#pubsubs.getValue()
                    transporters.set(name, transporter)
                    this.#pubsubs.next(transporters)
                    return transporter.link(this.#metadata$, this.#nodes$).pipe(
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
                }

                if ('broadcast' in transporter) {
                    this.#discovers.set(name, transporter)
                    return transporter.link(this.#metadata$, this.#nodes$).pipe(
                        tap(node => {
                            const nodes = this.#nodes$.value.nodes
                            nodes.set(node.node_id, {
                                ...nodes.get(node.node_id) || {},
                                ...node
                            })
                            this.#nodes$.next({
                                nodes,
                                last_updated_node_id: node.node_id
                            })
                            const services = this.#remote_services$.value
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
                            this.#remote_services$.next(services)

                        }),
                        finalize(() => this.#discovers.delete(name))
                    )
                }

                return EMPTY
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