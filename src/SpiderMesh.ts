import { BehaviorSubject, catchError, EMPTY, filter, finalize, firstValueFrom, from, lastValueFrom, map, mergeAll, mergeMap, Observable, of, ReplaySubject, retry, tap, throwError, timeout, timer } from "rxjs"
import { listBeforeMicroserviceOnlineMethods } from "./decorators/BeforeMicroserviceOnline.js";
import { RemoteService } from "./interfaces/RemoteService.js";
import { SpiderMeshNode } from "./interfaces/SpiderMeshNode.js";
import { PubsubTransporter } from "./interfaces/PubsubTransporter.js";
import { RpcTransporter, RpcOptions } from "./interfaces/RpcTransporter.js";
import { MicroserviceException } from "./helpers/MicroserviceException.js";
import { MicroserviceOfflineException } from "./helpers/MicroserviceOfflineException.js";
import { services$ } from "./decorators/Microservice.js";
import { networkInterfaces } from "os";
import { MicroserviceNotFound } from "./helpers/MicroserviceNotFound.js";
import { DiscoveryTransporter } from "./interfaces/DiscoveryTransporter.js";
import { MicroserviceRpcTimeout } from "./helpers/MicroserviceRpcTimeout.js";
import { SPIDERMESH_NAMESPACE } from "../const.js";

export type HelloEvent = SpiderMeshNode & { back?: boolean }
export type ServiceChecker = (nodes: SpiderMeshNode[]) => Promise<boolean> | boolean

export type NodesMap = {
    nodes: Map<string, SpiderMeshNode>,
    last_updated_node_id: string
}

export class SpiderMesh {

    public readonly node_id = `${Date.now()}`
    public readonly namespace = SPIDERMESH_NAMESPACE

    #rpcs = new Map<string, RpcTransporter>()
    #pubsubs = new BehaviorSubject(new Map<string, PubsubTransporter>())
    #discovers = new Map<string, DiscoveryTransporter>()

    #metadata$ = new BehaviorSubject<SpiderMeshNode>({
        ips: Object.values(networkInterfaces()).flat(2).filter(a => !a?.internal && !!a?.address).map(a => a?.address!),
        host: '',
        namespace: SPIDERMESH_NAMESPACE,
        node_id: this.node_id,
        services: {},
        topics: [],
        transporters: {},
        nodes: {},
        version: 0
    })

    #nodes = new BehaviorSubject({
        nodes: new Map<string, SpiderMeshNode & { rpc?: string }>(),
        last_updated_node_id: ''
    })

    #services$ = new BehaviorSubject({
        services: new Map<string, {
            index: number
            nodes: string[]
        }>(),
        last_updated_services: new Set<string>()
    })

    static #transporters$ = new ReplaySubject<RpcTransporter | PubsubTransporter | DiscoveryTransporter>()
    static linkTransporter(t: RpcTransporter | PubsubTransporter | DiscoveryTransporter) {
        SpiderMesh.#transporters$.next(t)
    }

    constructor() {
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

        SpiderMesh.#transporters$.pipe(
            tap(t => this.linkTransporter(t))
        ).subscribe()

    }


    #selectRpcTarget(filters: Partial<Pick<RpcOptions<any>, 'node_id' | 'ip' | 'service'>> = {}) {
        if (!filters.service) return null

        if (filters.node_id) {
            const node = this.#nodes.value.nodes.get(filters.node_id)
            if (!node) return null
            if (!node.services[filters.service]) return null
            if (!node.rpc) return null
            const transporter = this.#rpcs.get(node.rpc)
            if (!transporter) return null
            return { node, transporter }
        }

        const state = this.#services$.value.services.get(filters.service)
        if (!state || state.nodes.length == 0) return null


        const nodes = state.nodes.map(id => {
            const node = this.#nodes.value.nodes.get(id)
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

    callRemoteService<T>(options: RpcOptions<T>) {
        return of(0).pipe(
            mergeMap(async () => {
                await this.waitServiceOnline(options.service)
                const target = this.#selectRpcTarget(options)
                if (!target) throw new MicroserviceOfflineException()
                const force = !!options.node_id || !!options.ip
                return target.transporter.rpc<T>(options, target.node, force)
            }),
            mergeMap($ => $),
            retry({
                delay: (e: Error, count: number) => {
                    if (e instanceof MicroserviceOfflineException || e.message == MicroserviceOfflineException.code) {
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

    linkTransporter(
        transporter: RpcTransporter | PubsubTransporter | DiscoveryTransporter
    ) {
        const transporter_name = Object.getPrototypeOf(transporter).constructor.name

        if (transporter instanceof RpcTransporter) {
            // Is RpcTransporter
            const rpc = transporter
            if (this.#rpcs.has(transporter_name)) return

            this.#rpcs.set(transporter_name, rpc)


            return transporter.link(this.#metadata$, this.#nodes).pipe(
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
                        const node = this.#nodes.value.nodes.get(online)
                        if (node) {
                            node.rpc = transporter_name
                            const services = this.#services$.value.services
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
                            this.#services$.next({
                                services,
                                last_updated_services: new Set(Object.keys(node.services || {}))
                            })
                        }
                    }

                    const offline = e.offline
                    if (offline) {
                        const nodes = this.#nodes.value.nodes
                        const node = nodes.get(offline)



                        if (node) {
                            // Update node list
                            nodes.delete(offline)
                            this.#nodes.next({ nodes, last_updated_node_id: node.node_id })

                            // Update services
                            const services = this.#services$.value.services
                            for (const service of Object.values(node.services)) {
                                const target = services.get(service) || { index: 0, nodes: [] }
                                const nodes = target.nodes.filter(id => id != node.node_id)
                                nodes.length == 0 ? services.delete(service) : services.set(service, { ...target, nodes })
                            }
                            this.#services$.next({
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
                                [transporter_name]: metadata
                            },
                            version: this.#metadata$.value.version + 1
                        })
                    }
                }
                ),
                finalize(() => {
                    this.#rpcs.delete(transporter_name)
                })
            ).subscribe()
        }

        if (transporter instanceof PubsubTransporter) {
            const transporters = this.#pubsubs.getValue()
            transporters.set(transporter_name, transporter)
            this.#pubsubs.next(transporters)

            return transporter.link(this.#metadata$, this.#nodes).pipe(
                map(a => a.metadata),
                filter(Boolean),
                tap(metadata => {
                    this.#metadata$.next({
                        ... this.#metadata$.value,
                        transporters: {
                            ... this.#metadata$.value.transporters,
                            [transporter_name]: metadata
                        },
                        version: this.#metadata$.value.version + 1
                    })
                }),
                finalize(() => {
                    transporters.delete(transporter_name)
                    this.#pubsubs.next(transporters)
                })
            ).subscribe()
        }

        if (transporter instanceof DiscoveryTransporter) {
            // Is discover transporter
            const discover = transporter as DiscoveryTransporter
            this.#discovers.set(transporter_name, discover)
            return discover.link(this.#metadata$).pipe(
                tap(node => {
                    const nodes = this.#nodes.value.nodes
                    nodes.set(node.node_id, {
                        ...nodes.get(node.node_id) || {},
                        ...node
                    })
                    const last_updated_node_id = node.node_id
                    this.#nodes.next({
                        nodes,
                        last_updated_node_id
                    })
                }),
                finalize(() => this.#discovers.delete(transporter_name))
            ).subscribe()

        }
    }


    waitServiceOnline(service: string, check: ServiceChecker = (nodes => nodes.length > 0)) {
        return firstValueFrom(this.#services$.pipe(
            map(() => {
                const targets = this.#services$.value.services.get(service)?.nodes || []
                const nodes = targets.map(id => this.#nodes.value.nodes.get(id)!).filter(Boolean)
                return nodes
            }),
            mergeMap(async targets => check(targets)),
            filter(Boolean)
        ))
    }

    linkRemoteService<T>(factory: { new(...args: any[]): T }) {

        const service = factory.name

        const omitProperties = new Set([
            'caller',
            'callee',
            'arguments',
            'constructor',
            '__defineGetter__',
            '__defineSetter__',
            'hasOwnProperty',
            '__lookupGetter__',
            '__lookupSetter__',
            'isPrototypeOf',
            'propertyIsEnumerable',
            'toString',
            'valueOf',
            'toLocaleString',
            '__proto__',
            'onModuleInit',
            'onApplicationBootstrap',
            'onModuleDestroy',
            'beforeApplicationShutdown',
            'onApplicationShutdown'
        ])

        const listRpcNodes = () => {
            const targets = this.#services$.value.services.get(service)
            if (!targets || targets.nodes.length == 0) return []
            return targets.nodes.map(id => {
                const node = this.#nodes.value.nodes.get(id)
                if (node && node.rpc) return node
            }).filter(Boolean).map(node => node!)
        }

        const target = new Proxy({}, {
            get: (_, $: string) => {
                if (omitProperties.has($)) return null

                if ($ == 'wait$') {
                    return (fn?: (nodes: SpiderMeshNode[]) => boolean | Promise<boolean>) => this.waitServiceOnline(service, fn)
                }

                if ($ == 'nodes') return listRpcNodes()

                if ($ == 'watch$') return () => {
                    return this.#services$.pipe(
                        filter((e, index) => {
                            if (index == 0) return true
                            if (e.last_updated_services.has(service)) return true
                            return false
                        }),
                        map(e => e.services.get(service)?.nodes || []),
                        map(targets => targets.map(id => this.#nodes.value.nodes.get(id)!).filter(Boolean))
                    )
                }

                if ($.startsWith('__batch__')) {
                    const method = $.split('__batch__')?.[1]
                    return (...args: any[]) => from(listRpcNodes()).pipe(
                        mergeMap(node => (
                            this.callRemoteService({
                                service,
                                args,
                                method,
                                node_id: node.node_id,
                            }).pipe(
                                map(data => ({ node, data })),
                                catchError(e => of({ node, e }))
                            )
                        ))
                    )
                }

                if ($ == 'set') {
                    return (options: RpcOptions<any>) => new Proxy({}, {
                        get: (_, method: string) => {
                            return (...args: any[]) => {
                                const response = this.callRemoteService<Observable<any>>({
                                    ...options,
                                    args,
                                    method,
                                    service
                                })

                                return Object.assign(response, {
                                    then: async (s: Function, r: Function) => {
                                        try {
                                            s(await firstValueFrom(response))
                                        } catch (e) {
                                            r(e)
                                        }
                                    }
                                })
                            }
                        }
                    })
                }

                return (...args: any[]) => {
                    const response = this.callRemoteService<Observable<any>>({
                        args,
                        method: $,
                        service
                    })

                    return Object.assign(response, {
                        then: async (s: Function, r: Function) => {
                            try {
                                s(await firstValueFrom(response))
                            } catch (e) {
                                r(e)
                            }
                        }
                    })
                }
            }
        }) as RemoteService<T>

        return new Proxy<RemoteService<T>>(target, {
            get(target, p) {
                if (p == 'then') return target
                return (target as any)[p]
            },
        })

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