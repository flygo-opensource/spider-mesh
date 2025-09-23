import { BehaviorSubject, catchError, EMPTY, filter, finalize, firstValueFrom, from, lastValueFrom, map, merge, mergeAll, mergeMap, Observable, of, retry, tap, throwError, timer, toArray } from "rxjs"
import { listBeforeMicroserviceOnlineMethods } from "./decorators/BeforeMicroserviceOnline.js";
import { RemoteService } from "./interfaces/RemoteService.js";
import { SpiderMeshNode } from "./interfaces/SpiderMeshNode.js";
import { PubsubTransporter } from "./interfaces/PubsubTransporter.js";
import { RpcTransporter, RpcOptions } from "./interfaces/RpcTransporter.js";
import { MicroserviceException } from "./helpers/MicroserviceException.js";
import { MicroserviceOfflineException } from "./helpers/MicroserviceOfflineException.js";
import { services$ } from "./decorators/Microservice.js";
import { networkInterfaces } from "os";
import { randomUUID } from "./helpers/randomUUID.js";
import { MicroserviceNotFound } from "./helpers/MicroserviceNotFound.js";
import { DiscoveryTransporter } from "./interfaces/DiscoveryTransporter.js";
import { SPIDERMESH_NAMESPACE } from "const.js";

export type HelloEvent = SpiderMeshNode & { back?: boolean }
export type ServiceChecker = (nodes: SpiderMeshNode[]) => Promise<boolean> | boolean
export type NodesMap = {
    nodes: Map<string, SpiderMeshNode>,
    node?: SpiderMeshNode
}


export class SpiderMesh {

    public readonly node_id = randomUUID()

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

    #nodes$ = new BehaviorSubject<NodesMap>({
        nodes: new Map<string, SpiderMeshNode>(),
    })


    // Phân phối đều các node
    #services = new Map<string, {
        index: number
        nodes: Array<{
            id: string
            transporter: string
        }>
    }>()

    constructor() {
        services$.pipe(
            filter(list => Object.keys(list).length > 0),
            mergeMap(async services => {
                for (const v of Object.values(services)) {
                    const metadata = (v as any)['$'] || (
                        typeof v.metadata == 'function' ? await v.metadata() : v.metadata
                    )
                    Object.defineProperty(v, '$', { value: metadata })
                }
                return services
            }),
            tap(services => {
                const metadata = {
                    ... this.#metadata$.value,
                    services: Object.entries(services).reduce((p, [name, v]) => {
                        return {
                            ...p,
                            [name]: (v as any)['$']
                        }
                    }, {} as { [name: string]: any }),
                    version: Date.now()
                }
                this.#metadata$.next(metadata)
            }),
            catchError(e => EMPTY)
        ).subscribe()

    }

    #selectTarget<T>(options: RpcOptions<T>) {
        const nodes = this.#getRpcNodes(options)
        const target = this.#services.get(options.service)
        if (!target) return null
        target.index = (target.index + 1) % target.nodes.length
        return nodes[target.index]
    }

    callRemoteService<T>(options: RpcOptions<T>) {
        return of(0).pipe(
            mergeMap(async () => {
                await this.waitServiceOnline(options.service)
                const target = await this.#selectTarget(options)
                if (!target) throw new MicroserviceOfflineException()
                return target.transporter.rpc<T>(options, target.node)
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

            // sync unknown transporter method
            for (const [service, { nodes }] of this.#services) {
                for (const node of nodes) {
                    if (node.transporter == '?') {
                        const metadata = this.#nodes$.value.nodes.get(node.id)
                        if (metadata && metadata.transporters[transporter_name]) {
                            node.transporter = transporter_name
                        }
                    }
                }
            }

            return merge(
                // Metadata handler
                rpc.pipe(
                    map(a => a.metadata),
                    filter(Boolean),
                    tap(metadata => {
                        this.#metadata$.next({
                            ... this.#metadata$.value,
                            transporters: {
                                ... this.#metadata$.value.transporters,
                                [transporter_name]: metadata
                            }
                        })
                    })
                ),

                // RPC handler
                rpc.pipe(
                    map(a => a.rpc),
                    filter(Boolean),
                    map(({ callback, ...request }) => {
                        try {
                            const service = services$.value[request.service]
                            const instance = service?.instance
                            if (!instance) return callback(throwError(() => new MicroserviceNotFound()))
                            const response = instance[request.method](...request.args)
                            callback(response)
                        } catch (err) {
                            callback(throwError(() => err))
                        }
                    }),
                    finalize(() => {
                        this.#rpcs.delete(transporter_name)
                    })
                ),

                // Offline handler
                rpc.pipe(
                    map(a => a.offline),
                    filter(Boolean),
                    map(node_id => this.#nodes$.value.nodes.get(node_id)),
                    filter(Boolean),
                    tap(node => {
                        for (const service of Object.values(node.services)) {
                            const target = this.#services.get(service) || { index: 0, nodes: [] }
                            const nodes = target.nodes.filter(n => n.id != node.node_id)
                            nodes.length == 0 ? this.#services.delete(service) : this.#services.set(service, { index: 0, nodes })
                        }
                    })
                )

            ).subscribe()
        }

        if (transporter instanceof PubsubTransporter) {
            const transporters = this.#pubsubs.getValue()
            transporters.set(transporter_name, transporter)
            this.#pubsubs.next(transporters)

            return merge(

                // RPC handler
                transporter.pipe(
                    map(a => a.metadata),
                    filter(Boolean),
                    tap(metadata => {
                        this.#metadata$.next({
                            ... this.#metadata$.value,
                            transporters: {
                                ... this.#metadata$.value.transporters,
                                [transporter_name]: metadata
                            }
                        })
                    }),
                    finalize(() => {
                        transporters.delete(transporter_name)
                        this.#pubsubs.next(transporters)
                    })
                )
            ).subscribe()
        }

        if (transporter instanceof DiscoveryTransporter) {
            // Is discover transporter
            const discover = transporter as DiscoveryTransporter
            this.#discovers.set(transporter_name, discover)
            return discover.pipe(
                tap(node => {
                    // Set services
                    Object.keys(node.services || {}).forEach(service => {
                        const target = this.#services.get(service) || { index: 0, nodes: [] }
                        const transporter = [... this.#rpcs.keys()].reverse().find(name => name == node.transporters[name]) || '?'
                        this.#services.set(service, {
                            ...target,
                            nodes: [
                                ...target.nodes.filter(n => n.id != node.node_id),
                                { id: node.node_id, transporter },
                            ]
                        })
                    })
                    const nodes = this.#nodes$.getValue().nodes
                    nodes.set(node.node_id, node)
                    this.#nodes$.next({ nodes, node })
                }),
                finalize(() => this.#discovers.delete(transporter_name))
            ).subscribe()
        }
    }

    #getRpcNodes(filters: Partial<Pick<RpcOptions<any>, 'node_id' | 'ip' | 'service'>> = {}) {
        if (!filters.service) return []

        if (filters.node_id) {
            const node = this.#nodes$.getValue().nodes.get(filters.node_id)
            if (!node) return []
            if (!node.services[filters.service]) return []
            const transporter = [... this.#rpcs.entries()].find(([name, _]) => name == node?.transporters[name])?.[1]
            if (!transporter) return []
            return [{ node, transporter }]
        }

        const targets = this.#services.get(filters.service)
        if (!targets || targets.nodes.length == 0) return []


        const nodes = targets.nodes.map(({ id, transporter: transporter_name }) => {
            const node = this.#nodes$.getValue().nodes.get(id)
            const transporter = this.#rpcs.get(transporter_name)
            if (node && transporter) return { node, transporter }
        }).filter(Boolean).map(e => e!!)

        if (filters.ip) return nodes.filter(e => e.node.ips.includes(filters.ip!))

        return nodes
    }

    waitServiceOnline(service: string, check: ServiceChecker = (nodes => nodes.length > 0)) {
        return firstValueFrom(this.#nodes$.pipe(
            map(() => this.#getRpcNodes({ service })),
            mergeMap(async targets => check(targets.map(t => t.node))),
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

        const target = new Proxy({}, {
            get: (_, $: string) => {

                if (omitProperties.has($)) return null

                if ($ == 'wait$') {
                    return (fn?: (nodes: SpiderMeshNode[]) => boolean | Promise<boolean>) => this.waitServiceOnline(service, fn)
                }

                if ($ == 'nodes') return this.#getRpcNodes({ service })

                if ($ == 'watch$') return (fn: ServiceChecker = (nodes => nodes.length > 0)) => {
                    return this.waitServiceOnline(service, fn)
                }

                if ($.startsWith('batch__')) {
                    const method = $.split('__batch__')?.[1]
                    const nodes = this.#getRpcNodes({ service })
                    return (...args: any[]) => {
                        const $ = from(nodes).pipe(
                            mergeMap(({ node }) => (
                                this.callRemoteService({
                                    args,
                                    method,
                                    service,
                                    node_id: node.node_id,
                                }).pipe(
                                    map(data => ({ node, data })),
                                    catchError(e => of({ node, e }))
                                )
                            ))
                        )
                        return Object.assign($, {
                            then: async (s: Function, r: Function) => {
                                try {
                                    const arr = await lastValueFrom($.pipe(toArray()), { defaultValue: [] })
                                    s(arr)
                                } catch (e) {
                                    r(e)
                                }
                            }
                        })
                    }
                }

                if ($ == 'set') {
                    return (options: RpcOptions<any>) => new Proxy({}, {
                        get: (_, method: string) => {
                            return (...args: any[]) => this.callRemoteService({
                                ...options,
                                args,
                                method,
                                service
                            })
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
        const context = this.#nodes$.value.nodes
        return {
            publish: (data: T) => lastValueFrom(
                from(this.#pubsubs.getValue().values()).pipe(
                    mergeMap(t => t.publish(topic, data, context))
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

    async exposeLocalService(name: string, instance: any) {
        const hooks = listBeforeMicroserviceOnlineMethods(instance)
        for (const method of hooks) await instance[method]
        services$.next({
            ...services$.value,
            [name]: {
                instance,
                metadata: {},
                name
            }
        })
    }

}  