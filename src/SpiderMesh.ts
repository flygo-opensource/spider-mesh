import { BehaviorSubject, catchError, distinctUntilKeyChanged, EMPTY, filter, finalize, firstValueFrom, from, lastValueFrom, map, merge, mergeAll, mergeMap, Observable, of, retry, tap, throwError, timer, toArray } from "rxjs"
import { listBeforeMicroserviceOnlineMethods } from "./decorators/BeforeMicroserviceOnline.js";
import { RemoteService } from "./interfaces/RemoteService.js";
import { SpiderMeshNode } from "./interfaces/SpiderMeshNode.js";
import { NAMEPSACE } from "./const.js";
import { PubsubTransporter } from "./interfaces/PubsubTransporter.js";
import { RpcTransporter, RpcOptions } from "./interfaces/RpcTransporter.js";
import { MicroserviceException } from "./helpers/MicroserviceException.js";
import { MicroserviceOfflineException } from "./helpers/MicroserviceOfflineException.js";
import { services$ } from "./decorators/Microservice.js";
import { networkInterfaces } from "os";
import { randomUUID } from "./helpers/randomUUID.js";
import { MicroserviceNotFound } from "./helpers/MicroserviceNotFound.js";
import { DiscoveryTransporter } from "./interfaces/DiscoveryTransporter.js";

export type HelloEvent = SpiderMeshNode & { back?: boolean }


export class SpiderMesh {

    public readonly node_id = randomUUID()
    public readonly namespace = NAMEPSACE
    #rpcs = new Map<string, RpcTransporter>()
    #pubsubs = new BehaviorSubject(new Map<string, PubsubTransporter>())
    #discovers = new Map<string, DiscoveryTransporter>()

    #metadata$ = new BehaviorSubject<SpiderMeshNode>({
        ips: Object.values(networkInterfaces()).flat(2).filter(a => !a?.internal && !!a?.address).map(a => a?.address!),
        host: '',
        namespace: this.namespace,
        node_id: this.node_id,
        services: {},
        topics: [],
        transporters: {},
        nodes: {},
        version: 0
    })

    #nodes$ = new BehaviorSubject(new Map<string, SpiderMeshNode>())
    #services = new Map<string, Set<string>>()


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
            catchError(e => {
                return EMPTY
            })
        ).subscribe()

    }


    callRemoteService<T>(options: RpcOptions) {
        return of(0).pipe(
            mergeMap(async () => {
                while (true) {
                    const ids = this.#services.get(options.service) || new Set<string>
                    if (options.node_id && !ids.has(options.node_id)) throw new MicroserviceOfflineException()
                    const nodes = [...ids].map(id => this.#nodes$.value.get(id)!).filter(Boolean)
                    if (nodes.length == 0) {
                        await this.waitServiceOnline(options.service)
                        continue
                    }
                    const node = nodes[0]
                    this.#services.set(options.service, new Set(
                        ...[...ids].slice(1),
                        node.node_id
                    ))
                    const transporter = this.#rpcs.get(`${node.transporters.rpc}`)
                    if (!transporter) throw new MicroserviceOfflineException()
                    return transporter.rpc<T>(options, node)
                }
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
        transporter: RpcTransporter | PubsubTransporter | DiscoveryTransporter,
        metadata: { [key: string]: string | number | boolean }
    ) {

        this.#metadata$.next({
            ... this.#metadata$.value,
            transporters: {
                ... this.#metadata$.value.transporters,
                [transporter.name]: metadata
            }
        })

        if (transporter.name.startsWith('rpc-')) {
            const rpc = transporter as RpcTransporter
            if (this.#rpcs.has(rpc.name)) return
            this.#rpcs.set(rpc.name, rpc)
            return merge(

                // rpc handler
                rpc.requests$.pipe(
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
                        this.#rpcs.delete(rpc.name)
                    })
                ),

                // offline handler
                rpc.offline$.pipe(
                    map(node_id => this.#nodes$.getValue().get(node_id)),
                    filter(Boolean),
                    tap(node => {
                        for (const service of Object.values(node.services)) {
                            const nodes = this.#services.get(service)
                            if (nodes) {
                                nodes.delete(node.node_id)
                                this.#services.set(service, nodes)
                            }

                        }
                    })
                )
            ).subscribe()
        }

        // Is PubsubTransporter
        if (transporter.name.startsWith('pubsub-')) {
            const pubsub = transporter as PubsubTransporter
            const transporters = this.#pubsubs.getValue()
            transporters.set(pubsub.name, pubsub)
            this.#pubsubs.next(transporters)
            return new Observable(() => {
                return () => {
                    transporters.delete(pubsub.name)
                    this.#pubsubs.next(transporters)
                }
            }).subscribe()
        }

        // Is discover transporter
        if (transporter.name.startsWith('discover-')) {
            const discover = transporter as DiscoveryTransporter
            this.#discovers.set(discover.name, discover)
            return discover.pipe(
                tap(node => {
                    const nodes = this.#nodes$.getValue()
                    console.log(`Please check after all rpc inited`)
                    // Detect working RPC channel  
                    const rpc = Object.keys(node.transporters).find(id => this.#rpcs.has(id))
                    const mapped = {
                        ...node,
                        transporters: {
                            ...node.transporters,
                            ...rpc ? { rpc } : {}
                        }
                    }
                    nodes.set(node.node_id, mapped)

                    // Set services
                    rpc && Object.keys(node.services || {}).forEach(service => {
                        const ids = this.#services.get(service) || new Set()
                        this.#services.set(service, new Set([...ids, node.node_id]))
                    })
                    this.#nodes$.next(nodes)
                }),
                finalize(() => this.#discovers.delete(discover.name))
            ).subscribe()
        }

    }

    #getNodes(name: string) {
        return [... this.#nodes$.getValue().values()].filter(node => !!node.services[name])
    }


    waitServiceOnline(name: string, check: (nodes: SpiderMeshNode[]) => Promise<boolean> | boolean = nodes => nodes.length > 0) {
        return firstValueFrom(this.#nodes$.pipe(
            map(() => this.#getNodes(name)),
            mergeMap(async nodes => check(nodes)),
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

                if ($ == 'nodes') return this.#getNodes(service)

                if ($ == 'watch$') return () => {
                    return this.#nodes$.pipe(
                        map(m => {
                            const nodes = this.#getNodes(service)
                            const token = nodes.sort((a, b) => a.version - b.version).map(e => `${e.node_id}|${e.version}`).join('|')
                            return {
                                nodes,
                                token
                            }
                        }),
                        distinctUntilKeyChanged('token'),
                        map(a => a.nodes)
                    )
                }

                if ($.startsWith('batch__')) {
                    const method = $.split('__batch__')?.[1]
                    const nodes = this.#getNodes(service)
                    return (...args: any[]) => {
                        const $ = from(nodes).pipe(
                            mergeMap(node => (
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
                    return (options: RpcOptions) => new Proxy({}, {
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