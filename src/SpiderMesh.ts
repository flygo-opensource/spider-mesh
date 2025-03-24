import { BehaviorSubject, catchError, distinctUntilKeyChanged, EMPTY, filter, finalize, firstValueFrom, from, lastValueFrom, map, merge, mergeMap, Observable, of, retry, tap, timer, toArray } from "rxjs"
import { randomUUID } from "crypto";
import { listBeforeMicroserviceOnlineMethods } from "./decorators/BeforeMicroserviceOnline.js";
import { RemoteService } from "./interfaces/RemoteService.js";
import { SpiderMeshNode } from "./interfaces/SpiderMeshNode.js";
import { NAMEPSACE } from "./const.js";
import { DiscoveryTransporter } from "./interfaces/DiscoveryTransporter.js";
import { PubsubTransporter } from "./interfaces/PubsubTransporter.js";
import { RpcTransporter, RpcOptions } from "./interfaces/RpcTransporter.js";
import { MicroserviceException } from "./helpers/MicroserviceException.js";
import { MicroserviceOfflineException } from "./helpers/MicroserviceOfflineException.js";
import { services$ } from "./decorators/Microservice.js";
import { networkInterfaces } from "os";


export type HelloEvent = SpiderMeshNode & { back?: boolean }


export class SpiderMesh {

    public readonly METADATA_TOPIC = '@@metadata@@'
    public readonly node_id = randomUUID()
    public readonly namespace = NAMEPSACE


    public readonly metadata$ = new BehaviorSubject<SpiderMeshNode>({
        ips: Object.values(networkInterfaces()).flat(2).filter(a => !a?.internal && !a?.address).map(a => a?.address!),
        host: '',
        namespace: this.namespace,
        node_id: this.node_id,
        services: {},
        topics: [],
        transporters: {},
        nodes: {},
        version: 0
    })


    #transporters = {
        rpc: new Set<RpcTransporter>,
        pubsub: new Set<PubsubTransporter>,
        discovery: new Set<DiscoveryTransporter>
    }


    constructor() {
        const key = Symbol.for('metadatakey')
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
                    ... this.metadata$.value,
                    services: Object.entries(services).reduce((p, [name, v]) => {
                        return {
                            ...p,
                            [name]: (v as any)['$']
                        }
                    }, {} as { [name: string]: any }),
                    version: Date.now()
                }
                this.metadata$.next(metadata)
            }),
            catchError(e => {
                return EMPTY
            })
        ).subscribe()
    }

    async sync(node: SpiderMeshNode & { host: string }) {

        for (const t of this.#transporters.rpc) {
            t.link?.(node)
        }

        for (const t of this.#transporters.pubsub) {
            t.link?.(node)
        }

        // Update metadata
        const { [node.node_id]: old, ...nodes } = this.metadata$.value.nodes || {}
        this.metadata$.next({
            ... this.metadata$.value,
            nodes: node.online ? {
                ... this.metadata$.value.nodes,
                [node.node_id]: node.version
            } : {
                ...nodes
            }
        })

        // Say hello if is new node
        if (node.nodes[this.node_id] != this.metadata$.value.version) {
            for (const t of this.#transporters.discovery) {
                t.broadcast(this.metadata$.value, node.host)
            }
        }


    }


    rpc<T>(options: RpcOptions) {
        const started_at = Date.now()
        return of(0).pipe(
            mergeMap(async () => {
                await this.wait(options.service)
                const list = [...this.#transporters.rpc.values()]
                const transporters = list.length <= 1 ? [...this.#transporters.rpc.values()].filter(transporter => transporter.check(options.service).length > 0) : list
                const transporter = transporters[transporters.length == 1 ? 0 : Date.now() % transporters.length]
                if (!transporter) throw new MicroserviceOfflineException()
                return transporter.rpc<T>(options)
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
                if (options.fallback != undefined) return of(options.fallback)
                if (e.code) {
                    const error = new MicroserviceException(e.code, e.metadata)
                    error.stack = e.stack
                    console.error(error.stack)
                    throw error
                }
                throw e
            })
        )
    }


    lpc(options: RpcOptions) {
        const service = services$.value[options.service]
        const instance = service?.instance
        if (!instance) throw new MicroserviceException('Microservice not found')
        if (typeof instance[options.method] != 'function') throw new MicroserviceException('Micro method not found')
        return instance[options.method](...options.args)
    }

    add(instance: RpcTransporter | DiscoveryTransporter | PubsubTransporter) {

        // Is RpcTransporter
        if ((instance as RpcTransporter).rpc) {
            const transporter = instance as RpcTransporter
            if (this.#transporters.rpc.has(transporter)) return
            const subscription = merge(
                transporter.metadata$.pipe(
                    tap(metadata => {
                        const { transporters, ...rest } = this.metadata$.getValue()
                        this.metadata$.next({
                            ...rest,
                            transporters: {
                                ...transporters,
                                ...metadata
                            },
                            version: Date.now()
                        })
                    }),
                    finalize(() => {
                        this.#transporters.rpc.delete(transporter)
                    })
                )
            ).subscribe()
            this.#transporters.rpc.add(transporter)
            return () => subscription.unsubscribe()
        }


        // Is DiscoveryTransporter
        if ((instance as DiscoveryTransporter).broadcast) {
            const t = instance as DiscoveryTransporter
            if (this.#transporters.discovery.has(t)) return
            const subscription = this.metadata$.pipe(
                distinctUntilKeyChanged('version'),
                tap(() => {
                    const metadata = this.metadata$.value
                    t.broadcast(metadata)
                }),
                finalize(() => this.#transporters.discovery.delete(t))
            ).subscribe()
            this.#transporters.discovery.add(t)
            return () => subscription.unsubscribe()
        }


        // Is PubsubTransporter
        if ((instance as PubsubTransporter).publish) {
            const t = instance as PubsubTransporter
            if (this.#transporters.pubsub.has(t)) return
            this.#transporters.pubsub.add(t)
            return () => this.#transporters.pubsub.delete(t)
        }
    }

    #get_nodes(service_name: string) {
        const transporters = [...this.#transporters.rpc.values()]
        const nodes = transporters.map(t => t.check(service_name)).flat(2).reduce(
            (p, c) => ({ ...p, [c.node_id]: c })
            , {} as { [node_id: string]: SpiderMeshNode }
        )
        return Object.values(nodes)
    }


    wait(name: string, check: (nodes: SpiderMeshNode[]) => Promise<boolean> | boolean = nodes => Object.keys(nodes).length > 0) {
        return firstValueFrom(this.metadata$.pipe(
            mergeMap(async e => {
                const nodes = this.#get_nodes(name)
                if (await check(nodes)) return true
            }),
            filter(Boolean)
        ))
    }

    async expose(name: string, instance: any) {
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

    service<T>(factory: { new(...args: any[]): T }) {

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

        return new Proxy({}, {
            get: (_, $: string) => {

                if (omitProperties.has($)) return null

                if ($ == 'wait$') {
                    return (fn?: (nodes: SpiderMeshNode[]) => boolean | Promise<boolean>) => this.wait(service, fn)
                }

                if ($ == 'nodes') return this.#get_nodes(service)

                if ($ == 'watch$') return () => this.metadata$.pipe(
                    map(() => {
                        const nodes = this.#get_nodes(service)
                        const token = nodes.sort((a, b) => a.version - b.version).map(e => `${e.node_id}|${e.version}`).join('|')
                        return {
                            nodes,
                            token
                        }
                    }),
                    distinctUntilKeyChanged('token')
                )

                if ($.startsWith('__batch__')) {
                    const method = $.split('__batch__')?.[1]
                    const nodes = this.#get_nodes(service)
                    return (...args: any[]) => {
                        const $ = from(nodes).pipe(
                            mergeMap(node => (
                                this.rpc({
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
                            return (...args: any[]) => this.rpc({
                                ...options,
                                args,
                                method,
                                service
                            })
                        }
                    })
                }

                return (...args: any[]) => {
                    const response = this.rpc<Observable<any>>({
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

    }

    event<T>(factory: { new(...args: any[]): T }) {
        const topic = factory.name

        return {
            publish: (data: T) => lastValueFrom(
                from(this.#transporters.pubsub).pipe(
                    mergeMap(t => t.publish(topic, data))
                ), { defaultValue: undefined as any as void }
            ),
            listen: () => {
                return from(this.#transporters.pubsub).pipe(
                    mergeMap(t => t.listen<T>(topic)
                    )
                )
            }
        }
    }

}  