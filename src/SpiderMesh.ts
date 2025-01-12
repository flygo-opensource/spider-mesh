import { BehaviorSubject, catchError, debounceTime, delay, EMPTY, filter, firstValueFrom, from, interval, lastValueFrom, map, merge, mergeAll, mergeMap, Observable, of, retry, retryWhen, share, Subject, tap, throwError, timer, toArray } from "rxjs"
import { PublishOptions, RpcOptions, RpcRoutingOptions, SpiderMeshPubsubTransporter, SpiderMeshRpcTransporter } from "./interfaces/SpiderMeshTransporter.js";
import { randomUUID } from "crypto";
import { listBeforeMicroserviceOnlineMethods } from "./decorators/BeforeMicroserviceOnline.js";
import { RemoteService } from "./interfaces/RemoteService.js";
import { SpiderMeshNode } from "./interfaces/SpiderMeshNode.js";
import { $services } from "./decorators/Microservice.js";
import { ServiceDiscovery } from "./interfaces/ServiceDiscovery.js";


export class ServiceNotFound extends Error { }
export class MissingPubsubTransporter extends Error { }
export class ServiceOffline extends Error { }
export class RpcTimeout extends Error { }

export type HelloEvent = SpiderMeshNode & { back?: boolean }


export class SpiderMesh {

    #pubsub_transporters = new Set<SpiderMeshPubsubTransporter>()
    #rpc_transporters = new Set<SpiderMeshRpcTransporter>()
    #$local_services = new BehaviorSubject<{ [name: string]: any }>({})
    #remote_services = new Map<string, SpiderMeshRpcTransporter>()
    #remote_nodes = new BehaviorSubject({ nodes: new Map<string, SpiderMeshNode>(), node: undefined as undefined | SpiderMeshNode })

    public readonly node_id = randomUUID()
    public readonly METADATA_TOPIC = '@@metadata@@'

    constructor() {
        $services.subscribe(({ instance, name }) => this.exposeLocalService(name, instance))
    }

    public get metadata() {
        return {
            node_id: this.node_id,
            services: this.#$local_services.getValue()
        } as SpiderMeshNode
    }

    getLocalServices() {
        return this.#$local_services
    }

    rpc<T>(options: RpcOptions) {
        return of(0).pipe(
            mergeMap(async () => this.#remote_services.get(options.service) || await this.waitServiceReady(options.service, undefined, options.routing)),
            mergeMap(transporter => {
                if (!transporter) throw new ServiceOffline('SERVICE_OFFLINE')
                return transporter.rpc<T>(options)
            }),
            retry({
                delay: (e: Error, count: number) => {
                    if (e instanceof ServiceOffline) {
                        if (options.retry && count >= options.retry) {
                            if (options.fallback != undefined) throw of(options.fallback)
                            throw e
                        }
                        if (options.fallback != undefined) throw of(options.fallback)
                        return of(0).pipe(delay(1000))
                    }
                    throw e
                }
            }),
            catchError(e => {
                if (e instanceof Observable) return e
                throw e
            })
        )
    }

    handleRpc(options: RpcOptions) {
        if (options.method == this.METADATA_TOPIC) {
            return Promise.resolve(this.metadata)
        }
        const instance = this.#$local_services.getValue()[options.service]
        if (!instance) return throwError('SERVICE_NOT_FOUND')
        if (typeof instance[options.method] != 'function') return throwError('ACTION_NOT_FOUND')
        return instance[options.method](...options.args)
    }

    linkRpcTransporter(t: SpiderMeshRpcTransporter) {
        this.#rpc_transporters.add(t)
    }

    linkServiceDiscovery(t: ServiceDiscovery) {
        t.$nodes.subscribe(({ node_id, status }) => {
            if (status == 'offline') {
                const $ = this.#remote_nodes.getValue()
                const node = $.nodes.get(node_id)
                if (node) {
                    $.nodes.delete(node_id)
                    this.#remote_nodes.next({ nodes: $.nodes, node })
                }

            }
        })
    }

    linkPubsubTransporter(t: SpiderMeshPubsubTransporter) {
        this.#pubsub_transporters.add(t)

        this.#$local_services.pipe(
            debounceTime(1000),
            map((v, i) => {
                t.publish<HelloEvent>({
                    data: {
                        ...this.metadata,
                        back: i == 0
                    },
                    event: this.METADATA_TOPIC
                })
            })
        ).subscribe()

        merge(
            t.listen<HelloEvent>(this.METADATA_TOPIC),
            t.listen<HelloEvent>(this.node_id)
        ).subscribe(node => {
            const $ = this.#remote_nodes.getValue()
            $.nodes.set(node.node_id, node)
            this.#remote_nodes.next({ nodes: $.nodes, node })
            node.back && t.publish<HelloEvent>({
                event: node.node_id,
                data: this.metadata
            })
        })


    }

    async waitServiceReady(name: string, check: (nodes: SpiderMeshNode[]) => Promise<boolean> | boolean = nodes => nodes.length > 0, routing?: RpcRoutingOptions) {

        while (true) {
            const transporter = await firstValueFrom(from([...this.#rpc_transporters.values()]).pipe(
                mergeMap(async transporter => {
                    try {
                        const node = await firstValueFrom(merge(
                            transporter.rpc<SpiderMeshNode>({
                                args: [],
                                method: this.METADATA_TOPIC,
                                service: name,
                                routing
                            }).pipe(catchError(e => of(null))),
                            timer(1000)
                        ), { defaultValue: null })
                        if (!node) return
                        for (const [service, ready] of Object.entries(node.services)) {
                            if (ready) {
                                this.#remote_services.set(service, transporter)
                            }
                        }
                        const $ = this.#remote_nodes.getValue()
                        node && !$.nodes.has(node.node_id) && (
                            $.nodes.set(node.node_id, node),
                            this.#remote_nodes.next({ nodes: $.nodes, node })
                        )
                        const nodes = [...$.nodes.values()].filter(node => node.services[name])
                        if (check(nodes)) return transporter
                    } catch (e) { }
                })
            ))
            if (transporter) return transporter
            await firstValueFrom(timer(1000))
        }
    }

    async exposeLocalService(name: string, instance: any) {

        // run before check hooks
        const hooks = listBeforeMicroserviceOnlineMethods(instance)
        for (const method of hooks) await instance[method]

        // mark as ready
        this.#$local_services.next({
            ... this.#$local_services.getValue(),
            [name]: instance
        })
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

        return new Proxy({}, {
            get: (_, $: string) => {

                if (omitProperties.has($)) return null

                if ($ == '$wait') {
                    return (fn?: (nodes: SpiderMeshNode[]) => boolean | Promise<boolean>) => this.waitServiceReady(service, fn)
                }

                if ($ == '$nodes') {
                    const nodes = [... this.#remote_nodes.getValue().nodes.values()].filter(node => node.services[service])
                    return nodes
                }

                if ($ == '$watch') return () => this.#remote_nodes.pipe(
                    filter($ => $.node && $.node.services[service] ? true : false),
                    map($ => [...$.nodes.values()])
                )

                if ($.startsWith('__batch__')) {
                    const method = $.split('__batch__')?.[1]
                    const nodes = [... this.#remote_nodes.getValue().nodes.values()].filter(node => node.services[service])
                    return (...args: any[]) => {
                        const $ = from(nodes).pipe(
                            mergeMap(node => (
                                this.rpc({
                                    args,
                                    method,
                                    service,
                                    routing: node
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

                if ($ == '$set') {
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

    linkEvent<T>() {
        if (this.#pubsub_transporters.size == 0) throw new MissingPubsubTransporter('MISSING_PUBSUB_TRANSPORTER')


        return {
            publish: (options: PublishOptions<T>) => lastValueFrom(
                from(this.#pubsub_transporters).pipe(
                    mergeMap(t => t.publish(options))
                ), { defaultValue: undefined as any as void }
            ),
            listen: (name: string) => from(this.#pubsub_transporters).pipe(
                mergeMap(t => t.listen<T>(name)
                )
            )
        }
    }

}  