import { BehaviorSubject, debounceTime, filter, firstValueFrom, from, interval, lastValueFrom, merge, mergeMap, of, Subject } from "rxjs"
import { PublishOptions, RpcOptions, SpiderMeshPubsubTransporter, SpiderMeshRpcTransporter } from "./interfaces/SpiderMeshTransporter.js";
import { randomUUID } from "crypto";
import { listBeforeMicroserviceOnlineMethods } from "./decorators/BeforeMicroserviceOnline.js";
import { RemoteService } from "./interfaces/RemoteService.js";
import { SpiderMeshNode } from "./interfaces/SpiderMeshNode.js";


export class ServiceNotFound extends Error { }
export class MissingPubsubTransporter extends Error { }
export class ServiceOffline extends Error { }


export class SpiderMesh {

    #pubsub_transporters = new Set<SpiderMeshPubsubTransporter>()
    #rpc_transporters = new Set<SpiderMeshRpcTransporter>()
    #$local_services = new BehaviorSubject<{ [name: string]: any }>({})
    #remote_services = new Map<string, SpiderMeshRpcTransporter>()
    #remote_nodes = new Map<string, SpiderMeshNode>()
    #$node = new Subject<SpiderMeshNode & { status: 'online' | 'offline' }>

    public readonly node_id = randomUUID()
    public readonly METADATA_TOPIC = '__metadata__'

    public get metadata() {
        return {
            node_id: this.node_id,
            services: this.#$local_services.getValue()
        } as SpiderMeshNode
    }

    getLocalServices(){
        return this.#$local_services
    }

    async rpc<T>(options: RpcOptions) {
        const transporter = this.#remote_services.get(options.service) || await this.waitServiceReady(options.service)
        if (!transporter) throw new ServiceOffline('SERVICE_OFFLINE')
        return transporter.rpc<T>(options)
    }

    linkRpcTransporter(t: SpiderMeshRpcTransporter) {
        this.#rpc_transporters.add(t)
        t.$requests.subscribe(({ reply, ...options }) => {
            // handle rpc requests
        })
    }

    linkPubsubTransporter(t: SpiderMeshPubsubTransporter) {
        this.#pubsub_transporters.add(t)
        t.$nodes.subscribe(({ node_id, status }) => {
            status == 'offline' && this.#remote_nodes.delete(node_id)
        })
        this.#$local_services.pipe(debounceTime(1000)).subscribe(
            () => t.publish<SpiderMeshNode>({
                data: this.metadata,
                event: this.METADATA_TOPIC
            })
        )
        t.listen<SpiderMeshNode>(this.METADATA_TOPIC).subscribe(node => {
            this.#remote_nodes.set(node.node_id, node)
        })

    }

    waitServiceReady(name: string, check: (nodes: SpiderMeshNode[]) => Promise<boolean> | boolean = nodes => nodes.length > 0) {

        // Check via RPC
        return firstValueFrom(merge(of(0), interval(2000)).pipe(
            mergeMap(() => from([...this.#rpc_transporters.values()]).pipe(
                mergeMap(async transporter => {
                    const node = await firstValueFrom(transporter.rpc<SpiderMeshNode>({
                        args: [],
                        method: this.METADATA_TOPIC,
                        service: name
                    }))
                    if (!node) return
                    for (const [service, ready] of Object.entries(node.services)) {
                        if (ready) {
                            this.#remote_services.set(service, transporter)
                        }
                    }
                    node && !this.#remote_nodes.has(node.node_id) && this.#$node.next({
                        ...node,
                        status: 'online'
                    })
                    this.#remote_nodes.set(node.node_id, node)
                    const nodes = [... this.#remote_nodes.values()].filter(node => node.services.includes(name))
                    if (check(nodes)) return transporter
                })
            ), 1),
            filter(Boolean)
        ))
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
                    const nodes = [... this.#remote_nodes.values()].filter(node => node.services.includes(service))
                    return nodes
                }

                if ($ == '$watch') return this.#$node.pipe(filter(n => n.services.includes(service)))

                if ($.startsWith('__batch__')) {
                    const method = $.split('__batch__')?.[1]
                    const nodes = [... this.#remote_nodes.values()].filter(node => node.services.includes(service))
                    return (...args: any[]) => {
                        const o = new Subject()
                        from(nodes).pipe(
                            mergeMap(async node => {
                                try {
                                    const data = await firstValueFrom(await this.rpc({
                                        args,
                                        method,
                                        service
                                    }))
                                    return { node, data }
                                } catch (error) {
                                    return { node, error }
                                }
                            })
                        ).subscribe(o)
                        return o
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

                return null
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