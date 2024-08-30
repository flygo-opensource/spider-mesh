
import { DeepProxy } from './decorators/DeepProxy.js'
import { SpiderMeshNode, SpiderMeshNodeMetadata } from './interfaces/SpiderMeshNode.js'
import { PublishMetadata, SpiderMeshTransporter, SpiderMeshTransporterEvent, TcpNodeStatus } from './interfaces/SpiderMeshTransporter.js'
import { RPCOptions, RPCOptionsList } from './RPCOptions.js'
import { RemoteService } from './interfaces/RemoteService.js'
import { EventHub, listEventSubscribers } from './decorators/ListenEvent.js'
import { listReadyHookMethods } from './decorators/OnMicroserviceReady.js'
import { BehaviorSubject, Observable, ReplaySubject, Subject, Subscriber, bufferTime, catchError, debounceTime, filter, firstValueFrom, from, groupBy, map, merge, mergeAll, mergeMap, of, pipe, retry, scan, share, tap, timeout } from 'rxjs'
import { sleep } from './helpers/sleep.js'
import { Encodable } from './Encoder.js'
import { NAMEPSACE } from './const.js'
import { $services } from './decorators/Microservice.js'
import { randomUUID } from './helpers/randomUUID.js'
import axios from 'axios'


type SpiderMeshMetadata = {
    //** Spider mesh sender node id*/ 
    smnid:
    string
}


export type SpiderMeshRpcEvent = {
    request: {
        request_id: string,
        args: Buffer,
        service: string,
        method: string
    }
    response: {
        request_id: string,
        error?: Encodable
        data?: { value: Encodable }
        completed?: boolean
    }
}



export type SpiderMeshNamespace = string



export type SpiderMeshBatchPublishPayload<T extends Encodable = Encodable> = {
    topic: string | { new(): EventHub<T> },
    payload: T[],
    node_id?: string,
    local_transporter_id?: string
}



type NodeId = string
type RemoteTransporterID = string

const $public_ip = new Promise<string | undefined>(async resolve => {
    try {
        const { data } = await axios.get('https://api.ipify.org?format=json')
        resolve(data.ip)
    } catch (error) {
        resolve(undefined)
    }
})

export class SpiderMesh {

    public static $transporters = new ReplaySubject<SpiderMeshTransporter>()

    #node_id = randomUUID()

    #requests = new Map<string, { rpc_node_id: string, $response: Subscriber<any> }>

    #local_services = new Map<string, {
        instance: any,
        metadata: any
    }>()

    #remote_services = new Map<string, {
        last_call_index: number
        nodes: SpiderMeshNode[]
    }>
    #linked_nodes = new Map<string, SpiderMeshNode>()


    $nodes_monitor = new Subject<SpiderMeshNode>()


    #$isolated = new BehaviorSubject<boolean>(false)

    #transporters = new Map<string, {
        nodes: Map<RemoteTransporterID, NodeId>,
        transporter: SpiderMeshTransporter
    }>()

    constructor(private metadata: any = {}) {

        merge(

            // Link transporter
            SpiderMesh.$transporters.pipe(
                mergeMap(async transporter => {
                    await this.#link_transporter(transporter)
                    return transporter
                }),
                map(t => t.transporter_id)
            ),

            // Link local services
            $services.pipe(
                tap(({ instance, metadata }) => {
                    const prototype = Object.getPrototypeOf(instance)
                    const name = prototype.constructor.name
                    this.#local_services.set(name, { instance, metadata })
                }),
                map(() => undefined)
            )
        ).pipe(

            // Sync with other nodes
            debounceTime(2000),
            mergeMap(async transporter_id => {
                await this.#self_introduce(transporter_id)
            }, 1)
        ).subscribe()

    }


    #caculate_rpc_node_id(service_name: string, options: Partial<RPCOptions> = {}) {
        const current = this.#remote_services.get(service_name)
        if (!current || current.nodes.length == 0) return
        if (options.$node_id) {
            if (current.nodes.some(node => node.node_id == options.$node_id)) return options.$node_id
            return
        }
        const option_ip = options.$ip
        const nodes = option_ip ? (
            current
                .nodes
                .filter(node => node.public_ip == option_ip)
        ) : current.nodes
        if (nodes.length == 0) return

        current.last_call_index = (current.last_call_index + 1) % nodes.length
        return nodes[current.last_call_index].node_id

    }

    rpc<T = any>(service: string, method: string, args: any, options: Partial<RPCOptions> = {}) {
        const rpc_node_id = this.#caculate_rpc_node_id(service, options)
        if (!rpc_node_id) {
            const e = new Error(`SERVICE_NOT_RUNNING:${service}`, {
                cause: {
                    service,
                    method,
                    args,
                    options
                }

            })
            return Object.assign(
                new Observable<T>(o => o.error(e)),
                {
                    then: async (success: Function, error: Function) => {
                        error(e)
                    },
                    async catch() {
                        return {} as T
                    },
                    async finally() {
                        return {} as T
                    },
                    [Symbol.toStringTag]: ''
                }
            ) as Observable<T> & Promise<T>
        }
        if (options.$forgot) {
            this.publish<Pick<SpiderMeshRpcEvent, 'request'>>({
                topic: rpc_node_id,
                payload: [{
                    request: {
                        args,
                        method,
                        request_id: '#',
                        service
                    }
                }],
                node_id: rpc_node_id
            })
            return
        }

        const $ = new Observable($response => {
            const request_id = randomUUID()

            // Send RPC request here 
            this.#requests.set(request_id, { $response, rpc_node_id })

            // Send RPC request here 
            this.publish<Pick<SpiderMeshRpcEvent, 'request'>>({
                topic: rpc_node_id,
                payload: [{
                    request: {
                        args,
                        method,
                        request_id,
                        service
                    }
                }],
                node_id: rpc_node_id
            })

            return () => {
                this.#requests.delete(request_id)
            }
        }).pipe(
            retry({ count: options.$retry || 0, delay: options.$retry_delay || 1000 }),
            options.$timeout ? timeout(options.$timeout) : tap(),
            catchError(err => {
                if (options.$fallback) {
                    return of(options.$fallback)
                }
                throw err
            }),
            share()
        )

        $.subscribe({ error: () => { }, next: () => { } })



        return Object.assign($, {
            then: async (success: Function, error: Function) => {
                try {
                    const value = await firstValueFrom($)
                    success(options.$safe_mode ? [null, value] : value)
                } catch (e) {
                    options.$safe_mode ? success([e, null]) : error(e)
                }
            },
            async catch() {
                return {} as T
            },
            async finally() {
                return {} as T
            },
            [Symbol.toStringTag]: ''
        }) as Observable<T> & Promise<T>
    }

    async $metadata() {

        const services = [...this.#local_services.entries()].reduce(
            (p, [service_id, { metadata }]) => ({
                ...p,
                [service_id]: metadata
            }),
            {}
        )
        const isolated_nodes = [
            ...new Set([...this.#linked_nodes.values()]
                .filter(node => node.isolated)
                .map(node => node.local_transporter_id))
        ]

        const metadata: Omit<SpiderMeshNodeMetadata, 'local_transporter_id' | 'remote_transporter_id'> = {
            metadata: this.metadata,
            node_id: this.#node_id,
            node_version: process.version,
            last_online: Date.now(),
            online: true,
            services,
            namespace: NAMEPSACE,
            linked: [...this.#linked_nodes.keys()],
            isolated_nodes,
            isolated: this.#$isolated.value,
            public_ip: await $public_ip
        }
        return metadata
    }


    async #self_introduce(local_transporter_id?: string, node_id?: string) {
        const list = local_transporter_id ? [this.#transporters.get(local_transporter_id)] : [... this.#transporters.values()]
        for (const target of list) {
            if (!target) continue
            const me: SpiderMeshNodeMetadata = {
                ...await this.$metadata(),
                local_transporter_id: target.transporter.transporter_id,
                remote_transporter_id: '#OVERWRITE'
            }
            await this.publish({ payload: [me], topic: '#join', node_id }, target.transporter)
        }
    }

    async $update_isolate_mode(active: boolean) {
        this.#$isolated.next(active)
        const me = await this.#self_introduce()
        return me
    }

    #get_rpc_target(msg: SpiderMeshRpcEvent['request']) {
        if (msg.service == 'SpiderMesh') {
            if (!msg.method.startsWith('$')) throw `SPIDER_MESH_METHOD_NOT_ALLOW`
            return this
        }
        const service = this.#local_services.get(msg.service)?.instance
        if (!service) throw 'RPC_SERVICE_NOT_FOUND'
        if (typeof service[msg.method] != 'function') throw 'RPC_METHOD_NOT_FOUND'
        return service
    }

    async #node_event_handler(transporter: SpiderMeshTransporter, { payload: event, metadata: { smnid } }: SpiderMeshTransporterEvent<SpiderMeshRpcEvent, SpiderMeshMetadata>) {
        if (event.request) {
            const response = (data: Partial<SpiderMeshRpcEvent['response']>) => {
                const error = data.error ? (data.error instanceof Error ? {
                    cause: data.error.cause as string,
                    message: data.error.message,
                    name: data.error.name as string,
                    stack: data.error.stack as string
                } : data.error) : undefined
                const payload = [{
                    response: {
                        request_id: event.request.request_id,
                        ...data,
                        error
                    },
                }]
                this.publish<Pick<SpiderMeshRpcEvent, 'response'>>({
                    payload,
                    topic: smnid,
                    node_id: smnid
                }, transporter)
            }
            try {
                const instance = this.#get_rpc_target(event.request)

                try {
                    const value = await instance?.[event.request.method]?.(...event.request.args || [])
                    if (event.request.request_id == '#') return
                    if (value instanceof Observable) {
                        value.subscribe({
                            complete: () => response({ completed: true }),
                            error: error => response({ error }),
                            next: value => response({ data: { value } })
                        })
                        return
                    } else {
                        response({ completed: true, data: { value } })
                    }
                } catch (error) {
                    if (event.request.request_id == '#') return
                    response({ error: error as Encodable })
                }
            } catch (error) {
                if (event.request.request_id == '#') return
                response({ error: error as any })
            }
        }

        if (event.response) {
            const request = this.#requests.get(event.response.request_id)
            if (!request) return
            if (event.response.data) {
                request.$response.next(event.response.data.value)
            }
            if (event.response.completed) {
                request.$response.complete()
            }
            if (event.response.error) {
                request.$response.error(event.response.error)
            }
        }
    }

    async #link_transporter(transporter: SpiderMeshTransporter) {

        this.#transporters.set(transporter.transporter_id, {
            transporter,
            nodes: new Map()
        })


        // Listen RPC 
        this.listen<SpiderMeshRpcEvent>(this.#node_id, transporter).pipe(
            tap(msg => this.#node_event_handler(transporter, msg))
        ).subscribe()

        // Listen new node
        this.listen<SpiderMeshNode>('#join', transporter).pipe(
            filter(node => node.metadata.smnid != this.#node_id),
            groupBy(node => node.metadata.smnid),
            mergeMap(grouped => grouped.pipe(
                debounceTime(500),
                map(({ payload, sti }) => ({
                    ...payload,
                    local_transporter_id: transporter.transporter_id,
                    remote_transporter_id: sti,
                } as SpiderMeshNode)),
                filter(node => node.node_id != this.#node_id),
                mergeMap(node => this.#on_node_discovered(node), 1),
            )),
        ).subscribe()

        // Sync node status
        transporter.$nodes_status.pipe(
            tap(node => !node.online && this.#on_node_offline(transporter.transporter_id, node.remote_transporter_id)),
            groupBy(node => node.remote_transporter_id),
            mergeMap(grouped => grouped.pipe(
                scan((prev, current) => {
                    if (!prev || (prev.online != current.online)) return current as TcpNodeStatus
                    return undefined
                }, undefined as (undefined | TcpNodeStatus)),
                filter(Boolean),
                map(node => node as TcpNodeStatus),
                filter(node => node.online),
                tap(node => this.#self_introduce(transporter.transporter_id))
            ))
        ).subscribe()


        // Sync services with other nodes
        $services.subscribe(({ instance }) => {

            const prototype = Object.getPrototypeOf(instance)
            const name = prototype.constructor.name

            this.listen<SpiderMeshRpcEvent>(name, transporter).subscribe(evt => this.#node_event_handler(transporter, evt))


            // Active event subscribers
            const event_subscribers = listEventSubscribers(prototype)
            for (const { event, method, buffer_ms } of event_subscribers) {
                this.listen<SpiderMeshMetadata>(event, transporter).pipe(
                    filter(() => !this.#$isolated.value),
                    buffer_ms ? pipe(bufferTime(buffer_ms), filter(l => l.length > 0)) : pipe(map(item => [item]))
                ).subscribe(e => instance[method]?.(e, this))
            }
        })

    }

    async #on_node_discovered(node: SpiderMeshNode) {

        if (node.node_id == this.#node_id) return
        const saved_node = this.#linked_nodes.get(node.node_id)
        if (saved_node && saved_node.last_online > node.last_online) return
        const peer_updated = node.linked.includes(this.#node_id)
        const new_node: SpiderMeshNode = {
            ...node,
            online: true
        }

        this.#linked_nodes.set(node.node_id, new_node);
        this.#transporters.get(node.local_transporter_id)?.nodes.set(node.remote_transporter_id, node.node_id)
        if (!peer_updated && !this.#$isolated.value) await this.#self_introduce(node.local_transporter_id, node.node_id)

        for (const service_id of Object.keys(node.services)) {
            if (!this.#remote_services.has(service_id)) {
                this.#remote_services.set(service_id, {
                    last_call_index: -1,
                    nodes: []
                })
            }
            const $service = this.#remote_services.get(service_id)!
            const index = $service.nodes.findIndex(n => n.node_id == new_node.node_id)
            index >= 0 ? ($service.nodes[index] = new_node) : $service.nodes.push(new_node)
        }

        this.$nodes_monitor.next(node)

    }

    #on_node_offline(local_transporter_id: string, remote_transporter_id: string) {

        const node_id = this.#transporters.get(local_transporter_id)?.nodes?.get(remote_transporter_id)
        if (!node_id) return
        const node = this.#linked_nodes.get(node_id)
        if (!node) return


        // Remove remote node from linked remote services
        for (const service of this.#remote_services.values()) {
            service.nodes = service.nodes.filter(node => node.remote_transporter_id != remote_transporter_id)
        }

        // Throw errors fro requests
        for (const [rid, { $response, rpc_node_id }] of this.#requests) {
            if (rpc_node_id == node_id) {
                $response.error('OFFLINE')
                this.#requests.delete(rid)
            }
        }


        node && this.$nodes_monitor.next({ ...node, online: false })
        this.#linked_nodes.delete(node_id)

    }

    async link_remote_service<T>(factory: { new(...args: any[]): T }, wait_service_online: boolean = false) {

        const service_name = factory.name

        const omit_properties = new Set([
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

        const actions = new Set<string>()
        for (let f = factory.prototype; f != null; f = Object.getPrototypeOf(f)) {
            for (const method of Object.getOwnPropertyNames(f).filter(method => !omit_properties.has(method))) {
                typeof f[method] == 'function' && actions.add(method)
            }
        }


        wait_service_online && await this.#wait_service_online(service_name)

        return new Proxy({}, {
            get: (_, method: string) => {
                if (method == '$wait_service_online') {
                    return () => this.#wait_service_online(service_name)
                }

                if (method == '$watch') return () => this.$nodes_monitor.pipe(
                    filter(node => !!node.services[service_name])
                )

                if (method == '$list_nodes') {
                    return (
                        () => this.#remote_services.get(service_name)?.nodes.filter(node => !node.isolated).filter(node => node.online) || []
                    ) as RemoteService<T>['$list_nodes']
                }

                if (method.startsWith('$batch_')) {
                    const real_method = method.split('$batch_')?.[1]

                    if (!real_method || !actions.has(real_method)) return null
                    const nodes = this.#remote_services.get(service_name)?.nodes.filter(node => !node.isolated) || []

                    return (...args: any[]) => {
                        const o = new Subject()
                        from(nodes).pipe(
                            mergeMap(async node => {
                                try {
                                    const data = await this.rpc(
                                        service_name,
                                        real_method,
                                        args,
                                        { $node_id: node.node_id } as RPCOptions
                                    )
                                    return { node, data }
                                } catch (error) {
                                    return { node, error }
                                }
                            })
                        ).subscribe(o)
                        return o
                    }
                }

                if (actions.has(method) || method.startsWith('$')) {
                    return new DeepProxy(
                        method => RPCOptionsList.has(method),
                        (method: string, options) => (
                            (...args: any[]) => this.rpc(service_name, method, args, options)
                        )
                    ).nest()[method as keyof DeepProxy]
                }

                return null
            }
        }) as RemoteService<T>

    }

    async link_event<T extends Encodable = Encodable>(event_factory: { new(...args: any[]): T }, publish_buffer_ms?: number) {
        const $ = new Subject<T>()
        const $$: Observable<T | T[]> = publish_buffer_ms ? $.pipe(bufferTime(publish_buffer_ms), filter(l => l.length > 0)) : $
        $$.subscribe(data => this.publish({
            payload: [data],
            topic: event_factory.name
        }))

        return {
            publish: async (data: T) => $.next(data),
            listen: () => this.listen<T>(event_factory)
        }

    }



    async #wait_service_online(service_name: string = 'all') {

        while (true) {
            await sleep(1000)
            if (service_name == 'all') {
                if ([...this.#remote_services.values()].every(e => e.nodes.length > 0)) {
                    return true
                }
            } else {
                const nodes = this.#remote_services.get(service_name)?.nodes || []
                if (nodes.length > 0) return
            }
        }
    }

    async #active_ready_hooks(instance: any) {

        // Wait remote service ready
        await this.#wait_service_online()

        // Active ready hook
        for (const { method } of listReadyHookMethods(Object.getPrototypeOf(instance))) {
            instance[method]?.(this)
        }
    }


    publish<T extends Encodable>(
        { payload, topic, node_id, local_transporter_id }: SpiderMeshBatchPublishPayload<T>,
        transporter?: SpiderMeshTransporter
    ) {

        const event = typeof topic == 'string' ? topic : topic.name

        const e: PublishMetadata<T[], SpiderMeshMetadata> = {
            event,
            metadata: { smnid: this.#node_id },
            payload
        }

        if (transporter) {
            return transporter.publish(e)
        }


        if (node_id) {
            const node = this.#linked_nodes.get(node_id)
            if (!node) return
            const target = this.#transporters.get(node.local_transporter_id)
            if (!target) return
            return target.transporter.publish({
                ...e,
                rti: node.remote_transporter_id
            })
        }

        if (local_transporter_id) {
            const target = this.#transporters.get(local_transporter_id)
            return target && target.transporter.publish(e)
        }

        for (const { transporter } of this.#transporters.values()) {
            transporter.publish(e)
        }

    }


    listen<T extends Encodable = Encodable>(topic: string | { new(...args: any[]): T }, transporter?: SpiderMeshTransporter) {
        const topic_name = typeof topic == 'string' ? topic : topic.name
        const source = transporter ? of(transporter) : SpiderMesh.$transporters
        return source.pipe(
            mergeMap(transporter => transporter.listen<T[], SpiderMeshMetadata>(topic_name).pipe(
                map(msg => msg.payload.map(data => ({
                    ...msg,
                    payload: data,
                    node_id: msg.metadata!.smnid
                }))),
                mergeAll()
            ))
        )
    }
}
