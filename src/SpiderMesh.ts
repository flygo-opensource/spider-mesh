import { randomUUID } from 'crypto'
import { get } from 'http'
import { networkInterfaces } from 'os'
import { DeepProxy } from './decorators/DeepProxy.js'
import { ServiceMetadata, SpiderMeshNode, SpiderMeshNodeMetadata } from './interfaces/SpiderMeshNode.js'
import { SpiderMeshTransporter, SpiderMeshTransporterEvent } from './interfaces/SpiderMeshTransporter.js'
import { RPCOptions, RPCOptionsList } from './RPCOptions.js'
import { RemoteService } from './interfaces/RemoteService.js'
import os from 'os'
import { EventHub, listEventSubscribers } from './decorators/ListenEvent.js'
import { listReadyHookMethods } from './decorators/OnMicroserviceReady.js'
import { BehaviorSubject, EMPTY, Observable, ReplaySubject, Subject, Subscriber, Subscription, bufferTime, catchError, debounceTime, defer, filter, finalize, first, firstValueFrom, from, groupBy, lastValueFrom, map, merge, mergeAll, mergeMap, observable, of, retry, share, switchMap, takeUntil, tap, throttleTime, throwError, timer } from 'rxjs'
import { readFileSync } from 'fs'
import { serviceInstanceList } from './decorators/Microservice.js'
import { sleep } from './helpers/sleep.js'
import { BuiltinTransporter } from './builtin-transporter/BuiltinTransporter.js'
import { NAMEPSACE, NODE_ID } from './const.js'
import { Encodeable, Encoder } from './Encoder.js'

export type PureData = string | number | boolean | null | Buffer | PureData[] | { [key: string]: PureData }



export enum MessageType {
    RpcRequest = 10,
    RpcSubscribeChannel = 20,
    RpcUnsubscribeChannel = 30,
    RpcStream = 50,
}

export type RpcRequest = {
    session_id: string,
    type: MessageType.RpcRequest
    args: Buffer,
    service: string,
    method: string
}

export type RpcSubscribeChannel = {
    session_id: string,
    observable_id: string
    channel_id: string
    type: MessageType.RpcSubscribeChannel
}

export type RpcUnsubscribeChannel = {
    session_id: string,
    channel_id: string
    type: MessageType.RpcUnsubscribeChannel
}


export type RpcStream = {
    session_id: string,
    channel_id: string
    type: MessageType.RpcStream
    error?: PureData
    data?: PureData
    completed?: true
}



export type RpcEvent = RpcRequest | RpcStream | RpcSubscribeChannel | RpcUnsubscribeChannel


const PACKAGE_JSON = JSON.parse(readFileSync(`package.json`, 'utf8')) || {}

export type SpiderMeshNamespace = string

export type SpiderMeshRequest = {
    target_node_id: string
    observables: Map<string, Observable<PureData>> // Out data
    channels: Map<string, Subject<PureData>> // In data
    subscriptions: Map<string, Subscription>
}

export class SpiderMesh {

    #requests = new Map<string, SpiderMeshRequest>


    #local_services = new Map<string, {
        instance: any,
        metadata: any
    }>()

    #remote_services = new Map<string, {
        last_call_index: number
        nodes: SpiderMeshNode[]
    }>


    #linked_nodes = new Map<string, SpiderMeshNode>()

    #public_ip = new Promise<string | null>(async s => {
        for (let i = 1; i <= 5; i++) {
            const ip = await new Promise<string | null>(s => {
                get('http://api.ipify.org', (res) => {
                    res.setEncoding('utf8')
                    let rawData = ''
                    res.on('data', (chunk) => { rawData += chunk; })
                    res.on('end', () => s(rawData))
                }).on('error', () => s(null))
            })
            if (ip && ip != 'Bad Gateway') return s(ip)
            await new Promise(s => setTimeout(s, 100))
        }
        return s(null)
    })

    $nodes_monitor = new Subject<SpiderMeshNode>()

    #initing: Promise<any>

    #$isolated = new BehaviorSubject<boolean>(false)

    constructor(
        private transporter: SpiderMeshTransporter = new BuiltinTransporter(NODE_ID, NAMEPSACE)
    ) {
        this.#initing = this.#init()
    }



    #caculate_rpc_node_id(service_name: string, options: Partial<RPCOptions> = {}) {
        const current = this.#remote_services.get(service_name)
        if (!current || current.nodes.length == 0) return
        if (options.$node_id) {
            if (current.nodes.some(node => node.id == options.$node_id)) return options.$node_id
            return
        }
        const option_ip = options.$ip
        const nodes = option_ip ? (
            current
                .nodes
                .filter(node => node.public_ip == option_ip || node.ip_addresses.includes(option_ip))
        ) : current.nodes
        if (nodes.length == 0) return

        current.last_call_index = (current.last_call_index + 1) % nodes.length
        return nodes[current.last_call_index].id

    }



    rpc<T = any>(service: string, method: string, args: any, options: Partial<RPCOptions> = {}) {


        const target_node_id = this.#caculate_rpc_node_id(service, options)
        if (!target_node_id) throw `SERVICE_NOT_RUNNING:${service}`

        const session_id = randomUUID()

        const { buffer, observables } = Encoder.encode(args)

        // Send RPC request here
        const req: SpiderMeshRequest = {
            channels: new Map(),
            target_node_id,
            observables,
            subscriptions: new Map()
        }
        this.#requests.set(session_id, req)
        this.publish<RpcRequest>(
            target_node_id,
            {
                type: MessageType.RpcRequest,
                session_id,
                args: buffer,
                method,
                service,
            },
            target_node_id
        )

        const o = new Subject<PureData>()
        req.channels.set(session_id, o)

        const $ = o.pipe(
            first(),
            mergeMap(data => {
                if (data instanceof Buffer) {
                    const [response, is_pure] = this.#decode_rpc_buffer<any | Observable<any>>(req, data, session_id)
                    if (is_pure) {
                        this.#requests.delete(session_id)
                    }
                    req.channels.delete(session_id)
                    return response instanceof Observable ? response : of(response)
                }
                return EMPTY
            }),
            catchError(err => {
                if (options.$fallback) {
                    return of(options.$fallback)
                }
                throw err
            }),
            share()
        )

        $.subscribe()

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


    #get_rpc_target(msg: RpcRequest) {
        if (msg.service == 'SpiderMesh') {
            if (!msg.method.startsWith('$')) throw `SPIDER_MESH_METHOD_NOT_ALLOW`
            return this
        }
        const service = this.#local_services.get(msg.service)?.instance
        if (!service) throw 'RPC_SERVICE_NOT_FOUND'
        if (typeof service[msg.method] != 'function') throw 'RPC_METHOD_NOT_FOUND'
        return service
    }

    #decode_rpc_buffer<T>(req: SpiderMeshRequest, data: Buffer, session_id: string) {
        let is_pure = true
        const result = Encoder.decode<T>(
            data,
            () => () => {
                throw new Error('CAN_NOT_CALL_FUNCTION_IN_RPC')
            },
            observable_id => {

                is_pure = false
                const tid = setTimeout(() => {
                    throw new Error('Orphant Observable detected !', {
                        cause: 'All Observable object need to subscribe within 2 second'
                    })
                }, 2000)
                return new Observable(o => {
                    clearTimeout(tid)
                    if (!this.#requests.has(session_id)) {
                        return o.next('RPC_OFFLINE')
                    }
                    const channel_id = randomUUID()
                    const s = new Subject<PureData>()
                    req.channels.set(channel_id, s)
                    s.subscribe(o)
                    this.#subscribe_channel(req.target_node_id, session_id, observable_id, channel_id)
                    return () => {
                        this.#unsubcribe_channel(req.target_node_id, session_id, channel_id)
                    }
                })
            }
        )
        return [result, is_pure] as [T, boolean]
    }


    async #on_rpc(msg: RpcRequest, sender_node_id: string) {

        const instance = this.#get_rpc_target(msg)

        this.#requests.set(msg.session_id, {
            observables: new Map(),
            target_node_id: sender_node_id,
            subscriptions: new Map(),
            channels: new Map()
        })

        const req = this.#requests.get(msg.session_id)
        if (!req) return



        const channel_id = msg.session_id
        try {
            const [args] = this.#decode_rpc_buffer<any[]>(req, msg.args, msg.session_id)
            if (!args) return this.#stream_error(
                sender_node_id,
                msg.session_id,
                channel_id,
                Buffer.from('INVAILD_ARGS')
            )
            const result = await instance?.[msg.method]?.(...args)
            const { buffer, observables } = Encoder.encode(await result)
            for (const [observable_id, o] of observables) req.observables.set(observable_id, o)
            this.#stream_next(sender_node_id, msg.session_id, channel_id, buffer)
        } catch (error) {
            if (error instanceof Error) {
                this.#stream_error(
                    sender_node_id,
                    msg.session_id,
                    channel_id,
                    Encoder.encode(JSON.stringify(error)).buffer
                )
            }
        }

    }

    #stream_next(node_id: string, session_id: string, channel_id: string, data: PureData) {
        return this.publish<RpcStream>(
            node_id,
            {
                type: MessageType.RpcStream,
                channel_id,
                session_id,
                data
            },
            node_id
        )
    }

    #stream_error(node_id: string, session_id: string, channel_id: string, error: PureData) {
        return this.publish<RpcStream>(
            node_id,
            {
                type: MessageType.RpcStream,
                channel_id,
                session_id,
                error
            },
            node_id
        )
    }

    #stream_complete(node_id: string, session_id: string, channel_id: string) {
        return this.publish<RpcStream>(
            node_id,
            {
                type: MessageType.RpcStream,
                channel_id,
                completed: true,
                session_id
            },
            node_id
        )
    }



    #subscribe_channel(node_id: string, session_id: string, observable_id: string, channel_id: string,) {
        return this.publish<RpcSubscribeChannel>(
            node_id,
            {
                type: MessageType.RpcSubscribeChannel,
                channel_id,
                session_id,
                observable_id
            },
            node_id
        )
    }

    #on_subscribe_channel(msg: RpcSubscribeChannel, sender_node_id: string) {


        const response = (e: Omit<RpcStream, 'session_id' | 'channel_id' | 'type'>) => {

            this.publish<RpcStream>(
                sender_node_id,
                {
                    session_id: msg.session_id,
                    channel_id: msg.channel_id,
                    type: MessageType.RpcStream,
                    ...e
                },
                sender_node_id
            )
        }

        const req = this.#requests.get(msg.session_id)
        if (!req) {
            response({ error: 'RPC_SESSION_EXPIRED' })
            return
        }


        const o = req.observables.get(msg.observable_id)
        if (!o) return
        const subscription = o.pipe(
            finalize(() => {
                if (req.subscriptions.size == 0 && req.channels.size == 0) {
                    this.#requests.delete(msg.session_id)
                }
            })
        ).subscribe({
            complete: () => this.#stream_complete(sender_node_id, msg.session_id, msg.channel_id),
            error: error => this.#stream_error(sender_node_id, msg.session_id, msg.channel_id, error),
            next: data => this.#stream_next(sender_node_id, msg.session_id, msg.channel_id, data)
        })
        req.subscriptions.set(msg.channel_id, subscription)

    }
    #unsubcribe_channel(node_id: string, session_id: string, channel_id: string,) {
        return this.publish<RpcUnsubscribeChannel>(
            node_id,
            {
                type: MessageType.RpcUnsubscribeChannel,
                channel_id,
                session_id
            },
            node_id
        )
    }

    #on_unsubscribe_channel(msg: RpcUnsubscribeChannel) {

        const req = this.#requests.get(msg.session_id)
        if (!req) return

        req.subscriptions.get(msg.channel_id)?.unsubscribe()
        req.subscriptions.delete(msg.channel_id)

        if (req.channels.size == 0 && req.subscriptions.size == 0) {
            this.#requests.delete(msg.session_id)
        }
    }


    #on_stream(stream: RpcStream) {
        const req = this.#requests.get(stream.session_id)
        if (!req) return


        const $ = req.channels.get(stream.channel_id)
        if (!$) return



        if (stream.data != undefined) {
            $.next(stream.data)
        }
        if (stream.completed || stream.error) {
            stream.error && $.error(stream.error)
            stream.completed && $.complete()
            req.channels.delete(stream.channel_id);
            if (req.channels.size == 0 && req.subscriptions.size == 0) {
                this.#requests.delete(stream.session_id)
            }
        }
    }

    async $metadata() {
        const ips = (
            Object.values(networkInterfaces())
                .flat(2)
                .filter(a => a && !a.internal && a.address)
                .map(a => a?.address as string)
        )
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
                .map(node => node.id))
        ]

        const metadata: SpiderMeshNodeMetadata = {
            id: this.transporter.node_id,
            name: PACKAGE_JSON?.name || 'UNKNOWN',
            version: PACKAGE_JSON?.version || '1.0.0',
            path: process.cwd(),
            uptime: process.uptime(),
            hostname: os.hostname(),
            plaform: os.platform(),
            node_version: process.version,
            public_ip: await this.#public_ip,
            ip_addresses: ips,
            last_online: Date.now(),
            online: true,
            services,
            namespace: this.transporter.namespace,
            linked: [...this.#linked_nodes.keys()],
            isolated_nodes,
            isolated: this.#$isolated.value,
        }
        return metadata
    }

    async #self_introduce(node_id?: string) {
        const me = await this.$metadata()
        await this.publish<SpiderMeshNodeMetadata[]>('#join', [me], node_id)
        return me
    }

    async $update_isolate_mode(active: boolean) {
        this.#$isolated.next(active)
        const me = await this.#self_introduce()
        return me
    }

    #node_event_handler({ data: msg, sender_node_id }: { data: RpcEvent, sender_node_id: string }) {
        if (msg.type == MessageType.RpcRequest) return this.#on_rpc(msg, sender_node_id)
        if (msg.type == MessageType.RpcStream) return this.#on_stream(msg)
        if (msg.type == MessageType.RpcSubscribeChannel) return this.#on_subscribe_channel(msg, sender_node_id)
        if (msg.type == MessageType.RpcUnsubscribeChannel) return this.#on_unsubscribe_channel(msg)
    }

    async #init() {

        // Init transporter
        await this.transporter.start()

        // Listen RPC
        this.listen<RpcEvent>(this.transporter.node_id).subscribe(
            msg => this.#node_event_handler(msg)
        )

        // Listen new node
        this.listen<SpiderMeshNode>('#join')
            .pipe(
                groupBy(node => node.sender_node_id),
                mergeMap(grouped => grouped.pipe(debounceTime(1000))),
                mergeMap(({ data }) => this.#on_node_discovered(data), 1),
            )
            .subscribe()

        // Sync node status
        this.transporter.$nodes_status.pipe(
            tap(node => !node.online && this.#on_node_offline(node.node_id)),
            groupBy(node => node.node_id),
            mergeMap(grouped => grouped.pipe(debounceTime(1000))),
            filter(node => node.online),
            mergeMap(({ node_id }) => this.#self_introduce(node_id), 1)
        )
            .subscribe()

        serviceInstanceList.pipe(
            filter(i => i.namespace == this.transporter.namespace),
            mergeMap(async ({ instance, metadata }) => {
                await this.#active_local_service(instance, metadata)
                await this.#active_ready_hooks(instance)
            }),
            debounceTime(1000),
            mergeMap(() => this.#self_introduce())
        ).subscribe()
    }

    async #on_node_discovered(node: SpiderMeshNode) {
        if (node.id == this.transporter.node_id) return
        if (this.#$isolated.value) return
        const saved_node = this.#linked_nodes.get(node.id)
        if (saved_node && saved_node.last_online > node.last_online) return
        const peer_updated = node.linked.includes(this.transporter.node_id)
        const new_node: SpiderMeshNode = {
            ...node,
            online: true
        }

        if (!peer_updated) await this.#self_introduce(node.id)
        this.#linked_nodes.set(node.id, new_node);

        for (const service_id of Object.keys(node.services)) {
            if (!this.#remote_services.has(service_id)) {
                this.#remote_services.set(service_id, {
                    last_call_index: -1,
                    nodes: []
                })
            }
            const $service = this.#remote_services.get(service_id)!
            const index = $service.nodes.findIndex(n => n.id == new_node.id)
            index >= 0 ? ($service.nodes[index] = new_node) : $service.nodes.push(new_node)
        }

        this.$nodes_monitor.next(node)

    }

    #on_node_offline(id: string) {

        // Remove remote node from linked remote services
        for (const service of this.#remote_services.values()) {
            service.nodes = service.nodes.filter(node => node.id != id)
        }

        // Throw errors fro requests
        for (const [rid, { target_node_id, channels, observables, subscriptions }] of this.#requests) {
            if (target_node_id == id) {
                for (const [_, $] of channels) $.error(new Error('SERVICE_OFFLINE'))
                subscriptions.forEach(s => s.unsubscribe())
                this.#requests.delete(rid)
            }
        }

        const node = this.#linked_nodes.get(id)
        node && this.$nodes_monitor.next({ ...node, online: false })
        this.#linked_nodes.delete(id)

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
                                        { $node_id: node.id } as RPCOptions
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

    async link_event<T extends Encodeable = Encodeable>(event_factory: { new(...args: any[]): T }, publish_buffer_ms?: number) {
        const $ = new Subject<T>()
        const $$: Observable<T | T[]> = publish_buffer_ms ? $.pipe(bufferTime(publish_buffer_ms), filter(l => l.length > 0)) : $
        $$.subscribe(data => this.publish(event_factory.name, data))

        return {
            publish: async (data: T) => $.next(data),
            listen: () => this.listen<T>(event_factory)
        }

    }

    async #active_local_service(instance: any, metadata: ServiceMetadata) {

        const prototype = Object.getPrototypeOf(instance)
        const name = prototype.constructor.name

        this.#local_services.set(name, { instance, metadata })
        this.listen<RpcEvent>(name).subscribe(evt => this.#node_event_handler(evt))


        // Active event subscribers
        const event_subscribers = listEventSubscribers(prototype)
        for (const { event, method, buffer_ms } of event_subscribers) {
            const $ = this.listen(event).pipe(filter(() => !this.#$isolated.value))
            const $$: Observable<any> = buffer_ms ? $.pipe(bufferTime(buffer_ms), filter(l => l.length > 0)) : $;
            $$.subscribe(e => instance[method]?.(e, this))
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

    async publish<T extends Encodeable>(topic: string | { new(): EventHub<T> }, payload: T, node_id?: string) {
        await this.#initing
        const event = typeof topic == 'string' ? topic : topic.name
        const data = Array.isArray(payload) ? payload : [payload]
        this.transporter.publish({
            event,
            data,
            node_id
        })
    }

    listen<T extends Encodeable = Encodeable>(topic: string | { new(...args: any[]): T }) {
        const topic_name = typeof topic == 'string' ? topic : topic.name
        return this.transporter.listen<T>(topic_name).pipe(
            map(e => {
                const data = e.data
                if (!Array.isArray(data)) return []
                return data.map(
                    data => (
                        {
                            data: data as T,
                            sender_node_id: e.sender_node_id
                        }
                    )
                )
            }),
            mergeAll()
        )
    }


}
