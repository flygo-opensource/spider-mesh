import { randomUUID } from 'crypto'
import { get } from 'http'
import { networkInterfaces } from 'os'
import { DeepProxy } from './decorators/DeepProxy.js'
import { SpiderMeshNode, SpiderMeshNodeMetadata } from './interfaces/SpiderMeshNode.js'
import { SpiderMeshTransporter, SpiderMeshTransporterEvent } from './interfaces/SpiderMeshTransporter.js'
import { RPCOptions, RPCOptionsList } from './RPCOptions.js'
import { RemoteService } from './interfaces/RemoteService.js'
import os from 'os'
import { EventHub, listEventSubscribers } from './decorators/ListenEvent.js'
import { listReadyHookMethods } from './decorators/OnMicroserviceReady.js'
import { BehaviorSubject, Observable, Subject, bufferTime, filter, from, map, mergeAll, mergeMap, tap, throttleTime } from 'rxjs'
import { readFileSync } from 'fs'
import { serviceInstanceList } from './decorators/Microservice.js'
import { sleep } from './helpers/sleep.js'
import { BuiltinTransporter } from './builtin-transporter/BuiltinTransporter.js'
import { DEBUG, NAMEPSACE, NODE_ID } from './const.js'



export type RpcPayload = {
    id: string,
    type: string,
    args: any[],
    service: string,
    method: string,
    response: any,
    error: any
    index: number
    event: string
    data: any
    nervermind: boolean
}


export class SpiderMeshConfig {
    namespace?: string
    node_id?: string
    transporter: SpiderMeshTransporter
}

const PACKAGE_JSON = JSON.parse(readFileSync(`package.json`, 'utf8')) || {}

export type SpiderMeshNamespace = string


export class SpiderMesh {


    #$isolated = new BehaviorSubject<boolean>(false)

    #local_rpc_services = new Map<string, any>()

    #remote_nodes = new Map<string, SpiderMeshNode>

    #remote_rpc_services = new Map<string, {
        last_call_index: number
        nodes: SpiderMeshNode[]
    }>

    #rpc_queue = new Map<string, {
        success: Function,
        reject: Function,
        args: any[],
        request_time: number
        timeout: number
        last_ping: number
        target_node_id: string
    }>

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

    constructor(private transporter: SpiderMeshTransporter = new BuiltinTransporter(NODE_ID, NAMEPSACE)) {
        this.#initing = this.#init()
    }

    static add_transporter(transporter: SpiderMeshTransporter) {
        new this(transporter)
    }

    async $metadata(revalidate_on_join?: boolean) {
        const ips = Object.values(networkInterfaces()).map(itf => itf?.map(ip => ip.address) || []).flat(2)
        const metadata = {
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
            active: true,
            online: true,
            services: [...this.#local_rpc_services.keys()],
            namespace: this.transporter.namespace,
            linked: [...this.#remote_nodes.keys()],
            isolated_nodes: [... this.#remote_nodes.values()].filter(node => node.isolate).map(node => node.id),
            isolate: this.#$isolated.value,
            revalidate_on_join
        } as SpiderMeshNodeMetadata
        return metadata
    }

    async $update_isolate_mode(active: boolean) {
        this.#$isolated.next(active)
        const me = await this.$metadata()
        await this.publish('#join', me)
        return me
    }

    async #node_event_handler({ data: msg, sender_node_id }: SpiderMeshTransporterEvent<RpcPayload>) {



        if (msg.type == 'rpc') {
            if (msg.service == 'SpiderMesh' && !msg.method.startsWith('$')) {
                sender_node_id && this.transporter.publish({
                    data: [{
                        id: msg.id,
                        type: 'error',
                        error: 'NOT_ALLOW'
                    }],
                    event: sender_node_id,
                    node_id: sender_node_id
                })
                return
            }
            const instance = msg.service == 'SpiderMesh' ? this : this.#local_rpc_services.get(msg.service)

            if (!instance) return this.transporter.publish({
                data: [{
                    id: msg.id,
                    type: 'error',
                    error: 'SERVICE_NOT_FOUND'
                }],
                event: sender_node_id,
                node_id: sender_node_id
            })

            const args = msg.args.map(arg => arg != '__FUNCTION__' ? arg : (...args) => {
                sender_node_id && this.transporter.publish({
                    event: sender_node_id,
                    node_id: sender_node_id,
                    data: [{ id: msg.id, type: 'callback', args }]
                })
            })
            try {
                const response = await instance?.[msg.method]?.(...args)
                sender_node_id && this.transporter.publish({
                    event: sender_node_id,
                    node_id: sender_node_id,
                    data: [{ id: msg.id, type: 'response', response }]
                })
            } catch (error) {
                const { code, message } = error as any
                sender_node_id && this.transporter.publish({
                    event: sender_node_id,
                    node_id: sender_node_id,
                    data: [{ id: msg.id, type: 'error', error: code || message || error }]
                })
            }
            return
        }

        const request = this.#rpc_queue.get(msg.id)
        if (!request) return

        if (msg.type == 'callback') {
            request.args?.[msg.index]?.()
            return
        }

        if (msg.type == 'ack') {
            request.last_ping = Date.now()
            return
        }

        if (msg.type == 'error') {
            request.reject(msg.error)
            this.#rpc_queue.delete(msg.id)
            return
        }

        if (msg.type == 'response') {
            request.success(msg.response)
            this.#rpc_queue.delete(msg.id)
            return
        }
    }

    async #init() {

        // Init transporter
        await this.transporter.start()

        // Listen RPC
        this.listen<RpcPayload>(this.transporter.node_id).subscribe(
            evt => this.#node_event_handler(evt)
        )

        // Listen new node
        this.listen<SpiderMeshNode>('#join').subscribe(({ data }) => {
            this.#on_node_discovered(data)
        })

        // Manage nodes
        this.transporter.$nodes_status.subscribe(async ({ node_id, online }) => {
            !online && this.#on_node_offline(node_id)
            online && this.transporter.publish({
                event: '#join',
                data: [await this.$metadata()],
                node_id
            })
        })

        serviceInstanceList.pipe(
            filter(i => i.namespace == this.transporter.namespace),
            mergeMap(async ({ instance }) => {
                await this.#active_local_service(instance)
                await this.#active_ready_hooks(instance)
            }),
            throttleTime(1000),
            mergeMap(async () => {
                const me = await this.$metadata()
                await this.publish('#join', me)
            })
        ).subscribe()
    }

    async #on_node_discovered(node: SpiderMeshNode) {

        if (node.id == this.transporter.node_id) return
        if (this.#$isolated.value) return

        const peer_updated = node.linked.includes(this.transporter.node_id)
        DEBUG && console.log(`[${new Date().toLocaleString()}:${new Date().getMilliseconds()}] New node `, node, { peer_updated })

        const discovered = this.#remote_nodes.get(node.id)
        const new_node: SpiderMeshNode = {
            ...discovered || {},
            ...node,
            online: true
        }
        this.#remote_nodes.set(node.id, new_node)
        this.$nodes_monitor.next(node);

        (!peer_updated || node.revalidate_on_join) && await this.transporter.publish({
            event: '#join',
            data: [await this.$metadata(!peer_updated)],
            node_id: node.id
        })

        peer_updated && node.services.forEach(service => {

            const $service = this.#remote_rpc_services.get(service)

            if (!$service) {
                this.#remote_rpc_services.set(service, {
                    last_call_index: -1,
                    nodes: [node]
                })
                return
            }

            if ($service.nodes) {
                const index = $service.nodes.findIndex(n => n.id == new_node.id)
                index >= 0 ? ($service.nodes[index] = new_node) : $service.nodes.push(new_node)
            } else {
                $service.nodes = [new_node]
            }

        })





    }

    async #on_node_offline(id: string) {
        const node = this.#remote_nodes.get(id)
        if (!node) return
        node.online = false
        DEBUG && console.log(`[${new Date().toLocaleString()}] Node ${id} offline`)
        this.$nodes_monitor.next(node)

        this.#remote_rpc_services.forEach(service => {
            service.nodes = service.nodes?.filter(node => node.id != id)
        });
        [...this.#rpc_queue.entries()].forEach(([rid, { reject }]) => {
            reject(new Error('SERVICE_OFFLINE'))
            this.#rpc_queue.delete(rid)
        })
    }

    #caculate_rpc_node_id(service_name: string, fixed_node_id?: string) {
        const current = this.#remote_rpc_services.get(service_name)
        if (!current || current.nodes.length == 0) return
        if (fixed_node_id) {
            if (current.nodes.some(node => node.id == fixed_node_id)) return fixed_node_id
            return
        }
        const index = ++current.last_call_index % current.nodes.length
        return current.nodes[index].id

    }

    async rpc<T = any>(service: string, method: string, args: any, options: RPCOptions) {
        await this.#initing
        return await new Promise<T>(async (success, r) => {

            const reject = (err) => (options.$fallback !== undefined) ? success(options.$fallback) : r(err);

            const rid = randomUUID();

            options.$timeout && setTimeout(() => {
                reject(new Error('TIMEOUT'))
                this.#rpc_queue.delete(rid)
            }, options.$timeout)

            !options.$nevermind && this.#rpc_queue.set(rid, {
                args,
                last_ping: 0,
                reject,
                request_time: Date.now(),
                success,
                timeout: options.$timeout,
                target_node_id: options.$node_id
            })

            const retry_count = options.$retry || 1
            for (let i = retry_count; i > 0; i--) {
                try {
                    const node_id = this.#caculate_rpc_node_id(service, options.$node_id)
                    if (!node_id) return reject(Object.assign(new Error(`SERVICE_INSTANCE_NOT_FOUND`), { service }))
                    await this.publish(service, { type: 'rpc', id: rid, args, method, service }, node_id)
                    return
                } catch (e) { }

                i > 1 && await new Promise(
                    s => setTimeout(s, options.$retry_delay || 1000)
                )
            }
            if (options.$fallback) return success(options.$fallback)
        })
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
            '__proto__'
        ])

        const actions = new Set<string>()
        for (let f = factory.prototype; f != null; f = Object.getPrototypeOf(f)) {
            for (const method of Object.getOwnPropertyNames(f).filter(method => !omit_properties.has(method))) {
                typeof f[method] == 'function' && actions.add(method)
            }
        }
        if (!this.#remote_rpc_services.has(service_name)) {
            this.#remote_rpc_services.set(service_name, { last_call_index: -1, nodes: [] })
        }

        wait_service_online && await this.#wait_service_online(service_name)

        return new Proxy({}, {
            get: (_, method: string) => {
                if (method == '$wait_service_online') {
                    return () => this.#wait_service_online(service_name)
                }

                if (method == '$watch') return () => this.$nodes_monitor.pipe(
                    filter(node => node.services.includes(service_name))
                )

                if (method == '$list_nodes') {
                    return (
                        () => [...this.#remote_nodes.values()].filter(
                            node => node.services.includes(service_name) && !node.isolate
                        )
                    ) as RemoteService<T>['$list_nodes']
                }

                if (method.startsWith('$batch_')) {
                    const real_method = method.split('$batch_')?.[1]
                    if (!real_method || !actions.has(real_method)) return null
                    const nodes = [...this.#remote_nodes.values()].filter(node => node.services.includes(service_name) && !node.isolate)
                    return (...args) => from(nodes).pipe(
                        mergeMap(async node => {
                            try {
                                const data = await this.rpc(
                                    service_name,
                                    real_method,
                                    args,
                                    { $node_id: node.id } as RPCOptions
                                )
                                return { data, node, error: null }
                            } catch (error) {
                                return { node, data: null, error }
                            }
                        })
                    )
                }

                if (actions.has(method) || method.startsWith('$')) {
                    return new DeepProxy(
                        method => RPCOptionsList.has(method),
                        (method: string, options) => (
                            async (...args) => {
                                const safe = options.$safe_mode
                                try {
                                    const data = await this.rpc(service_name, method, args, options)
                                    return safe ? [null, data] : data
                                } catch (e) {
                                    if (safe) return [e, null]
                                    throw e
                                }
                            }
                        )
                    ).nest()[method]
                }

                return null
            }
        }) as RemoteService<T>

    }

    async link_event<T>(event_factory: { new(...args: any[]): T }, publish_buffer_ms?: number) {
        const $ = new Subject<T>()
        const $$: Observable<T | T[]> = publish_buffer_ms ? $.pipe(bufferTime(publish_buffer_ms), filter(l => l.length > 0)) : $
        $$.subscribe(data => this.publish(event_factory.name, data))

        const event_hub: EventHub<T> = {
            publish: async (data: T) => $.next(data),
            listen: () => this.listen<T>(event_factory.name)
        }
        return event_hub
    }

    async #active_local_service(instance: any) {

        const prototype = Object.getPrototypeOf(instance)
        const name = prototype.constructor.name

        this.#local_rpc_services.set(name, instance)
        this.listen(name).subscribe(evt => this.#node_event_handler(evt))


        // Active event subscribers
        const event_subscribers = listEventSubscribers(prototype)
        for (const { event, method, buffer_ms } of event_subscribers) {
            const $ = this.listen(event).pipe(filter(() => !this.#$isolated.value))
            const $$: Observable<any> = buffer_ms ? $.pipe(bufferTime(buffer_ms), filter(l => l.length > 0)) : $;
            $$.subscribe(e => instance[method]?.(e))
        }


    }

    async #wait_service_online(service_name: string = 'all') {
        if (this.#remote_rpc_services.size == 0) return

        while (true) {
            await sleep(500)
            const nodes = service_name == 'all' ? (
                [...this.#remote_rpc_services.values()].map(s => s.nodes).flat(2)
            ) : (
                this.#remote_rpc_services.get(service_name)?.nodes || []
            )
            if (nodes.length > 0) break
        }
    }

    async #active_ready_hooks(instance: any) {

        // Wait remote service ready
        await this.#wait_service_online()

        // Active ready hook
        for (const { method } of listReadyHookMethods(Object.getPrototypeOf(instance))) {
            instance[method]?.()
        }
    }

    async publish<T = any>(topic: string | { new(): EventHub<T> }, payload: T, node_id?: string) {
        await this.#initing
        const event = typeof topic == 'string' ? topic : topic.name
        const data = Array.isArray(payload) ? payload : [payload]
        this.transporter.publish({ event, data, node_id })
    }

    listen<T = any>(topic: string | { new(): EventHub<T> }) {
        const topic_name = typeof topic == 'string' ? topic : topic.name
        return this.transporter.listen<T[]>(topic_name).pipe(
            map(e => {
                if (!Array.isArray(e.data)) return []
                return e.data.map(
                    data => (
                        {
                            data,
                            sender_node_id: e.sender_node_id
                        } as SpiderMeshTransporterEvent<T>
                    )
                )
            }),
            mergeAll()
        )
    }

}
