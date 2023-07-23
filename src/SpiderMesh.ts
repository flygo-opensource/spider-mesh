import { randomUUID } from 'crypto'
import { get } from 'http'
import { networkInterfaces } from 'os'
import { DeepProxy } from './DeepProxy'
import { SpiderMeshNode, SpiderMeshNodeMetadata } from './SpiderMeshNode'
import { SpiderMeshTransporter, SpiderMeshTransporterEvent } from './SpiderMeshTransporter'
import { RPCOptions, RPCOptionsList } from './RPCOptions'
import { RemoteService } from './RemoteService'
import os from 'os'
import { listEventSubscribers } from './decorators/SubscribeEvent'
import { listReadyHookMethods } from './decorators/OnMicroserviceReady'
import { BehaviorSubject } from 'rxjs'
import { readFileSync } from 'fs'
import { serviceInstanceList } from './decorators/Microservice'


export type ServiceNodeMonitor = (online: boolean, node: SpiderMeshNode) => any

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

export class SpiderMesh {


    #LinkingServices = new Map<string, { online: boolean }>()
    #$isolated = new BehaviorSubject<boolean>(false)
    #local_services = new Map<string, any>()
    #remote_services = new Map<string, {
        last_call_index: number
        nodes: SpiderMeshNode[]
    }>

    #nodes = new Map<string, SpiderMeshNode>


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


    #services_status_monitor = new Map<string, Map<string, ServiceNodeMonitor>>()

    constructor(private transporter: SpiderMeshTransporter) {
    }

    async $metadata() {
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
            offline: false,
            services: [...this.#local_services.keys()],
            namespace: this.transporter.namespace,
            linked: [...this.#nodes.keys()],
            isolated_nodes: [... this.#nodes.values()].filter(node => node.isolate).map(node => node.id),
            isolate: this.#$isolated.value
        } as SpiderMeshNodeMetadata

        process.env.SPIDERMESH_DEBUG && console.log({ me: metadata })
        return metadata
    }

    async $update_isolate_mode(active: boolean) {
        this.#$isolated.next(active)
        const me = await this.$metadata()
        await this.transporter.publish({
            event: '#join',
            data: me
        })
        return me
    }

    async #active_node_responder({ data: msg, sender_node_id }: SpiderMeshTransporterEvent<RpcPayload>) {

        if (msg.type == 'rpc') {
            if (msg.service == 'SpiderMesh' && !msg.method.startsWith('$')) {
                sender_node_id && this.transporter.publish({
                    data: {
                        id: msg.id,
                        type: 'error',
                        error: 'NOT_ALLOW'
                    },
                    event: sender_node_id,
                    node_id: sender_node_id
                })
                return
            }
            const instance = msg.service = 'SpiderMesh' ? this : this.#local_services.get(msg.service)

            const args = msg.args.map(arg => arg != '__FUNCTION__' ? arg : (...args) => {
                sender_node_id && this.transporter.publish({
                    event: sender_node_id,
                    node_id: sender_node_id,
                    data: { id: msg.id, type: 'callback', args }
                })
            })
            try {
                const response = await instance?.[msg.method]?.(...args)
                sender_node_id && this.transporter.publish({
                    event: sender_node_id,
                    node_id: sender_node_id,
                    data: { id: msg.id, type: 'response', response }
                })
            } catch (error) {
                const { code, message } = error as any
                sender_node_id && this.transporter.publish({
                    event: sender_node_id,
                    node_id: sender_node_id,
                    data: { id: msg.id, type: 'error', error: code || message || error }
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

    async start() {

        // Init transporter
        await this.transporter.start()

        // Listen RPC
        this.transporter.listen<RpcPayload>(this.transporter.node_id).subscribe(
            evt => this.#active_node_responder(evt)
        )

        // Listen new node
        this.transporter.listen<SpiderMeshNode>('#join').subscribe(({ data, sender_node_id }) => {
            this.#on_node_discovered(data)
        })

        // Listen offline node
        this.transporter.$nodes_status.subscribe(async ({ node_id, online }) => {
            !online && this.#on_node_offline(node_id)
        })


        // Active local services
        const instances = serviceInstanceList.filter(i => i.namespaces.includes(this.transporter.namespace || 'default'))
        for (const instance of instances) await this.#active_local_service(instance)


        // Broadcast running services
        const me = await this.$metadata()
        this.publish('#join', me)


        // Active ready hooks
        for (const instance of instances) await this.#active_ready_hooks(instance)
    }

    async #on_node_discovered(node: SpiderMeshNode) {

        if (node.id == this.transporter.node_id) return
        if (this.#$isolated.value) return

        process.env.SPIDERMESH_DEBUG && console.log(`[${new Date().toLocaleString()}] New node `, node)

        const discovered = this.#nodes.get(node.id)
        const new_node: SpiderMeshNode = {
            ...discovered || {},
            ...node,
            offline: false
        }
        this.#nodes.set(node.id, new_node)

        node.services.forEach(service => {
            this.#LinkingServices.set(service, { online: true })
            this.#services_status_monitor.get(service)?.forEach(
                fn => fn(true, new_node)
            )

            const $service = this.#remote_services.get(service)

            if (!$service) {
                this.#remote_services.set(service, {
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


        !node.linked.includes(this.transporter.node_id) && await this.transporter.publish({
            event: '#join',
            data: await this.$metadata()
        })

    }

    async #on_node_offline(id: string) {
        const node = this.#nodes.get(id)
        if (!node) return
        process.env.SPIDERMESH_DEBUG && console.log(`[${new Date().toLocaleString()}] Node ${id} offline`)
        node.services.map(service => {
            this.#services_status_monitor.get(service)?.forEach(
                fn => fn(false, node)
            )
        })
        this.#remote_services.forEach(service => {
            service.nodes = service.nodes?.filter(node => node.id != id)
        });
        [...this.#rpc_queue.entries()].forEach(([rid, { reject }]) => {
            reject(new Error('SERVICE_OFFLINE'))
            this.#rpc_queue.delete(rid)
        })
    }

    async rpc(service: string, method: string, args: any, options: RPCOptions) {

        return await new Promise<any>(async (success, r) => {

            const reject = (err) => (options.fallback !== undefined) ? success(options.fallback) : r(err);

            const rid = randomUUID();

            (!options.timeout || options.timeout > 0) && setTimeout(() => {
                reject(new Error('TIMEOUT'))
                this.#rpc_queue.delete(rid)
            }, options.timeout || 10000)

            !options.nevermind && this.#rpc_queue.set(rid, {
                args,
                last_ping: 0,
                reject,
                request_time: Date.now(),
                success,
                timeout: options.timeout,
                target_node_id: options.node_id
            })
            const retry_count = options.retry || 1
            for (let i = retry_count; i > 0; i--) {
                try {
                    await this.transporter.publish({
                        event: service,
                        node_id: options.node_id,
                        routing_key: method,
                        data: { type: 'rpc', id: rid, args, method, service }
                    })
                    return
                } catch (e) {

                }
                i > 1 && await new Promise(s => setTimeout(s, options.retry_delay || 1000))
            }
            if (options.fallback) return success(options.fallback)
        })
    }

    link_remote_service<T>(factory: { new(...args: any[]): T }) {

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

        this.#LinkingServices.set(service_name, { online: false })

        return new Proxy({}, {
            get: (_, method: string) => {

                if (method == '$wait') return (
                    async (cb, delay: number = 1000) => {
                        for (let i = 0; true; i++) {
                            const nodes = [...this.#nodes.values()].filter(node => node.services.includes(service_name) && !node.isolate)
                            const result = await cb(nodes, i)
                            if (result) return result
                            await new Promise(s => setTimeout(s, delay))
                        }
                    }
                ) as RemoteService<T>['$wait']

                if (method == '$list_nodes') return (
                    () => [...this.#nodes.values()].filter(node => node.services.includes(service_name) && !node.isolate)
                ) as RemoteService<T>['$list_nodes']

                if (method == '$monitor') return (
                    cb => {
                        const hid = randomUUID()
                        const map = this.#services_status_monitor.get(service_name) || new Map<string, ServiceNodeMonitor>()
                        map.set(hid, cb)
                        this.#services_status_monitor.set(service_name, map)
                        return {
                            unsubscribe: () => map.delete(hid)
                        }
                    }
                ) as RemoteService<T>['$monitor']

                if (actions.has(method) || method.startsWith('$set_')) {
                    return new DeepProxy(
                        RPCOptionsList,
                        (method: string, options) => (...args) => this.rpc(service_name, method, args, options)
                    ).nest()[method]
                }

                return null
            }
        }) as RemoteService<T>
    }

    async #active_local_service(instance: any) {

        const prototype = Object.getPrototypeOf(instance)
        const name = prototype.constructor.name

        this.#local_services.set(name, instance)


        // Active event subscribers
        const event_subscribers = listEventSubscribers(prototype)
        for (const { event, method } of event_subscribers) {
            this.transporter.listen(event).subscribe(evt => instance[method]?.(evt.data, evt.sender_node_id))
        }


    }

    async #active_ready_hooks(instance: any) {
        // Wait remote service ready
        while (true) {
            await new Promise(s => setTimeout(s, 1000))
            if ([...this.#LinkingServices.values()].every(service => service.online)) {
                break
            }
        }

        // Active ready hook
        for (const method of listReadyHookMethods(Object.getPrototypeOf(instance))) {
            instance[method]?.()
        }
    }

    async publish<T = any>(event: string, data: T) {
        this.transporter.publish({ event, data })
    }

    async subscribe<T = any>(topic: string) {
        return this.transporter.listen<T>(topic)
    }

}
