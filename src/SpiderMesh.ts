import { randomUUID } from 'crypto'
import { get } from 'http'
import { readFileSync, existsSync } from 'fs'
import { networkInterfaces, type } from 'os'
import { DeepProxy } from './DeepProxy'
import { SpiderMeshNode, SpiderMeshNodeMetadata } from './SpiderMeshNode'
import { SpiderMeshTransporter, SpiderMeshTransporterFactory } from './SpiderMeshTransporter'
import { RPCOptions, RPCOptionsList } from './RPCOptions'
import { RemoteService } from './RemoteService'
import os from 'os'
import { BuiltinTransporter } from './BuiltinTransporter'
import { listEventSubscribers } from './decorators/createMicroserviceEvent'
import { listReadyHookMethods } from './decorators/OnMicroserviceReady'

export class SpiderMesh {

    #package_json = existsSync(`package.json`) ? JSON.parse(readFileSync(`package.json`, 'utf8')) || {} : {}
    public readonly node_id = randomUUID()
    public readonly namespace = process.env.NAMESPACE || this.#package_json.name || 'default'

    #local_services = new Map<string, any>()
    #remote_services = new Map<string, {
        last_call_index: number
        nodes: SpiderMeshNode[]
    }>

    #nodes = new Map<string, SpiderMeshNode>

    #transporters = new Map<string, SpiderMeshTransporter>

    #rpc_queue = new Map<string, {
        success: Function,
        reject: Function,
        args: any[],
        request_time: number
        timeout: number
        last_ping: number
        target_node_id: string
    }>

    #public_ip = new Promise<string | null>(s => {
        get('http://api.ipify.org', (res) => {
            res.setEncoding('utf8')
            let rawData = ''
            res.on('data', (chunk) => { rawData += chunk; })
            res.on('end', () => s(rawData))
        }).on('error', () => s(null))
    })

    static #LinkingServices = new Map<string, { online: boolean }>()

    private constructor() { }

    static async init(...transporters: SpiderMeshTransporterFactory[]) {
        const ms = new this()
        transporters.length == 0 && transporters.push(BuiltinTransporter)
        for (const transporter of transporters) {
            await ms.add_transporter(transporter)
        }
        return ms
    }

    async $metadata() {
        const ips = Object.values(networkInterfaces()).map(itf => itf?.map(ip => ip.address) || []).flat(2)
        const { name, version } = this.#package_json
        return {
            id: this.node_id,
            name,
            version,
            path: process.cwd(),
            argv: process.argv,
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
            namespace: this.namespace,
            linked: [...this.#nodes.keys()],
        } as SpiderMeshNodeMetadata
    }

    async add_transporter(factory: { new(...args): SpiderMeshTransporter }) {
        const transporter = new factory(this.node_id, this.namespace)
        this.#transporters.set(factory.name, transporter)
        transporter.on_node_offline(id => {
            const node = this.#nodes.get(id)
            if (!node) return
            process.env.SPIDERMESH_DEBUG && console.log(`Node ${id} offline`)
            node.transporters?.delete(factory.name)
            node.transporters?.size == 0 && this.#nodes.delete(id)
            this.#remote_services.forEach(service => service.nodes = service.nodes?.filter(node => node.id != id));
            [...this.#rpc_queue.entries()].forEach(([rid, { reject }]) => {
                reject(new Error('SERVICE_OFFLINE'))
                this.#rpc_queue.delete(rid)
            })
        })

        transporter.listen<{
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
        }>(this.node_id, async (sender_node_id, msg) => {


            if (msg.type == 'rpc') {
                const instance = this.#local_services.get(msg.service)

                const args = msg.args.map(arg => arg != '__FUNCTION__' ? arg : (...args) => {
                    sender_node_id && transporter.publish(sender_node_id, sender_node_id, { id: msg.id, type: 'callback', args })
                })
                try {
                    const response = await instance?.[msg.method]?.(...args)
                    sender_node_id && transporter.publish(sender_node_id, sender_node_id, { id: msg.id, type: 'response', response })
                } catch (error) {
                    const { code, message } = error as any
                    sender_node_id && transporter.publish(sender_node_id, sender_node_id, { id: msg.id, type: 'error', error: code || message || error })
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
        })

        transporter.listen('#join', (sender_node_id: string, node: SpiderMeshNode) => this.#on_node_discovered(node, transporter))

        const me = await this.$metadata()
        await transporter.publish('#join', null, me)
    }

    async #on_node_discovered(node: SpiderMeshNode, transporter: SpiderMeshTransporter) {

        if (node.id == this.node_id) return

        process.env.SPIDERMESH_DEBUG && console.log(`New node `, node)

        const discovered = this.#nodes.get(node.id)
        const new_node: SpiderMeshNode = {
            ...discovered,
            ...node,
            transporters: discovered?.transporters || new Map(),
            offline: false
        }
        this.#nodes.set(node.id, new_node)
        const transporter_name = Object.getPrototypeOf(transporter).constructor.name
        this.#nodes.get(node.id)?.transporters?.set(transporter_name, transporter)

        node.services.forEach(service => {
            SpiderMesh.#LinkingServices.set(service, { online: true })
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


        !node.linked.includes(this.node_id) && await transporter.publish('#join', null, await this.$metadata())

    }

    async #caculate_request_node_id(service: string, options: RPCOptions) {
        const $ = this.#remote_services.get(service)
        if (!$ || $.nodes.length == 0) return null
        $.last_call_index = ($.last_call_index + 1) % $.nodes.length
        return $.nodes[$.last_call_index].id
    }

    async rpc(service: string, method: string, args: any, options: RPCOptions) {

        return await new Promise<any>(async (success, r) => {

            const reject = (err) => (options.fallback !== undefined) ? success(options.fallback) : r(err);

            const node_id = options.node_id || await this.#caculate_request_node_id(service, options)
            if (!node_id) return reject(new Error('SERVICE_OFFLINE'))
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
                target_node_id: node_id
            })
            const retry_count = options.retry || 1
            for (let i = retry_count; i > 0; i--) {
                for (const [__, transporter] of this.#nodes.get(node_id)?.transporters || []) {
                    try {
                        await transporter.publish(
                            node_id,
                            node_id,
                            { type: 'rpc', id: rid, args, method, service }
                        )
                        return
                    } catch (e) {

                    }
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

        SpiderMesh.#LinkingServices.set(service_name, { online: false })

        return new Proxy({}, {
            get: (_, method: string) => actions.has(method) || method.startsWith('$set_') ? new DeepProxy(
                RPCOptionsList,
                (method: string, options) => (...args) => this.rpc(service_name, method, args, options)
            ).nest()[method] : null
        }) as RemoteService<T>
    }

    async active_local_services(list: any[]) {

        if (list.length == 0) return

        for (const instance of list) {
            const prototype = Object.getPrototypeOf(instance)
            const name = prototype.constructor.name

            this.#local_services.set(name, instance)

            // Active event requester
            for (const { event, requests } of listEventSubscribers(prototype)) {
                requests.on('data', data => this.publish(event, data))
            }


            // Active event subscribers
            const event_subscribers = listEventSubscribers(prototype)
            for (const { event, method } of event_subscribers) {
                this.subscribe(event, (_, data) => instance[method]?.(data))
            }

        }

        // Broadcast running services
        const me = await this.$metadata()
        this.#transporters.forEach(t => t.publish('#join', null, me))

        // Wait remote service ready
        while (true) {
            await new Promise(s => setTimeout(s, 1000))
            if ([...SpiderMesh.#LinkingServices.values()].every(service => service.online)) {
                break
            }
        }

        // Active ready hook
        for (const instance of list) {
            for (const method of listReadyHookMethods(Object.getPrototypeOf(instance))) {
                instance[method]?.()
            }
        }


    }



    async publish<T = any>(topic: string, data: T) {
        this.#transporters.forEach(t => t.publish(topic, null, data))
    }

    async subscribe<T = any>(topic: string, cb: (from_node_id: string, data: T) => any) {
        const subscriptions = [... this.#transporters.values()].map(t => t.listen(topic, cb))
        return { unsubscribe: () => subscriptions.forEach(s => s.unsubscribe()) }
    }

}
