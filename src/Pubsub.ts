import { SpiderMesh, SpiderMeshNode, type PubsubTransporter, SpiderMeshDiscoveryRegistry } from "@spider-mesh/core";
import { Subject } from "rxjs";
import { ReplaySubject } from 'rxjs'
import { AddressInfo } from "net";
import { SPIDERMESH_TLS_CA_PATH, SPIDERMESH_TLS_CERT_PATH, SPIDERMESH_TLS_KEY_PATH, SPIDERMESH_UDP_BROADCAST_PORT } from "./const.js";
import { ClientHttp2Session, createSecureServer } from "http2";
import http2 from 'http2'

export const isSecure = SPIDERMESH_UDP_BROADCAST_PORT && SPIDERMESH_TLS_CERT_PATH && SPIDERMESH_TLS_CA_PATH

type NodeId = string

export type PubsubMessage<T = any>= {
    namespace: string
    node_id: string
    sender_id: string
    topic: string
    data: T 
}

export class Pubsub implements PubsubTransporter {

    public readonly type: 'pubsub' = 'pubsub'
    public readonly metadata$ = new ReplaySubject<{ [name: string]: string | number | boolean; }>;

    #broadcaster = SpiderMeshDiscoveryRegistry.create<PubsubMessage>('http2pubsub')
    #nodes = new Map<string, ClientHttp2Session>()
    #subscriptions = new Map<string, Subject<any>>()
    #topics = new Map<string, Set<NodeId>>()


    constructor(sm: SpiderMesh) {
        const server = createSecureServer({
            ca: SPIDERMESH_TLS_CA_PATH,
            key: SPIDERMESH_TLS_KEY_PATH,
            cert: SPIDERMESH_TLS_CERT_PATH,
            allowHTTP1: true,
            requestCert: true,
            rejectUnauthorized: true
        })
        server.listen(0, '0.0.0.0', 0, () => {
            const address = server.address() as AddressInfo
           
            sm.linkTransporter(this)
        })
        server.on('request', (req, res) => {
            const event = req.headers[':path']?.split('/')?.[2]
            if (!event) return
            const buffers = [] as Buffer[]
            req.on('data', b => buffers.push(b as Buffer))
            req.on('end', () => {
                const $ = this.#subscriptions.get(event)
                if (!$) return
                const data = JSON.parse(Buffer.concat(buffers).toString('utf8'))
                $.next(data)
            })
        })

    }

    #connect(node: SpiderMeshNode) {
        const port = 123
        if (isNaN(Number(port))) return
        const url = `${isSecure ? 'https' : 'http'}://${node.host}:${port}`
        return http2.connect(url)
    }

    link(node: SpiderMeshNode) {
        if (!this.#nodes.has(node.node_id) && node.online) {
            const connection = this.#connect(node)
            if (!connection) return
            this.#nodes.set(node.node_id, connection)
            connection.once('error', () => {
                this.#nodes.delete(node.node_id)
            })
        }

        for (const topic of node.topics) {
            const set = this.#topics.get(topic) || new Set<string>()
            if (node.online) {
                set.add(node.node_id)
                this.#topics.set(topic, set)
            } else {
                set.delete(node.node_id)
                set.size == 0 && this.#topics.delete(topic)
            }
        }
    }

    listen<T>(topic: string) {
        const $ = this.#subscriptions.get(topic) || new Subject<any>()
        if (!this.#subscriptions.has(topic)) {
            this.#subscriptions.set(topic, $)

            this.#broadcaster.send({
                data:null,
                namespace:'',
                node_id:'',
                sender_id:'',
                topic:''
            })
        }
        return $ as Subject<T>
    }

    async publish<T>(topic: string, data: T) {
        const ids = this.#topics.get(topic) || new Set<NodeId>
        const buffer = Buffer.from(JSON.stringify(data))
        for (const id of ids) {
            const connection = this.#nodes.get(id)
            if (!connection) continue
            if (!connection.destroyed && !connection.closed) {
                const req = connection.request({
                    ':path': `/events/${topic}`,
                    ':method': 'POST',
                    'content-type': 'application/json'
                })
                req.write(buffer)
                req.end()
            }
        }
    }
}
