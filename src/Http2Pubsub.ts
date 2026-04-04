import { PubsubTransporter, PubsubTransporterEvent, NodesMap, SpiderMeshNode } from "@spider-mesh/types";
import { map, Observable, Subject } from "rxjs";
import { filter } from 'rxjs'
import { ClientHttp2Session, createSecureServer } from "http2";
import http2 from 'http2'
import { AddressInfo } from "net";
import { merge } from "rxjs/internal/observable/merge";
import { unpack, pack } from 'msgpackr'


type NodeId = string

export type PubsubMessage<T = any> = {
    namespace: string
    node_id: string
    sender_id: string
    topic: string
    data: T
}

export class Http2Pubsub implements PubsubTransporter {

    public readonly type = 'pubsub'
    #nodes = new Map<string, ClientHttp2Session>()
    #subscriptions = new Map<string, Subject<any>>()
    #topics = new Map<string, Set<NodeId>>()


    #server() {
        return new Observable<PubsubTransporterEvent>(o => {

            // Init server
            const server = createSecureServer({
                allowHTTP1: true,
                requestCert: true,
                rejectUnauthorized: true,

            })
            server.listen(() => {
                const address = server.address() as AddressInfo
                o.next({
                    metadata: {
                        http2pubsub: address.port
                    }
                })
            })
            server.on('request', (req, res) => {
                const event = req.headers[':path']?.split('/')?.[2]
                if (!event) return
                const buffers = [] as Buffer[]
                req.on('data', b => buffers.push(b as Buffer))
                req.on('end', () => {
                    const $ = this.#subscriptions.get(event)
                    if (!$) return
                    try {
                        const data = unpack(Buffer.concat(buffers))
                        $.next(data)
                    } catch (e) {

                    }
                })
            })

            return () => {
                server.close()
            }
        })
    }


    link(metadata$: Observable<SpiderMeshNode>, nodes$: Observable<NodesMap>): Observable<PubsubTransporterEvent> {
        return merge(this.#server(), nodes$.pipe(
            map(e => {
                const node = e.nodes.get(e.last_updated_node_id)
                if (!node) return
                if (!this.#nodes.has(node.node_id)) {
                    const port = node.transporters.http2pubsub
                    if (isNaN(Number(port))) return
                    const url = `http://${node.host}:${port}`
                    const connection = http2.connect(url)
                    if (!connection) return
                    this.#nodes.set(node.node_id, connection)
                    connection.once('error', () => {
                        this.#nodes.delete(node.node_id)
                    })
                }

                for (const topic of node.topics) {
                    const set = this.#topics.get(topic) || new Set<string>()
                    set.add(node.node_id)
                    this.#topics.set(topic, set)
                    // if (node.online) {
                    //     set.add(node.node_id)
                    //     this.#topics.set(topic, set)
                    // } else {
                    //     set.delete(node.node_id)
                    //     set.size == 0 && this.#topics.delete(topic)
                    // }
                }
            }),
            map(() => null),
            filter(Boolean)
        ))
    }

    listen<T>(topic: string) {
        const $ = this.#subscriptions.get(topic) || new Subject<any>()
        if (!this.#subscriptions.has(topic)) {
            this.#subscriptions.set(topic, $)
        }
        return $ as Subject<T>
    }

    async publish<T>(topic: string, data: T) {
        const ids = this.#topics.get(topic) || new Set<NodeId>
        const buffer = pack(data)
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
