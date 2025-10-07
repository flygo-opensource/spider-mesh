import { SpiderMesh, RpcTransporter, type RpcOptions, SpiderMeshNode, MicroserviceOfflineException, RpcEvent, NodesMap, MicroserviceException } from "@spider-mesh/core";
import { firstValueFrom, merge, Observable, delayWhen, BehaviorSubject, of, fromEvent, catchError, finalize, tap, distinctUntilChanged } from 'rxjs'
import { createServer, connect, ClientHttp2Session, IncomingHttpHeaders, IncomingHttpStatusHeader, ServerHttp2Stream } from 'node:http2'
import { map, scan, mergeAll, filter, mergeMap, takeWhile } from "rxjs/operators";
import { EMPTY } from "rxjs/internal/observable/empty";
import { SPIDERMESH_HTTP2_AUTO_LOAD_BALANCE } from "./const.js";
import { AddressInfo, isIPv4, isIPv6 } from "node:net";
import { unpack, pack } from 'msgpackr'
import { Subject } from "rxjs/internal/Subject";
import { timer } from "rxjs/internal/observable/timer";


export type RequestHeaders = {
    ':path': string
    ':authority': string,
    smnid: string
}


export class Http2Rpc extends RpcTransporter {

    #offline$ = new Subject<string>()
    #connections = new Map<string, ClientHttp2Session>()

    constructor(private sm: SpiderMesh) {
        super()
        sm.linkTransporter(this)
    }


    link(nodes$: Observable<NodesMap>): Observable<RpcEvent> {
        return new Observable<RpcEvent>(o => {

            const server = createServer({

            })

            server.on('stream', (stream: ServerHttp2Stream, headers: RequestHeaders) => {

                const [_, service, method] = headers[':path'].split('/')
                const buffers = [] as Buffer[]
                stream.on('data', (b: Buffer) => buffers.push(b))
                stream.on('end', async () => {
                    const data = Buffer.concat(buffers)
                    const args = unpack(data) || []
                    const callback = async (res: Promise<any>) => {
                        try {
                            const response = await res
                            if (response instanceof Observable) {
                                stream.respond({
                                    ':status': 200,
                                    'content-type': 'application/octet-stream',
                                    'smnid': this.sm.node_id
                                })
                                response.pipe(
                                    map(obj => {
                                        const encoded = pack(obj)
                                        const length = Buffer.alloc(4)
                                        length.writeInt32LE(encoded.length)
                                        const data = Buffer.concat([length, encoded])
                                        !stream.destroyed && stream.writable && stream.write(data)
                                    }),
                                    catchError(e => {
                                        const encoded = pack(e)
                                        const length = Buffer.alloc(4)
                                        length.writeInt32LE(-encoded.length)
                                        const data = Buffer.concat([length, encoded])
                                        !stream.destroyed && stream.writable && stream.write(data)
                                        if (!(e instanceof MicroserviceException)) throw e
                                        return EMPTY
                                    }),
                                    finalize(() => {
                                        stream.end()
                                    })

                                ).subscribe()
                                return
                            }

                            const r = await response
                            stream.respond({
                                ':status': 200,
                                'content-type': 'application/json',
                                'smnid': this.sm.node_id
                            })
                            const payload = pack(r)
                            stream.write(payload)
                            stream.end()
                        } catch (e: any) {
                            if (stream.destroyed || !stream.writable) return
                            const data = pack({
                                ...e.metadata || {},
                                code: e.code || 'UNKNOWN',
                                stack: e.stack,
                                name: e.name
                            })
                            stream.respond({
                                ':status': 500,
                                'content-type': 'application/json'
                            })
                            stream.end(data)
                            if (!e.code) throw e
                        }
                    }
                    o.next({
                        rpc: {
                            args,
                            callback,
                            method,
                            service
                        }
                    })
                })
            });

            server.listen({
                port: 0,
                isIPv4: true,
                isIPv6: true
            }, () => {
                const { port } = server.address() as AddressInfo
                o.next({
                    metadata: { port }
                })
            })


            // Handle new node
            const online_subscription = nodes$.pipe(
                map(e => {
                    if (!e.last_updated_node_id) {
                        return [...e.nodes.values()]
                    }
                    const node = e.nodes.get(e.last_updated_node_id)
                    return node ? [node] : []
                }),
                mergeAll(),
                mergeMap(async node => {
                    if (node.transporters.Http2Rpc === undefined) return
                    try{
                        await this.#connect({ service: '', method: '', args: [] }, node, false)
                        o.next({ online: node.node_id })
                    }catch(e){

                    }
                })
            ).subscribe()




            // Handle node offline
            const offline_subscription = this.#offline$.pipe(
                distinctUntilChanged(),
                mergeMap(node_id => {
                    this.#connections.delete(node_id)
                    return of({ offline: node_id })
                })
            ).subscribe(o)

            return () => {
                server.close()
                online_subscription.unsubscribe()
                offline_subscription.unsubscribe()
            }
        })
    }

    async #connect(r: RpcOptions, node: SpiderMeshNode, force: boolean) {
        const current_connection = this.#connections.get(node.node_id)
        if (current_connection) {
            if (!current_connection.destroyed && !current_connection.closed) {
                return current_connection
            }
        }

        const auto = SPIDERMESH_HTTP2_AUTO_LOAD_BALANCE || !force

        const urls = auto ? [
            `http://${node.host}:${node.transporters.Http2Rpc.port}`,
        ] : node.ips.sort((a, b) => a.length - b.length).map(ip => `http://${ip.includes(':') ? `[${ip}]` : ip}:${node.transporters.Http2Rpc.port}`)

        for (const url of urls) {
            const connection = connect(url)
            const connected = connection.connecting ? await firstValueFrom(merge(
                fromEvent(connection, 'connect').pipe(map(() => true)),
                fromEvent(connection, 'error').pipe(map(e => false)),
            )) : true
            if (connected) {
                connection.once('close', () => {
                    this.#offline$.next(node.node_id)
                })
                this.#connections.set(node.node_id, connection)
                return connection
            }
        }
        throw new MicroserviceOfflineException()
    }

    rpc<T>(r: RpcOptions, node: SpiderMeshNode, force: boolean) {


        const header$ = new BehaviorSubject<undefined | {
            headers: IncomingHttpHeaders & IncomingHttpStatusHeader,
            json: boolean,
            error: boolean
        }>(undefined)
        return of(0).pipe(
            mergeMap(() => this.#connect(r, node, force)),
            mergeMap(connection => {
                const req = connection.request({
                    ':method': 'POST',
                    ':path': `/${r.service}/${r.method}`,
                    'content-type': 'application/octet-stream'
                })
                req.on('response', headers => {
                    header$.next({
                        headers,
                        json: !!headers['content-type']?.startsWith('application/json'),
                        error: headers[':status'] != 200
                    })
                })
                return merge(
                    fromEvent(connection, 'close').pipe(
                        map(() => {
                            throw new MicroserviceOfflineException()
                        })
                    ),
                    fromEvent<Buffer>(req, 'data'),
                    new Observable<null>(o => {
                        req.on('end', () => o.next(null))
                        const data = pack(r.args)
                        req.write(data)
                        req.end()
                    })
                )
            }),
            delayWhen(() => header$.value ? of(1) : header$.pipe(filter(Boolean))),
            scan((p, c) => {
                const json = header$.value!.json
                if (json) {
                    if (c) return {
                        completed: false,
                        frames: [],
                        tmp: Buffer.concat([p.tmp, c])
                    }
                    const data = unpack(p.tmp)
                    const error = !!header$.value?.error
                    return {
                        tmp: p.tmp,
                        frames: [{ data, error }],
                        completed: true
                    }
                }
                if (!c) return {
                    completed: true,
                    frames: [],
                    tmp: p.tmp
                }
                const tmp = Buffer.concat([p.tmp, c])
                const frames: Array<{ data: T | undefined, error: boolean }> = []
                for (let index = 0; true; null) {
                    const flag = tmp.readInt32LE()
                    const error = flag < 0
                    const length = Math.abs(flag)
                    if (index + length + 4 > tmp.length) return {
                        frames,
                        tmp: tmp.slice(index),
                        completed: false
                    }
                    const frame = tmp.slice(index + 4, index + length + 4)
                    const data = unpack(frame) as T
                    frames.push({ data, error })
                    index += length + 4
                }
            }, {
                frames: [],
                tmp: Buffer.alloc(0),
                completed: false
            } as {
                frames: Array<{ data: T | undefined, error: boolean }>,
                tmp: Buffer,
                completed: boolean
            }),
            takeWhile(e => !e.completed, true),
            map(e => e.frames),
            mergeAll(),
            map(({ data, error }) => {
                if (error) throw data
                return data
            })
        )

    }

} 