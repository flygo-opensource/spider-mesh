import { RpcTransporter, type RpcOptions, SpiderMeshNode, RpcEvent, NodesMap, SpiderMeshError } from "@spider-mesh/types";
import { firstValueFrom, defer, merge, Observable, delayWhen, BehaviorSubject, of, fromEvent, catchError, tap, finalize, take, distinctUntilChanged, groupBy, exhaustMap, timer, takeUntil, switchMap } from 'rxjs'
import { createServer, connect, ClientHttp2Session, IncomingHttpHeaders, IncomingHttpStatusHeader, ServerHttp2Stream } from 'node:http2'
import { map, scan, mergeAll, filter, mergeMap, takeWhile } from "rxjs/operators";
import { EMPTY } from "rxjs/internal/observable/empty";
import { SPIDERMESH_HTTP2_AUTO_LOAD_BALANCE } from "./const.js";
import { AddressInfo } from "node:net";
import { unpack, pack } from 'msgpackr'
import { Subject } from "rxjs/internal/Subject";


export type RequestHeaders = {
    ':path': string
    ':authority': string,
    smnid: string
}


export class Http2Rpc implements RpcTransporter {

    #offline$ = new Subject<string>()
    #connections = new Map<string, ClientHttp2Session>()


    link(metadata$: Observable<SpiderMeshNode>, nodes$: Observable<NodesMap>): Observable<RpcEvent> {
        return new Observable<RpcEvent>(o => {

            const server = createServer({})

            server.on('stream', (stream: ServerHttp2Stream, headers: RequestHeaders) => {
                const [_, service, method] = headers[':path'].split('/')
                const buffers = [] as Buffer[]
                stream.on('data', (b: Buffer) => buffers.push(b))
                stream.once('end', async () => {
                    const data = Buffer.concat(buffers)
                    buffers.length = 0
                    const args = unpack(data) || []
                    const callback = async (res: Promise<any>) => {
                        try {
                            const response = await res
                            const metadata = await firstValueFrom(metadata$)
                            if (typeof response?.["pipe"] === 'function') {
                                stream.respond({
                                    ':status': 200,
                                    'content-type': 'application/octet-stream',
                                    'smnid': metadata.node_id
                                });

                                (response as Observable<any>).pipe(
                                    tap((obj) => {
                                        const encoded = pack(obj)
                                        const length = Buffer.alloc(4)
                                        length.writeInt32LE(encoded.length)
                                        const data = Buffer.concat([length, encoded])
                                        !stream.destroyed && stream.writable && stream.write(data)
                                    }),
                                    catchError(e => {
                                        const code = e.code || e.name || 'UNKNOWN_ERROR'
                                        const message = e.message || 'An unknown error occurred'
                                        const encoded = pack({ code, message })
                                        const length = Buffer.alloc(4)
                                        length.writeInt32LE(-encoded.length)
                                        const data = Buffer.concat([length, encoded])
                                        if (!stream.destroyed && stream.writable) stream.write(data)
                                        return EMPTY
                                    }),
                                    finalize(() => {
                                        if (!stream.destroyed && stream.writable) stream.end()
                                    }),
                                    takeUntil(fromEvent(stream, 'close')),
                                    takeUntil(fromEvent(stream, 'error'))
                                ).subscribe()
                                return
                            }
                            stream.respond({
                                ':status': 200,
                                'content-type': 'application/json',
                                'smnid': metadata.node_id
                            })
                            const payload = pack(response)
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
                stream.on('close', () => {
                    if (buffers.length > 0) {
                        buffers.length = 0
                    }
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
                groupBy(node => node.node_id, { duration: () => timer(60_000) }),
                mergeMap($ => $.pipe(
                    exhaustMap(async node => {
                        if (node.transporters.Http2Rpc === undefined) return
                        try {
                            await this.#connect(node, false)
                            o.next({ online: node.node_id })
                        } catch (e) {
                            console.error('Spidermesh HTTP2 RPC connection error', node, e)
                        }
                    })
                ))
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

    async #connect(node: SpiderMeshNode, force: boolean) {
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
        const e: SpiderMeshError = {
            code: 'MICROSERVICE_OFFLINE',
            message: `All connection attempts to node ${node.node_id} failed`
        }
        throw e
    }

    rpc<T>(r: RpcOptions, node: SpiderMeshNode, force: boolean) { 
        return of(0).pipe(
            switchMap(() => this.#connect(node, force)),
            switchMap(connection => {
                const req = connection.request({
                    ':method': 'POST',
                    ':path': `/${r.service}/${r.method}`,
                    'content-type': 'application/octet-stream'
                })
                return merge(
                    fromEvent<[IncomingHttpHeaders & IncomingHttpStatusHeader]>(req, 'response').pipe(
                        map(([hds]) => {
                            return {
                                headers: Object.assign(hds, {
                                    json: !!hds['content-type']?.startsWith('application/json'),
                                    error: Number(hds[':status']) != 200
                                })
                            }
                        })
                    ),
                    this.#offline$.pipe(
                        filter(id => id === node.node_id),
                        take(1),
                        map(() => {
                            const e: SpiderMeshError = {
                                code: 'MICROSERVICE_OFFLINE',
                                message: `Connection to node ${node.node_id} closed`
                            }
                            throw e
                        })
                    ),
                    fromEvent<Buffer>(req, 'data').pipe(
                        map(data => ({ data }))
                    ),
                    fromEvent(req, 'error').pipe(
                        mergeMap(err => { throw err })
                    ),
                    defer(() => {
                        const data = pack(r.args)
                        req.write(data, () => req.end())
                        return fromEvent(req, 'end').pipe(
                            map(() => ({ data: null }))
                        )
                    })
                ).pipe(
                    finalize(() => {
                        if (!req.destroyed) {
                            req.close()
                        }
                    })
                )
            }),
            scan((p, c) => {
                const headers = 'headers' in p ? p.headers : undefined
                if (headers) return {
                    ...p,
                    events: 'data' in c ? [{ data: c.data, headers }] : []
                }
                if ('headers' in c) return {
                    headers: c.headers,
                    queue: [],
                    events: p.queue.map(data => ({ data, headers: c.headers }))
                }
                if ('data' in c) return {
                    ...p,
                    queue: [...p.queue, c.data]
                }
                return p
            }, {
                queue: [],
                events: [],
            } as {
                queue: Array<Buffer | null>,
                headers?: IncomingHttpHeaders & IncomingHttpStatusHeader,
                events: Array<{ data: Buffer | null, headers: IncomingHttpHeaders & IncomingHttpStatusHeader }>
            }),
            map(d => d.events),
            mergeAll(),
            scan((p, { data: c, headers }) => {
                const json = !!headers.json
                const error = !!headers.error
                if (json) {
                    if (c) return {
                        completed: false,
                        frames: [],
                        tmp: Buffer.concat([p.tmp, c])
                    }
                    const data = unpack(p.tmp)
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
                frames: [] as Array<{ data: T | undefined, error: boolean }>,
                tmp: Buffer.alloc(0),
                completed: false
            })
        ).pipe(
            takeWhile(e => !e.completed, true),
            map(e => e.frames),
            mergeAll(),
            map(({ data, error }) => {
                if (error) {
                    const error = new Error(data?.message || 'An error occurred during RPC call');
                    error.name = data?.code || 'RPC_ERROR'
                    error.stack = ''
                    throw error
                }
                return data
            })
        )

    }

} 