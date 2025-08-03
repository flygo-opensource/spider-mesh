import { SpiderMesh, type RpcTransporter, type RpcOptions, SpiderMeshNode, MicroserviceException, MicroserviceOfflineException, SpiderMeshDiscoveryRegistry } from "@spider-mesh/core";
import { firstValueFrom, merge, Observable, ReplaySubject, EMPTY, delayWhen, BehaviorSubject, of, fromEvent, Subject, lastValueFrom } from 'rxjs'
import { createServer, connect, ClientHttp2Session, ServerHttp2Stream, IncomingHttpHeaders, IncomingHttpStatusHeader, createSecureServer } from 'node:http2'
import { AddressInfo } from "net";
import { Encoder } from "./Encoder.js";
import { map, finalize, scan, mergeAll, catchError, filter, mergeMap, takeWhile, debounceTime } from "rxjs/operators";
import { isSecure } from "./Pubsub.js";
import { SPIDERMESH_TLS_CA_PATH, SPIDERMESH_TLS_CERT_PATH, SPIDERMESH_TLS_KEY_PATH } from "./const.js";


export type RequestHeaders = {
    ':path': string
    ':authority': string,
    smnid: string
}



export class Rpc implements RpcTransporter {

    public readonly type: "rpc" = 'rpc'
    public readonly requests$ = new Subject<{ req: RpcOptions; res: (o: any | Promise<any> | Observable<any>) => void }>()
    public readonly services = new Map<string, Set<string>>()
    public readonly nodes$ = new BehaviorSubject(new Map<string, SpiderMeshNode>())

    #connections = new Map<string, ClientHttp2Session>()
    #discoverer = SpiderMeshDiscoveryRegistry.create<SpiderMeshNode & { port: number }>('http2')
    #server = isSecure ? createSecureServer({
        ca: SPIDERMESH_TLS_CA_PATH,
        key: SPIDERMESH_TLS_KEY_PATH,
        cert: SPIDERMESH_TLS_CERT_PATH,
        allowHTTP1: true,
        requestCert: true,
        rejectUnauthorized: true
    }) : createServer()
    #metadata = new ReplaySubject<SpiderMeshNode>(1)

    async join(me: SpiderMeshNode) {
        this.#metadata.next(me)
    }

    constructor(private sm: SpiderMesh) {

        this.#server.on('stream', (stream: ServerHttp2Stream, headers: RequestHeaders) => {

            const [_, service, method] = headers[':path'].split('/')
            const buffers = [] as Buffer[]
            stream.on('data', (b: Buffer) => buffers.push(b))
            stream.on('end', async () => {
                const data = Buffer.concat(buffers)
                const args = Encoder.decode<any[]>(data) || []
                const res = async (rs: any) => {
                    try {
                        const response = await rs

                        if (response instanceof Observable) {
                            stream.respond({
                                ':status': 200,
                                'content-type': 'application/octet-stream',
                                'smnid': this.sm.node_id
                            })
                            stream.on('error', () => { })
                            response.pipe(
                                map(obj => {
                                    const encoded = Encoder.encode(obj)
                                    const length = Buffer.alloc(4)
                                    length.writeInt32LE(encoded.length)
                                    const data = Buffer.concat([length, encoded])
                                    !stream.destroyed && stream.writable && stream.write(data)
                                }),
                                catchError(e => {
                                    const encoded = Encoder.encode(e)
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
                        const payload = Encoder.encode(r)
                        stream.write(payload)
                        stream.end()
                    } catch (e: any) {
                        if (stream.destroyed || !stream.writable) return
                        const data = Encoder.encode({
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
                this.requests$.next({
                    res,
                    req: { args, method, service }
                })
            })
        });

        this.#server.listen(0, () => {
            const { port } = this.#server.address() as AddressInfo
            this.#metadata.pipe(
                debounceTime(100),
                mergeMap(async (metadata, i) => {
                    await this.#discoverer.send({
                        ...metadata,
                        transporters: {
                            ...metadata.transporters,
                            http2: port,
                        },
                        port
                    })
                    i == 0 && sm.linkTransporter(this)
                }),
            ).subscribe()
        })


        lastValueFrom(this.#discoverer.pipe(
            mergeMap(async node => {

            })
        ))
    }

    async #select(r: RpcOptions) {
        const target = this.services.get(r.service)
        if (!target) throw new MicroserviceOfflineException()
        const nodes = [...target].map(id => this.nodes$.getValue().get(id))
        const is_direct_rpc = r.node_id || r.ip
        const matched = is_direct_rpc ? [
            nodes.find(node => {
                if (!node) return
                if (r.node_id) return r.node_id == node.node_id
                if (r.ip) return node.ips.includes(r.ip)
                return false
            })
        ] : nodes

        for (const node of matched) {
            if (!node) continue
            const current_connection = this.#connections.get(node.node_id)
            if (!current_connection) continue
            const droppped = current_connection.destroyed || current_connection.closed
            const url = `http${isSecure ? 's' : ''}://${node.host}:${node.transporters.http2}`
            const connection = current_connection && !droppped ? current_connection : connect(url)
            const connected = connection.connecting ? await firstValueFrom(merge(
                fromEvent(connection, 'connect').pipe(map(() => true)),
                fromEvent(connection, 'error').pipe(map(e => false))
            )) : true
            if (!connected) continue
            if (!is_direct_rpc) {
                const set = this.services.get(r.service) || new Set()
                set.delete(node.node_id)
                this.services.set(r.service, new Set([
                    ...set,
                    node.node_id
                ]))
            }
            return connection
        }
        throw new MicroserviceOfflineException()
    }


    rpc<T>(r: RpcOptions) {
        const header$ = new BehaviorSubject<undefined | {
            headers: IncomingHttpHeaders & IncomingHttpStatusHeader,
            json: boolean,
            error: boolean
        }>(undefined)
        return of(0).pipe(
            mergeMap(async () => {
                const connection = await this.#select(r)
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
                return req
            }),
            mergeMap(req => merge(
                fromEvent<Buffer>(req, 'data'),
                new Observable<null>(o => {
                    req.on('end', () => o.next(null))
                    const data = Encoder.encode(r.args)
                    req.write(data)
                    req.end()
                })
            )),
            delayWhen(() => header$.value ? of(1) : header$.pipe(filter(Boolean))),
            scan((p, c) => {
                const json = header$.value!.json
                if (json) {
                    if (c) return {
                        completed: false,
                        frames: [],
                        tmp: Buffer.concat([p.tmp, c])
                    }
                    const data = Encoder.decode<any>(p.tmp)
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
                    const data = Encoder.decode<T>(frame)
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