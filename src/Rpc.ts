import { SpiderMesh, type RpcTransporter, type RpcOptions, SpiderMeshNode, MicroserviceException, MicroserviceOfflineException } from "@spider-mesh/core";
import { firstValueFrom, merge, Observable, ReplaySubject, timer, EMPTY, delayWhen, BehaviorSubject, of, fromEvent } from 'rxjs'
import { createServer, connect, ClientHttp2Session, ServerHttp2Stream, IncomingHttpHeaders, IncomingHttpStatusHeader, createSecureServer } from 'node:http2'
import { AddressInfo } from "net";
import { Encoder } from "./Encoder.js";
import { map, finalize, scan, mergeAll, catchError, filter, mergeMap, takeUntil, retry, switchMap, takeWhile, tap } from "rxjs/operators";
import { isSecure } from "./Pubsub.js";
import { SPIDERMESH_TLS_CA_PATH, SPIDERMESH_TLS_CERT_PATH, SPIDERMESH_TLS_KEY_PATH } from "./const.js";



export type RequestHeaders = {
    ':path': string
    ':authority': string,
    smnid: string
}

const TransporterIndexName = 'http2rpc'

export class Rpc implements RpcTransporter {

    public readonly metadata$ = new ReplaySubject<{ [name: string]: string | number | boolean; }>(1)
    #server = isSecure ? createSecureServer({
        ca: SPIDERMESH_TLS_CA_PATH,
        key: SPIDERMESH_TLS_KEY_PATH,
        cert: SPIDERMESH_TLS_CERT_PATH,
        allowHTTP1: true,
        requestCert: true,
        rejectUnauthorized: true
    }) : createServer()
    #nodes = new Map<string, { connection?: ClientHttp2Session, metadata: SpiderMeshNode }>()
    #services = new Map<string, { ids: string[] }>()


    constructor(
        private sm: SpiderMesh
    ) {

        sm.add(this)


        this.#server.on('stream', (stream: ServerHttp2Stream, headers: RequestHeaders) => {
            const [_, service, method] = headers[':path'].split('/')
            const buffers = [] as Buffer[]
            stream.on('data', (b: Buffer) => buffers.push(b))
            stream.on('end', async () => {

                const data = Buffer.concat(buffers)
                const args = Encoder.decode<any[]>(data) || []

                try {
                    const response = await sm.lpc({
                        args,
                        method,
                        service
                    })

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

            })
        });

        this.#server.listen(0, () => {
            const { port } = this.#server.address() as AddressInfo
            this.metadata$.next({ [TransporterIndexName]: port })
        });


    }

    link(metadata: SpiderMeshNode & { host: string }) {
        if (!metadata.online) return
        const port = Number(metadata.transporters[TransporterIndexName])
        if (isNaN(port)) return
        for (const service in metadata.services) {
            const target = this.#services.get(service) || { ids: [] }
            if (target.ids.includes(metadata.node_id)) continue
            target.ids.push(metadata.node_id)
            this.#services.set(service, target)
        }
        const url = `http${isSecure ? 's' : ''}://${metadata.host}:${metadata.transporters[TransporterIndexName]}`
        firstValueFrom(of(1).pipe(
            map(() => connect(url)),
            switchMap(connection => merge(
                fromEvent(connection, 'close'),
                fromEvent(connection, 'error').pipe(
                    map(e => { throw e })
                )
            )),
            retry({
                delay(error, retryCount) {
                    if (retryCount == 1) return EMPTY
                    return timer(2000)
                },
            }),
            finalize(() => {
                this.#nodes.delete(metadata.node_id)
                for (const [n, s] of this.#services) {
                    s.ids = s.ids.filter(id => id != metadata.node_id)
                }
                this.sm.sync({ ...metadata, online: false })
            })
        ), { defaultValue: null })
        this.#nodes.set(metadata.node_id, { metadata })
    }

    check(service: string) {
        const target = this.#services.get(service) || { ids: [] }
        if (!target) return []
        const nodes = (
            target.ids
                .map(id => this.#nodes.get(id))
                .filter(Boolean)
                .map(a => a!.metadata)
        )
        return nodes
    }

    async #select(r: RpcOptions) {
        const target = this.#services.get(r.service)
        if (!target) throw new MicroserviceOfflineException()
        const nodes = target.ids.map(id => this.#nodes.get(id))
        const is_direct_rpc = r.node_id || r.ip
        const matched = is_direct_rpc ? [
            nodes.find(node => {
                if (!node) return
                const { metadata: { host, node_id, ips } } = node
                if (r.node_id) return node_id == r.node_id
                if (r.ip) return ips.includes(r.ip) || r.ip == host
                return false
            })
        ] : nodes

        for (const node of matched) {
            if (!node) continue
            const droppped = node.connection && (node.connection.destroyed || node.connection.closed)
            const url = `http${isSecure ? 's' : ''}://${node.metadata.host}:${node.metadata.transporters[TransporterIndexName]}`
            const connection = node.connection && !droppped ? node.connection : connect(url)
            const connected = connection.connecting ? await firstValueFrom(merge(
                fromEvent(connection, 'connect').pipe(map(() => true)),
                fromEvent(connection, 'error').pipe(map(e => false))
            )) : true
            if (!connected) continue
            if (!is_direct_rpc) {
                target.ids = [
                    ...target.ids.filter(id => id != node.metadata.node_id),
                    node.metadata.node_id
                ]
            }
            return connection
        }
        throw new MicroserviceOfflineException()
    }


    rpc<T>(r: RpcOptions) {
        console.log({ rpc: r })
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