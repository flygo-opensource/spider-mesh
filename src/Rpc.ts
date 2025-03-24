import { SpiderMesh, type RpcTransporter, type RpcOptions, SpiderMeshNode, MicroserviceException, MicroserviceOfflineException } from "@spider-mesh/core";
import { firstValueFrom, merge, Observable, ReplaySubject } from 'rxjs'
import { createServer, connect, ClientHttp2Session, ServerHttp2Stream, IncomingHttpHeaders, IncomingHttpStatusHeader, createSecureServer } from 'node:http2'
import { AddressInfo } from "net";
import { Encoder } from "./Encoder.js";
import { EMPTY, delayWhen } from "rxjs";
import { map, finalize, scan, mergeAll, catchError, filter, mergeMap, takeUntil } from "rxjs/operators";
import { BehaviorSubject } from "rxjs/internal/BehaviorSubject";
import { of } from "rxjs/internal/observable/of";
import { fromEvent } from "rxjs/internal/observable/fromEvent";
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
    #nodes = new Map<string, { connection: ClientHttp2Session, metadata: SpiderMeshNode }>()
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
                try {
                    const data = Buffer.concat(buffers)
                    const args = Encoder.decode<any[]>(data) || []

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

                    try {
                        const data = JSON.stringify(await response)
                        console.log({ data })
                        stream.respond({
                            ':status': 200,
                            'content-type': 'application/json',
                            'smnid': this.sm.node_id
                        })
                        stream.write(Buffer.from(data))
                        stream.end()
                    } catch (e: any) {
                        const data = JSON.stringify(e)
                        stream.respond({
                            ':status': 500,
                            'content-type': 'application/json'
                        })
                        stream.end(Buffer.from(data))
                        if (!e.code) throw e
                    }

                } catch (e) {
                    if (stream.destroyed || !stream.writable) return
                    stream.respond({
                        ':status': 400
                    })
                    stream.end()
                }
            })
        });

        this.#server.listen(0, () => {
            const { port } = this.#server.address() as AddressInfo
            this.metadata$.next({ [TransporterIndexName]: port })
        });


    }

    link(node: SpiderMeshNode & { host: string }) {
        const port = Number(node.transporters.http2)
        if (isNaN(port)) return
        for (const service in node.services) {
            const target = this.#services.get(service) || { ids: [] }
            target.ids.push(node.node_id)
            this.#services.set(service, target)
        }
    }

    check(service: string) {
        const target = this.#services.get(service) || { ids: [] }
        if (!target) return []
        return target.ids.map(id => this.#nodes.get(id)).filter(Boolean).map(a => a!.metadata)
    }

    async #select(r: RpcOptions) {
        const target = this.#services.get(r.service)
        if (!target) throw new MicroserviceOfflineException()
        const nodes = target.ids.map(id => this.#nodes.get(id))
        const is_round_robin = r.node_id || r.ip
        const matched = is_round_robin ? nodes : [
            nodes.find(node => {
                if (!node) return
                const { metadata: { host, node_id, ips } } = node
                if (r.node_id) return node_id == r.node_id
                if (r.ip) return ips.includes(r.ip) || r.ip == host
                return false
            })
        ]

        for (const node of matched) {
            if (!node) continue
            const connecting = !node.connection.destroyed && !node.connection.closed
            const url = `http${isSecure ? 's' : ''}://${node.metadata.host}:${node.metadata.transporters[TransporterIndexName]}`
            const connection = connecting ? node.connection : connect(url)
            const connected = connecting ? true : await firstValueFrom(merge(
                fromEvent(connection, 'connect').pipe(map(() => true)),
                fromEvent(connection, 'error').pipe(map(e => false))
            ))
            if (!connected) continue
            if (is_round_robin) {
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

        const header$ = new BehaviorSubject<undefined | {
            headers: IncomingHttpHeaders & IncomingHttpStatusHeader,
            json: boolean,
            error: boolean
        }>(undefined)
        return of(0).pipe(
            mergeMap(() => this.#select(r)),
            mergeMap(req => merge(
                fromEvent<IncomingHttpHeaders & IncomingHttpStatusHeader>(req, 'response').pipe(
                    map(headers => {
                        header$.next({
                            headers,
                            json: !!headers['content-type']?.startsWith('application/json'),
                            error: headers[':status'] != 200
                        })
                        return null
                    }),
                    filter(Boolean),
                ),
                fromEvent<Buffer>(req, 'data')
            ).pipe(
                takeUntil(fromEvent(req, 'end'))
            )
            )
        ).pipe(
            delayWhen(() => header$.value ? of(1) : header$.pipe(filter(Boolean))),
            scan((p, c) => {
                const json = header$.value!.json

                if (json) {
                    if (c) return { frames: [], tmp: Buffer.concat([p.tmp, c]) }
                    const data = JSON.parse(p.tmp.toString('utf8')) as T
                    const error = !!header$.value?.error
                    return {
                        frames: [{ data, error }],
                        tmp: Buffer.alloc(0)
                    }
                }

                const tmp = Buffer.concat([p.tmp, c])
                const frames: Array<{ data: T | undefined, error: boolean }> = []
                for (let index = 0; true; null) {
                    const flag = tmp.readInt32LE()
                    const error = flag < 0
                    const length = Math.abs(flag)
                    if (index + length + 4 > tmp.length) return {
                        frames,
                        tmp: tmp.slice(index)
                    }
                    const frame = tmp.slice(index + 4, index + length + 4)
                    const data = Encoder.decode<T>(frame)
                    frames.push({ data, error })
                    index += length + 4
                }
            }, { frames: [], tmp: Buffer.alloc(0) } as { frames: Array<{ data: T | undefined, error: boolean }>, tmp: Buffer }),
            map(e => e.frames),
            mergeAll(),
            map(({ data, error }) => {
                if (error) throw data
                return data
            })
        )

    }




} 