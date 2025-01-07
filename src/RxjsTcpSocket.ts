import { TcpNetConnectOpts, createConnection, Socket, connect } from "net"
import { Subject, catchError, filter, finalize, firstValueFrom, fromEvent, map, merge, mergeMap, of, retry, tap } from "rxjs"
import frame from 'frame-stream'


export class RxjsTcpSocket extends Subject<Buffer> {

    #fromRemote = false
    #$out = new Subject<Buffer>
    #remoteAddress?: string

    public get fromRemote() {
        return this.#fromRemote
    }

    public get remoteAddress() {
        return this.#remoteAddress
    }

    static async connect(options: TcpNetConnectOpts) {
        const t = new this()
        t.#remoteAddress = options.host
        t.#fromRemote = false
        const connected = await new Promise<boolean>(s => firstValueFrom(of(0).pipe(
            map(() => connect(
                { ...options, autoSelectFamily: true, },
                () => s(true))
            ), 
            mergeMap(socket => t.#join(socket)),
            retry({ count: 10, delay: 100, resetOnSuccess: true }),
            catchError(e => {
                t.complete()
                s(false)
                return of(null)
            }),
        )))
        return connected ? t : null
    }

    static from(socket: Socket) {
        const t = new this()
        t.#fromRemote = true
        t.#remoteAddress = socket.remoteAddress
        t.#join(socket).subscribe({
            error: e => t.error(e),
            complete: () => t.complete()
        })
        return t
    }

    private constructor() {
        super()
    }

    #join(socket: Socket) {
        const encoder = frame.encode()
        const decoder = frame.decode()

        return merge(
            fromEvent(socket, 'error').pipe(map(e => { throw e })),
            fromEvent(socket, 'close').pipe(map(() => 'CLOSED')),
            fromEvent(socket, 'timeout').pipe(map(e => { throw e })),
            fromEvent(socket, 'end').pipe(map(() => 'ENDED')),
            merge(
                this.#$out.pipe(
                    map(data => encoder.writable && encoder.write(data))
                ),
                fromEvent<Buffer>(encoder, 'data').pipe(
                    map(buffer => socket.writable && socket.write(buffer))
                ),
                fromEvent<Buffer>(socket, 'data').pipe(
                    map(data => decoder.writable && decoder.write(data))
                ),
                fromEvent<Buffer>(decoder, 'data').pipe(
                    map(data => this.next(data))
                )
            ).pipe(
                filter(() => false)
            ),
        )
    }

    send(data: Buffer) {
        this.#$out.next(data)
    }


}

