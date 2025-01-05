import { TcpNetConnectOpts, createConnection, Socket } from "net"
import { BehaviorSubject, EMPTY, Observable, Subject, Subscriber, catchError, filter, finalize, firstValueFrom, fromEvent, lastValueFrom, map, merge, mergeMap, of, range, retry, tap } from "rxjs"
import frame from 'frame-stream'


export class RxjsTcpSocket extends Observable<Buffer> {

    #fromRemote = false
    #$out = new Subject<Buffer>
    #remoteAddress?: string

    public get fromRemote() {
        return this.#fromRemote
    }

    public get remoteAddress() {
        return this.#remoteAddress
    }

    constructor(
        private $: Socket | TcpNetConnectOpts
    ) {
        super(o => {
            if ($ instanceof Socket) {
                this.#fromRemote = true
                firstValueFrom(of(0).pipe(
                    mergeMap(() => this.#join($, o)),
                    catchError(e => {
                        o.complete()
                        return of(null)
                    })
                ))
            } else {
                firstValueFrom(of(0).pipe(
                    map(() => createConnection({
                        ...$ as TcpNetConnectOpts,
                        autoSelectFamily: true
                    })),
                    mergeMap(socket => this.#join(socket, o)),
                    retry({ count: 10, delay: 100, resetOnSuccess: true }),
                    catchError(e => {
                        o.complete()
                        return of(null)
                    })
                ), { defaultValue: [] })
            }

        })
    }

    async #join(socket: Socket, o: Subscriber<Buffer>) {
        const encoder = frame.encode()
        const decoder = frame.decode()
        this.#remoteAddress = socket.remoteAddress
        return merge(
            fromEvent(socket, 'error').pipe(map(e => { throw e })),
            fromEvent(socket, 'close').pipe(map(() => 'CLOSED')),
            fromEvent(socket, 'timeout').pipe(map(e => { throw e })),
            fromEvent(socket, 'end').pipe(map(() => 'CLOSED')),
            this.#$out.pipe(map(data => encoder.writable && encoder.write(data))),
            merge(
                fromEvent<Buffer>(encoder, 'data').pipe(
                    map(buffer => socket.writable && socket.write(buffer))
                ),
                fromEvent<Buffer>(socket, 'data').pipe(
                    map(data => decoder.writable && decoder.write(data))
                ),
                fromEvent<Buffer>(decoder, 'data').pipe(
                    map(data => o.next(data))
                )
            ).pipe(
                filter(() => false)
            ),
        )
    }


    write(data: Buffer) {
        this.#$out.next(data)
    }


}

