import { TcpNetConnectOpts, createConnection, Socket } from "net"
import { BehaviorSubject, Subject, finalize, firstValueFrom, fromEvent, map, merge, takeUntil, tap, timer } from "rxjs"
import frame from 'frame-stream'
import { sleep } from "../../src/helpers/sleep.js"


export class RxjsTcpSocket<T = any> {

    #$outgoing_data = new Subject<Buffer>()
    $incoming_data = new Subject<Buffer>()
    $status = new BehaviorSubject<'connecting' | 'ready' | 'closed' | 'error'>('connecting')
    rawSocket: Socket

    constructor(public readonly opened_by_remote_side: boolean) {
        opened_by_remote_side && this.$status.next('ready')
    }

    static connect<T = any>({ retry_times = 5, retry_delay_ms = 5000, ...options }: TcpNetConnectOpts & { retry_times?: number, retry_delay_ms?: number }) {
        const $this = new this<T>(false)
        return new Promise<RxjsTcpSocket<T> | null>(async s => {
            for (let i = 1; i <= retry_times; i++) {
                const socket = createConnection({ ...options, autoSelectFamily: true })
                console.log(`Connect to ${options.host}:${options.port}`)
                const connected = await firstValueFrom(merge(
                    fromEvent(socket, 'connect').pipe(map(() => true)),
                    fromEvent(socket, 'error').pipe(map(() => false)),
                    timer(1000).pipe(map(() => false))
                ))
                console.log({connected})
                if (!connected) {
                    await sleep(retry_delay_ms)
                    continue
                }
                $this.$status.next('ready')
                const $error = $this.#join_util_error(socket)
                s($this)
                i = 0
                const status = await $error
                if (status == 'closed') return
            }
            s(null)
            $this.$status.next('error')
        })
    }

    static async join<T = any>(socket: Socket) {
        const $this = new this<T>(true)
        $this.#join_util_error(socket)
        return $this
    }

    async #join_util_error(socket: Socket) {
        const $error = merge(
            fromEvent(socket, 'error').pipe(map(() => 'error' as 'error')),
            fromEvent(socket, 'close').pipe(map(() => 'closed' as 'closed')),
            fromEvent(socket, 'timeout').pipe(map(() => 'error' as 'error')),
            fromEvent(socket, 'end').pipe(map(() => 'closed' as 'closed')),
        ).pipe(
            tap(status => this.$status.next(status))
        )


        this.rawSocket = socket

        const encoder = frame.encode()
        const decoder = frame.decode()

        decoder.on('data', (msg: Buffer) => {
            this.$incoming_data.next(msg)
        })
        socket.on('data', data => decoder.write(data))
        encoder.on('data', buffer => socket.writable && socket.write(buffer))

        this.#$outgoing_data.pipe(
            takeUntil($error),
            finalize(() => {
                socket.removeAllListeners()
                decoder.removeAllListeners()
                encoder.removeAllListeners()
            }),
            map(data => encoder.write(data), 1)
        ).subscribe()

        return firstValueFrom($error)
    }


    async write(data: Buffer) {
        this.#$outgoing_data.next(data)
    }
}

