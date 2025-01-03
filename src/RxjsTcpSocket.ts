import { TcpNetConnectOpts, createConnection, Socket } from "net"
import { BehaviorSubject, Subject, finalize, firstValueFrom, fromEvent, map, merge, takeUntil, tap, timer } from "rxjs"
import frame from 'frame-stream'


export class RxjsTcpSocket {

    #$outgoing_data = new Subject<Buffer>()
    $incoming_data = new Subject<Buffer>()
    $status = new BehaviorSubject<'connecting' | 'ready' | 'closed' | 'error'>('connecting')
    rawSocket: Socket

    private constructor(public readonly fromRemote: boolean) {
        fromRemote && this.$status.next('ready')
    }

    static connect({ retry_times = 5, retry_delay_ms = 5000, ...options }: TcpNetConnectOpts & { retry_times?: number, retry_delay_ms?: number }) {
        const $this = new this(false)
        return new Promise<RxjsTcpSocket | null>(async s => {
            for (let i = 0; i <= retry_times; i++) {
                const socket = createConnection({ ...options, autoSelectFamily: true })
                socket.on('error', () => { })
                const connected = await firstValueFrom(merge(
                    fromEvent(socket, 'connect').pipe(map(() => true)),
                    fromEvent(socket, 'error').pipe(map(() => false)),
                    timer(1000).pipe(map(() => false))
                ))
                if (!connected) {
                    socket.destroy()
                    await firstValueFrom(timer(retry_delay_ms))
                    if (i == 0) {
                        break
                    } else {
                        continue
                    }
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

    static async join(socket: Socket) {
        const $this = new this(true)
        $this.#join_util_error(socket)
        return $this
    }

    async #join_util_error(socket: Socket) {
        const $error = firstValueFrom(merge(
            fromEvent(socket, 'error').pipe(map(() => 'error' as 'error')),
            fromEvent(socket, 'close').pipe(map(() => 'closed' as 'closed')),
            fromEvent(socket, 'timeout').pipe(map(() => 'error' as 'error')),
            fromEvent(socket, 'end').pipe(map(() => 'closed' as 'closed')),
        ).pipe(
            tap(status => this.$status.next(status))
        ))


        this.rawSocket = socket

        const encoder = frame.encode()
        const decoder = frame.decode()
        encoder.on('error', () => { })
        decoder.on('error', () => { })
        socket.on('error', () => { })

        decoder.on('data', (msg: Buffer) => {
            this.$incoming_data.next(msg)
        })
        socket.on('data', data => decoder.writable && decoder.write(data))
        encoder.on('data', buffer => socket.writable && socket.write(buffer))

        this.#$outgoing_data.pipe(
            takeUntil($error),
            finalize(() => {
                socket.end()
                decoder.end()
                encoder.end()
            }),
            map(data => encoder.writable && encoder.write(data), 1)
        ).subscribe()

        return $error
    }


    write(data: Buffer) {
        this.#$outgoing_data.next(data)
    }

    close() {
        this.rawSocket?.end()
    }
}

