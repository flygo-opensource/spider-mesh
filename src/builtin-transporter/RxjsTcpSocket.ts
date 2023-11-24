import { TcpNetConnectOpts, createConnection, Socket } from "net"
import { BehaviorSubject, Observable, Subject, filter, firstValueFrom, fromEvent, map, mergeMap, takeUntil } from "rxjs"
import { sleep } from "../helpers/sleep.js"
import { DEBUG } from "../const.js"
import frame from 'frame-stream'


export class RxjsTcpSocket<T = any> {

    #$outgoing_data = new Subject<Buffer>()
    $incoming_data = new Subject<Buffer>()
    $status = new BehaviorSubject<'connecting' | 'ready' | 'closed' | 'error'>('connecting')
    rawSocket: Socket

    constructor(public readonly opened_by_remote_side: boolean) { }

    static connect<T = any>(options: TcpNetConnectOpts & { retry_times?: number }) {
        const retry_times = options.retry_times || 5
        const $this = new this<T>(false)
        return new Promise<RxjsTcpSocket<T> | null>(async s => {
            for (let i = 1; i <= retry_times; i++) {
                const socket = createConnection(options)
                const connect_status = await new Promise<boolean>(s => {
                    socket.once('connect', () => s(true))
                    socket.once('error', () => s(false))
                    socket.once('timeout', () => s(false))
                })
                if (!connect_status) continue
                await $this.#join(socket)
                s($this)
                i = 0
                const status = await firstValueFrom($this.$status.pipe(filter(s => s == 'error' || s == 'closed')))
                socket.removeAllListeners()
                if (status == 'closed') {
                    $this.$status.next('closed')
                    return
                }
            }
            s(null)
            $this.$status.next('error')
        })
    }

    static async join<T = any>(socket: Socket) {
        const $this = new this<T>(true)
        await $this.#join(socket)
        return $this
    }

    async #join(socket: Socket) {
        socket.once('error', () => this.$status.next('error'))
        socket.once('close', () => this.$status.next('closed'))
        socket.on('drain', () => this.$status.next('ready'))
        socket.on('ready', () => this.$status.next('ready'))
        socket.once('timeout', () => this.$status.next('error'))
        socket.once('end', () => this.$status.next('closed'))

        this.rawSocket = socket

        const encoder = frame.encode()
        const decoder = frame.decode()



        socket.on('data', data => decoder.write(data))
        decoder.on('data', (msg: Buffer) => this.$incoming_data.next(msg))
        encoder.on('data', buffer => socket.writable && socket.write(buffer))
        this.#$outgoing_data.pipe(
            takeUntil(this.$status.pipe(filter(s => s == 'error' || s == 'closed'))),
            map(data => encoder.write(data), 1)
        ).subscribe()

    }


    async write(data: Buffer) {
        DEBUG && console.log({
            time: `${new Date().getMinutes()}:${new Date().getSeconds()}:${new Date().getMilliseconds()}`,
            send: data
        })
        this.#$outgoing_data.next(data)
    }
}

