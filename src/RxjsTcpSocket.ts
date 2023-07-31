import { TcpNetConnectOpts, createConnection, Socket } from "net"
import { BehaviorSubject, Observable, Subject, filter, firstValueFrom, fromEvent, mergeMap, takeUntil } from "rxjs"
import { sleep } from "./helpers/sleep"



export class RxjsTcpSocket<T = any> {

    static readonly #separator = String.fromCharCode(0x1E)
    #$outgoing_data = new Subject<Buffer>()
    $incoming_data = new Subject<T>()
    $status = new BehaviorSubject<'connecting' | 'ready' | 'closed' | 'error'>('connecting')
    rawSocket: Socket

    constructor(public readonly opened_by_remote_side: boolean) { }

    static connect<T = any>(options: TcpNetConnectOpts) {

        const $this = new this<T>(false)
        return new Promise<RxjsTcpSocket<T> | null>(async s => {
            for (let i = 0; i <= 5; i++) {
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
                if (status == 'closed') return
                process.env.SPIDERMESH_DEBUG && console.log(`[${new Date().toLocaleTimeString()}] Socket error, retrying in 1 sec`)
                await sleep(5000)
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

        this.#$outgoing_data.pipe(
            takeUntil(this.$status.pipe(filter(s => s == 'error' || s == 'closed'))),
            mergeMap(async buffer => {
                const wrote = socket.write(buffer)
                !wrote && await firstValueFrom(fromEvent(socket, 'drain'))
            }, 1)
        ).subscribe()

        let buffer = ''
        socket.on('data', msg => {

            buffer += msg.toString('utf8')

            if (buffer.includes(RxjsTcpSocket.#separator)) {
                const parts = buffer.split(RxjsTcpSocket.#separator)
                for (const part of parts) {
                    if (part != '') {
                        try {
                            const json = JSON.parse(part)
                            this.$incoming_data.next(json)
                        } catch (e) {
                         
                        }
                    }
                }
                buffer = parts.pop() || ''
            }
        })

    }


    async write(data: T) {
        const msg = JSON.stringify(data) + RxjsTcpSocket.#separator
        const buffer = Buffer.from(msg)
        this.#$outgoing_data.next(buffer)
    }
}

