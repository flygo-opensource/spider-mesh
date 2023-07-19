import { Socket } from "net"
import EventEmitter from "events"
import { TcpNetConnectOpts, createConnection } from "net"
import { Subject, map } from "rxjs"
import { waitFirstEvent } from "./helpers/waitFirstEvent"
import { sleep } from "./helpers/sleep"



export class StableTCP extends EventEmitter {

    static readonly #separator = String.fromCharCode(0x1E)

    #input = [] as Buffer[]
    #$data = new Subject<void>()

    connect: () => Promise<StableTCP | null>
    
    constructor(options?: TcpNetConnectOpts, socket?: Socket) {
        super()
        this.connect = async () => {
            this.connect = async () => null
            if (socket) {
                this.#join(socket)
                return this
            }
            if (options) {
                const success = await this.#init(options)
                return success ? this : null
            }
            throw new Error('INVAILD_TCP_OPTIONS')
        }
    } 

    async #join(socket: Socket) {
        let buffer = ''
        const subscription = this.#$data.pipe(
            map(() => {
                while (this.#input.length > 0) {
                    const buffer = this.#input.pop()
                    buffer && socket.write(buffer)
                }
            })
        ).subscribe()

        socket.on('data', msg => {

            buffer += msg.toString('utf8')

            if (buffer.includes(StableTCP.#separator)) {
                const parts = buffer.split(StableTCP.#separator)
                for (const part of parts) {
                    if (part != '') {
                        try {
                            const json = JSON.parse(part)
                            this.emit('data', json)
                        } catch (e) {
                            console.log({
                                can_not_decode: part
                            })
                        }
                    }
                }
                buffer = parts.pop() || ''
            }
        })

        await waitFirstEvent(socket, 'error', 'close', 'drop', 'end', 'drain', 'timeout')
        subscription.unsubscribe()
    }

    async #init(options: TcpNetConnectOpts) {
        return await new Promise<boolean>(async s => {
            for (let i = 0; i <= 5; i++) {
                const socket = createConnection(options)
                const [status] = await waitFirstEvent(socket, 'ready', 'connect', 'timeout', 'error')
                if (status == 'timeout' || status == 'error') continue
                s(true)
                await this.#join(socket)
                i = 0
                const [error] = await waitFirstEvent(socket, 'close', 'error', 'end')
                if (error == 'close') return this.emit('close')
                process.env.SPIDERMESH_DEBUG && console.log(`[${new Date().toLocaleTimeString()}] Socket error, retrying in 1 sec`)
                await sleep(1000)
            }
            this.emit('error', new Error('CAN_NOT_CONNECT'))
            s(false)
        })
    }

    async write(data: any) {
        const msg = JSON.stringify(data) + StableTCP.#separator
        const buffer = Buffer.from(msg)
        this.#input.unshift(buffer)
        this.#$data.next()
    }
}
