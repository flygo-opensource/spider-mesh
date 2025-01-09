import { createServer } from "net"
import { BehaviorSubject, Observable, firstValueFrom, fromEvent } from "rxjs"
import { RxjsTcpSocket } from "./RxjsTcpSocket.js"


export class RxjsTcpServer extends Observable<RxjsTcpSocket> {

    $online = new BehaviorSubject<{ port: number } | null>(null)

    get port() {
        return this.$online?.getValue()?.port
    }

    constructor(start_port: number = 10001) {
        super(o => {
            setTimeout(async () => {
                for (let port = start_port; true; port++) {
                    const server = createServer()
                    const success = await new Promise<boolean>(s => {
                        server.once('listening', () => s(true))
                        server.once('error', () => s(false))
                        server.listen(port)
                    })
                    if (!success) continue
                    server.on('connection', async socket => {
                        o.next(RxjsTcpSocket.from(socket))
                    })
                    this.$online.next({ port })
                    const $error = fromEvent(server, 'error') as Observable<Error>
                    await firstValueFrom($error)
                    server.removeAllListeners()
                }
            })
        })
    }

}


