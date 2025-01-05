import { createServer } from "net"
import { BehaviorSubject, Observable, Subject, firstValueFrom, from, fromEvent, lastValueFrom, of } from "rxjs"
import { RxjsTcpSocket } from "./RxjsTcpSocket.js"


export class RxjsTcpServer extends Observable<RxjsTcpSocket> {

    $port = new BehaviorSubject<number>(0)

    constructor(start_port: number = 10001) {
        super(o => {
            setTimeout(async () => {
                for (let tcp_port = start_port; true; tcp_port++) {
                    const server = createServer()
                    const success = await new Promise<boolean>(s => {
                        server.once('listening', () => s(true))
                        server.once('error', () => s(false))
                        server.listen(tcp_port)
                    })
                    if (!success) continue
                    this.$port.next(tcp_port)
                    server.on('connection', async socket => {
                        o.next(new RxjsTcpSocket(socket))
                    })
                    const $error = fromEvent(server, 'error') as Observable<Error>
                    await firstValueFrom($error)
                    server.removeAllListeners()
                }
            })
        })
    }

}


