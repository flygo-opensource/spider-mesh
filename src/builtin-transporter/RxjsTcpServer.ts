import { createServer } from "net"
import { Observable, Subject, firstValueFrom, fromEvent, tap } from "rxjs"
import { RxjsTcpSocket } from "./RxjsTcpSocket.js"


export class RxjsTcpServer {


    static start<T = object>(start_port: number = 10001) {
        const $online = new Subject<{
            port: number,
            $connection: Subject<RxjsTcpSocket<T>>,
            $error: Observable<Error>
        }>()


        setTimeout(async () => {
            for (let port = start_port; true; port++) {
                const $connection = new Subject<RxjsTcpSocket<T>>()
                const server = createServer()
                const success = await new Promise<boolean>(s => {
                    server.once('listening', () => s(true))
                    server.once('error', () => s(false))
                    server.listen(port)
                })
                if (!success) continue
                server.on('connection', async socket => {
                    const stable_socket = await RxjsTcpSocket.join<T>(socket)
                    $connection.next(stable_socket)
                })
                const $error = fromEvent(server, 'error') as Observable<Error>
                $online.next({ port, $connection, $error })
                await firstValueFrom($error)
                server.removeAllListeners()
            }
        })
        return { $online }
    }

}


