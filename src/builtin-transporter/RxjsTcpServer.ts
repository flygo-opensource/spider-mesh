import { createServer } from "net"
import { Subject } from "rxjs"
import { RxjsTcpSocket } from "./RxjsTcpSocket.js"


export class RxjsTcpServer {


    static start<T = object>(start_port: number = 10001) {
        const $online = new Subject<{
            port: number,
            $connection: Subject<RxjsTcpSocket<T>>,
            $error: Promise<Error>
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
                const $error = new Promise<Error>(s => server.once('error', s))
                $online.next({ port, $connection, $error })
                await $error
                server.removeAllListeners()
            }
        })
        return $online
    }

}


