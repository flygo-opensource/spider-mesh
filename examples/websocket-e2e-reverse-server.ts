import { firstValueFrom, timeout, type Observable } from 'rxjs'
import { RemoteServiceLinker, SpiderMesh } from '../src/index.js'
import { WebsocketTransporter } from '../src/transporters/WebsocketTransporter.js'

const wsUrl = process.env.WS_URL || 'ws://127.0.0.1:8787'

const transporterOptions = {
    heartbeatIntervalMs: 1000,
    reconnectIntervalMs: 500,
}

const transporter = new WebsocketTransporter(wsUrl, transporterOptions)

const subscription = transporter.subscribe(event => {
    if (event?.metadata?.connected) {
        console.log('WebSocket reverse e2e server connected')
        subscription.unsubscribe()
    }
})

type ClientResponderService = {
    helloFromServer(name: string): Observable<string>
}

async function main() {
    const mesh = new SpiderMesh({ transporters: [transporter] })
    const responder = RemoteServiceLinker.link<ClientResponderService>(mesh, {
        service: 'ClientResponderService',
        timeout: 3000,
        retry: 2,
    })

    const guard = setTimeout(() => {
        console.error('WebSocket reverse e2e server timed out')
        process.exit(1)
    }, 10000)

    try {
        await responder.wait(() => mesh.listRpcNodes('ClientResponderService').length > 0)
        const result = await firstValueFrom(responder.helloFromServer('from server').pipe(timeout(5000)))
        console.log(result)
        process.exit(0)
    } catch (error) {
        console.error(error)
        process.exit(1)
    } finally {
        clearTimeout(guard)
    }
}

await main()