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
        console.log('WebSocket e2e client connected')
        subscription.unsubscribe()
    }
})

type GreetingService = {
    hello(name: string): Observable<string>
}

async function main() {
    const mesh = new SpiderMesh({ transporters: [transporter] })
    const greeter = RemoteServiceLinker.link<GreetingService>(mesh, {
        service: 'GreetingService',
        timeout: 3000,
        retry: 2,
    })

    const guard = setTimeout(() => {
        console.error('WebSocket e2e client timed out')
        process.exit(1)
    }, 10000)

    try {
        await greeter.wait(() => mesh.listRpcNodes('GreetingService').length > 0)
        const result = await firstValueFrom(greeter.hello('websocket e2e').pipe(timeout(5000)))
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