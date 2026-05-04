import { firstValueFrom, timeout, type Observable } from 'rxjs'
import { RemoteServiceLinker, SpiderMesh } from '@spider-mesh/core'
import { WebsocketTransporter } from '../src/index.js'

const wsUrl = process.env.WS_URL || 'ws://127.0.0.1:8787'

type GreetingService = {
    hello(name: string): Observable<string>
}

async function main() {
    const transporter = new WebsocketTransporter({
        heartbeatIntervalMs: 1000,
        reconnectIntervalMs: 500,
    })
    transporter.connect(wsUrl)
    console.log('WebSocket round-robin client connected')

    const mesh = new SpiderMesh({ transporters: [transporter] })
    const greeter = RemoteServiceLinker.link<GreetingService>(mesh, {
        service: 'GreetingService',
        timeout: 3000,
        retry: 2,
    })

    const guard = setTimeout(() => {
        console.error('WebSocket round-robin client timed out')
        process.exit(1)
    }, 12000)

    try {
        await greeter.wait(() => mesh.listRpcNodes('GreetingService').length >= 2)

        const results = await Promise.all([
            firstValueFrom(greeter.hello('round-robin-1').pipe(timeout(5000))),
            firstValueFrom(greeter.hello('round-robin-2').pipe(timeout(5000))),
            firstValueFrom(greeter.hello('round-robin-3').pipe(timeout(5000))),
            firstValueFrom(greeter.hello('round-robin-4').pipe(timeout(5000))),
        ])

        console.log(JSON.stringify({ results }))
        process.exit(0)
    } catch (error) {
        console.error(error)
        process.exit(1)
    } finally {
        clearTimeout(guard)
    }
}

await main()