import process from 'node:process'
import { Observable } from 'rxjs'
import { RemoteServiceLinker } from '@spider-mesh/core'
import { createMesh } from './helpers/createMesh.js'

const wsUrl = process.env.WS_URL || 'ws://127.0.0.1:8787'

type TickService = {
    ticks(): Observable<string>
    activeStreams(): Promise<number>
}

async function main() {
    const { mesh } = createMesh({ wsUrl, heartbeatIntervalMs: 1000, reconnectIntervalMs: 500, topology: true })
    console.log('WebSocket stream offline client connected')

    // Cố ý KHÔNG đặt `timeout`: test phải chứng minh stream đóng nhờ tín hiệu offline,
    // không phải nhờ idle-timeout của chính caller.
    const ticker = RemoteServiceLinker.link<TickService>(mesh, {
        service: 'TickService',
        retry: 0,
    })

    const guard = setTimeout(() => {
        console.error('WebSocket stream offline client guard exceeded')
        process.exit(1)
    }, 25000)

    await ticker.wait(() => mesh.listRpcNodes('TickService').length > 0)

    let received = 0
    let lastTickAt = 0

    ticker.ticks().subscribe({
        next: () => {
            received++
            lastTickAt = Date.now()
            if (received === 1) console.log(JSON.stringify({ streamStarted: true }))
        },
        error: (error: any) => {
            clearTimeout(guard)
            console.log(JSON.stringify({
                streamErrored: true,
                code: error?.code,
                received,
                sinceLastTickMs: Date.now() - lastTickAt,
            }))
            process.exit(error?.code === 'MICROSERVICE_OFFLINE' ? 0 : 1)
        },
        complete: () => {
            clearTimeout(guard)
            console.error('Stream completed instead of erroring after the provider went offline')
            process.exit(1)
        },
    })
}

await main()
