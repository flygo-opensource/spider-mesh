import process from 'node:process'
import { Observable } from 'rxjs'
import { RemoteServiceLinker } from '@spider-mesh/core'
import { createMesh } from './helpers/createMesh.js'

const wsUrl = process.env.WS_URL || 'ws://127.0.0.1:8787'

type TickService = {
    ticks(): Observable<string>
    activeStreams(): Promise<number>
}

/** Subscribe một stream dài hạn rồi để process bị kill — provider không được giữ stream lại. */
async function main() {
    const { mesh } = createMesh({ wsUrl, heartbeatIntervalMs: 1000, reconnectIntervalMs: 500, topology: true })
    console.log('WebSocket stream leaked client connected')

    const ticker = RemoteServiceLinker.link<TickService>(mesh, {
        service: 'TickService',
        retry: 0,
    })

    await ticker.wait(() => mesh.listRpcNodes('TickService').length > 0)

    let received = 0
    ticker.ticks().subscribe({
        next: () => {
            received++
            if (received === 1) console.log(JSON.stringify({ streamStarted: true }))
        },
        error: () => undefined,
    })

    setInterval(() => undefined, 1000)
}

await main()
