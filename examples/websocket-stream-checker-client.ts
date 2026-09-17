import process from 'node:process'
import { Observable } from 'rxjs'
import { RemoteServiceLinker } from '@spider-mesh/core'
import { createMesh } from './helpers/createMesh.js'

const wsUrl = process.env.WS_URL || 'ws://127.0.0.1:8787'

type TickService = {
    ticks(): Observable<string>
    activeStreams(): Promise<number>
}

/** Hỏi provider còn bao nhiêu stream đang chạy — dùng để phát hiện stream rò rỉ. */
async function main() {
    const { mesh } = createMesh({ wsUrl, heartbeatIntervalMs: 1000, reconnectIntervalMs: 500, topology: true })
    console.log('WebSocket stream checker client connected')

    const ticker = RemoteServiceLinker.link<TickService>(mesh, {
        service: 'TickService',
        timeout: 5000,
        retry: 0,
    })

    const guard = setTimeout(() => {
        console.error('WebSocket stream checker client guard exceeded')
        process.exit(1)
    }, 20000)

    try {
        await ticker.wait(() => mesh.listRpcNodes('TickService').length > 0)
        const activeStreams = await ticker.activeStreams()
        console.log(JSON.stringify({ activeStreamsChecked: true, activeStreams }))
        process.exit(0)
    } catch (error) {
        console.error(error)
        process.exit(1)
    } finally {
        clearTimeout(guard)
    }
}

await main()
