import process from 'node:process'
import { Observable } from 'rxjs'
import { RemoteServiceLinker } from '@spider-mesh/core'
import { createMesh } from './helpers/createMesh.js'

const wsUrl = process.env.WS_URL || 'ws://127.0.0.1:8787'

type TickService = {
    ticks(): Observable<string>
    activeStreams(): Promise<number>
}

const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms))

async function main() {
    const { mesh } = createMesh({ wsUrl, heartbeatIntervalMs: 1000, reconnectIntervalMs: 500, topology: true })
    console.log('WebSocket stream cancel client connected')

    const ticker = RemoteServiceLinker.link<TickService>(mesh, {
        service: 'TickService',
        timeout: 5000,
        retry: 0,
    })

    const guard = setTimeout(() => {
        console.error('WebSocket stream cancel client guard exceeded')
        process.exit(1)
    }, 25000)

    try {
        await ticker.wait(() => mesh.listRpcNodes('TickService').length > 0)

        // Unsubscribe NGAY trong cùng một tick, trước khi promise của `transporter.send()`
        // resolve. Đây là cửa sổ mà `cancel` từng bị bỏ rơi và provider giữ stream chạy mãi.
        const subscription = ticker.ticks().subscribe({ error: () => undefined })
        subscription.unsubscribe()

        await delay(2000)

        const activeStreams = await ticker.activeStreams()
        console.log(JSON.stringify({ cancelRaceChecked: true, activeStreams }))
        process.exit(activeStreams === 0 ? 0 : 1)
    } catch (error) {
        console.error(error)
        process.exit(1)
    } finally {
        clearTimeout(guard)
    }
}

await main()
