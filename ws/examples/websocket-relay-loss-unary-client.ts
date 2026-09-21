import process from 'node:process'
import { RemoteServiceLinker } from '@spider-mesh/core'
import { createMesh } from './helpers/createMesh.js'

const wsUrl = process.env.WS_URL || 'ws://127.0.0.1:8787'

type SlowService = {
    hang(input: string): Promise<string>
}

async function main() {
    const { mesh } = createMesh({ wsUrl, heartbeatIntervalMs: 1000, reconnectIntervalMs: 500, topology: true })
    console.log('WebSocket relay-loss unary client connected')

    // Cố ý KHÔNG đặt `timeout`: request chỉ được kết thúc nhờ tín hiệu mất kết nối.
    const slow = RemoteServiceLinker.link<SlowService>(mesh, {
        service: 'SlowService',
        retry: 0,
    })

    const guard = setTimeout(() => {
        console.error('WebSocket relay-loss unary client guard exceeded')
        process.exit(1)
    }, 25000)

    await slow.wait(() => mesh.listRpcNodes('SlowService').length > 0)
    console.log(JSON.stringify({ unaryCalling: true }))

    try {
        await slow.hang('never-answered')
        console.error('SlowService.hang resolved, but it never answers')
        process.exit(1)
    } catch (error: any) {
        clearTimeout(guard)
        console.log(JSON.stringify({ unaryErrored: true, code: error?.code }))
        process.exit(error?.code === 'MICROSERVICE_OFFLINE' ? 0 : 1)
    }
}

await main()
