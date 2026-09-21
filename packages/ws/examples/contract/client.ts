import { createMesh } from '../helpers/createMesh.js'
import { runContract } from './runContract.js'

// CONTRACT_TOPOLOGY=1: node được chọn qua Topology thay vì để relay tự chọn.
const { mesh } = createMesh({
    wsUrl: process.env.WS_URL,
    heartbeatIntervalMs: 1000,
    reconnectIntervalMs: 500,
    topology: process.env.CONTRACT_TOPOLOGY === '1',
})
const results = await runContract(mesh, 'websocket')
console.log(JSON.stringify({ contract: results }))
process.exit(0)
