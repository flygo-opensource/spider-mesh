import { createMesh } from '../helpers/createMesh.js'
import { ContractService } from './ContractService.js'

createMesh({ wsUrl: process.env.WS_URL, heartbeatIntervalMs: 1000, reconnectIntervalMs: 500 })
new ContractService()
console.log('contract provider ready')
setInterval(() => undefined, 1000)
