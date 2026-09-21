import { Microservice } from '@spider-mesh/core'
import { createMesh } from './helpers/createMesh.js'

// SERVICES=a  → chỉ ServiceA
// SERVICES=a,b → ServiceA và ServiceB
const services = (process.env.SERVICES || 'a').split(',')
const wsUrl = process.env.WS_URL || 'ws://127.0.0.1:8787'

@Microservice({ role: 'provider', mode: 'e2e' })
class ServiceA {
    async ping() { return 'pong-from-ServiceA' }
}

@Microservice({ role: 'provider', mode: 'e2e' })
class ServiceB {
    async ping() { return 'pong-from-ServiceB' }
}

if (services.includes('a')) new ServiceA()
if (services.includes('b')) new ServiceB()

createMesh({ wsUrl, heartbeatIntervalMs: 1000, reconnectIntervalMs: 500 })

console.log(`WebSocket discovery provider ready (SERVICES=${services.join(',')})`)
setInterval(() => undefined, 1000)
