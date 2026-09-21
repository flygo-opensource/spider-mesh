import { Microservice } from '@spider-mesh/core'
import { createMesh } from './helpers/createMesh.js'

// SERVICES=a  → chỉ ServiceA
// SERVICES=a,b → ServiceA và ServiceB
const services = (process.env.SERVICES || 'a').split(',')

@Microservice({ role: 'provider', mode: 'tcp-e2e' })
class ServiceA {
    async ping() { return 'pong-from-ServiceA' }
}

@Microservice({ role: 'provider', mode: 'tcp-e2e' })
class ServiceB {
    async ping() { return 'pong-from-ServiceB' }
}

if (services.includes('a')) new ServiceA()
if (services.includes('b')) new ServiceB()

createMesh()

console.log(`TCP discovery provider ready (SERVICES=${services.join(',')})`)
setInterval(() => undefined, 1000)
