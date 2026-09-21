import { Microservice } from '@spider-mesh/core'
import { createMesh } from './helpers/createMesh.js'

// Provider with a method that never resolves — used to trigger MICROSERVICE_RPC_TIMEOUT
@Microservice({ role: 'provider', mode: 'tcp-timeout' })
class SlowService {
    async hang(_input: string): Promise<string> {
        await new Promise<never>(() => {})
        return ''
    }
}

new SlowService()
createMesh()

console.log('TCP timeout provider ready')

setInterval(() => undefined, 1000)
