import { Microservice, SpiderMesh } from '@spider-mesh/core'
import { createTransporters } from './helpers/createTransporters.js'

const providerId = process.env.PROVIDER_ID || 'provider'

@Microservice({ role: 'provider', mode: 'tcp-e2e' })
class GreetingService {
    async hello(name: string) {
        return `hello ${name} from ${providerId}`
    }
}

new GreetingService()
new SpiderMesh({ transporters: createTransporters() })

console.log(`TCP e2e provider ready (${providerId})`)

setInterval(() => undefined, 1000)