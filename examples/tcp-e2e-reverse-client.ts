import { Microservice, SpiderMesh } from '@spider-mesh/core'
import { createTransporters } from './helpers/createTransporters.js'

@Microservice({ role: 'client', mode: 'tcp-reverse-e2e' })
class ClientResponderService {
    async helloFromServer(name: string) {
        return `hello ${name} from client`
    }
}

new ClientResponderService()
new SpiderMesh({ transporters: createTransporters() })

console.log('TCP reverse e2e client connected')

setInterval(() => undefined, 1000)