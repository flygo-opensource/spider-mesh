import { Microservice } from '@spider-mesh/core'
import { createMesh } from './helpers/createMesh.js'

@Microservice({ role: 'client', mode: 'tcp-reverse-e2e' })
class ClientResponderService {
    async helloFromServer(name: string) {
        return `hello ${name} from client`
    }
}

new ClientResponderService()
createMesh()

console.log('TCP reverse e2e client connected')

setInterval(() => undefined, 1000)