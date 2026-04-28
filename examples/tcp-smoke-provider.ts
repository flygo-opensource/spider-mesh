import { Microservice, SpiderMesh } from '@spider-mesh/core'
import { createTransporters } from './helpers/createTransporters.js'

class SmokeEvent {
    constructor(
        public readonly message: string,
        public readonly sender: string,
    ) { }
}

@Microservice({ role: 'provider', mode: 'tcp-smoke' })
class GreetingService {
    async hello(name: string) {
        return `hello ${name} from smoke provider`
    }
}

async function main() {
    new GreetingService()
    const mesh = new SpiderMesh({ transporters: createTransporters() })
    mesh.linkEvent(SmokeEvent).listen().subscribe(event => {
        console.log(JSON.stringify({ smokeEvent: event }))
        process.exit(0)
    })

    console.log('TCP smoke provider ready')
    setInterval(() => undefined, 1000)
}

await main()