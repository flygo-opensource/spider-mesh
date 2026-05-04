import { createTcpTestEnv, start, stopAll, waitForOutput } from './helpers/e2eHarness.js'

async function main() {
    const env = {
        ...createTcpTestEnv(),
        SPIDERMESH_MULTICAST_PORT: String(24000 + Math.floor(Math.random() * 1000)),
    }

    const listener = start('discovery-contract-listener', 'examples/tcp-discovery-contract-listener.ts', env)
    await waitForOutput(listener, /DISCOVERY_LISTENER_READY/, 8000, 'discovery-contract-listener')

    const sender = start('discovery-contract-sender', 'examples/tcp-discovery-contract-sender.ts', env)
    await waitForOutput(sender, /DISCOVERY_SENDER_SENT/, 8000, 'discovery-contract-sender')

    const discoveryOutput = await waitForOutput(listener, /"type":"discovery"/, 8000, 'discovery-contract-listener')
    console.log(discoveryOutput.trim())
    console.log('TCP discovery contract test passed')
}

try {
    await main()
} finally {
    await stopAll()
}