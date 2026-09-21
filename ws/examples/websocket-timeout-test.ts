import { createWsTestEnv, randomPort, start, stopAll, waitForOutput } from './helpers/e2eHarness.js'

async function main() {
    const port = randomPort(9800)
    const env = createWsTestEnv(port)

    const relay = start('relay', 'examples/websocket-server.ts', env)
    await waitForOutput(relay, /listening on/, 5000, 'relay')

    const provider = start('provider', 'examples/websocket-timeout-provider.ts', env)
    await waitForOutput(provider, /WebSocket timeout provider ready/, 5000, 'provider')

    const client = start('client', 'examples/websocket-timeout-client.ts', env)
    await waitForOutput(client, /WebSocket timeout client connected/, 5000, 'client')

    // Client calls a hanging method with timeout:2000 — must get MICROSERVICE_RPC_TIMEOUT
    const output = await waitForOutput(client, /"timeoutDetected":true/, 12000, 'client')
    console.log(output.trim())
}

try {
    await main()
} finally {
    await stopAll()
}
