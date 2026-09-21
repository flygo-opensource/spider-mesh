import { createWsTestEnv, randomPort, start, stopAll, waitForOutput } from './helpers/e2eHarness.js'

async function main() {
    const port = randomPort(9860)
    const env = createWsTestEnv(port)

    const relay = start('relay', 'examples/websocket-server.ts', env)
    await waitForOutput(relay, /listening on/, 5000, 'relay')

    // Reuse existing provider (GreetingService.hello)
    const provider = start('provider', 'examples/websocket-e2e-provider.ts', env)
    await waitForOutput(provider, /provider ready/, 5000, 'provider')

    const client = start('client', 'examples/websocket-concurrent-client.ts', env)
    await waitForOutput(client, /WebSocket concurrent client connected/, 5000, 'client')

    // All 10 concurrent calls must complete correctly
    const output = await waitForOutput(client, /"concurrentOk":true/, 15000, 'client')
    console.log(output.trim())
}

try {
    await main()
} finally {
    await stopAll()
}
