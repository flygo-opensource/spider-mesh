import { createWsTestEnv, randomPort, start, stopAll, waitForOutput } from './helpers/e2eHarness.js'

async function main() {
    const port = randomPort(9820)
    const env = createWsTestEnv(port)

    const relay = start('relay', 'examples/websocket-server.ts', env)
    await waitForOutput(relay, /listening on/, 5000, 'relay')

    // No provider started — only the client runs, must receive fallback value
    const client = start('client', 'examples/websocket-fallback-client.ts', env)
    await waitForOutput(client, /WebSocket fallback client connected/, 5000, 'client')

    const output = await waitForOutput(client, /"fallbackReceived":true/, 8000, 'client')
    console.log(output.trim())
}

try {
    await main()
} finally {
    await stopAll()
}
