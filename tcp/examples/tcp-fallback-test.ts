import { createTcpTestEnv, start, stopAll, waitForOutput } from './helpers/e2eHarness.js'

async function main() {
    const env = createTcpTestEnv()

    // No provider started — client must receive the configured fallback value
    const client = start('client', 'examples/tcp-fallback-client.ts', env)
    await waitForOutput(client, /TCP fallback client connected/, 5000, 'client')

    const output = await waitForOutput(client, /"fallbackReceived":true/, 8000, 'client')
    console.log(output.trim())
}

try {
    await main()
} finally {
    await stopAll()
}
