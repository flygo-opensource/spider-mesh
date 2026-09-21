import { createWsTestEnv, randomPort, start, stopAll, waitForOutput } from './helpers/e2eHarness.js'

async function main() {
    const port = randomPort(9600)
    const env = createWsTestEnv(port)

    const relay = start('relay', 'examples/websocket-server.ts', env)
    await waitForOutput(relay, /listening on/, 5000, 'relay')

    const provider = start('provider', 'examples/websocket-stream-provider.ts', env)
    await waitForOutput(provider, /stream provider ready/, 5000, 'provider')

    const client = start('client', 'examples/websocket-stream-cancel-client.ts', env)
    const output = await waitForOutput(client, /"cancelRaceChecked":true/, 20000, 'client')
    console.log(output.split('\n').find(line => line.includes('"cancelRaceChecked"'))?.trim())
}

try {
    await main()
} finally {
    await stopAll()
}
