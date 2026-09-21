import { createWsTestEnv, randomPort, start, stopAll, waitForOutput } from './helpers/e2eHarness.js'

async function main() {
    const port = randomPort(9400)
    const env = createWsTestEnv(port)

    const relay = start('relay', 'examples/websocket-server.ts', env)
    await waitForOutput(relay, /listening on/, 5000, 'relay')

    const provider = start('provider', 'examples/websocket-stream-provider.ts', env)
    await waitForOutput(provider, /stream provider ready/, 5000, 'provider')

    const client = start('client', 'examples/websocket-stream-offline-client.ts', env)
    await waitForOutput(client, /"streamStarted":true/, 15000, 'client')

    // Provider chết giữa lúc stream đang chạy. Client không đặt `timeout`, nên nếu không có
    // tín hiệu offline thì nó sẽ treo im vô hạn.
    provider.kill('SIGKILL')

    const output = await waitForOutput(client, /"streamErrored":true/, 10000, 'client')
    console.log(output.split('\n').find(line => line.includes('"streamErrored"'))?.trim())
}

try {
    await main()
} finally {
    await stopAll()
}
