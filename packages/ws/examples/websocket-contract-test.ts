import { createWsTestEnv, randomPort, start, stopAll, waitForOutput } from './helpers/e2eHarness.js'

// websocket-contract-test.ts [relay|topology]
const mode = process.argv[2] === 'topology' ? 'topology' : 'relay'

async function main() {
    const env = { ...createWsTestEnv(randomPort(9500)), CONTRACT_TOPOLOGY: mode === 'topology' ? '1' : '0' }

    const relay = start('relay', 'examples/websocket-server.ts', env)
    await waitForOutput(relay, /listening on/, 5000, 'relay')
    const provider = start('provider', 'examples/contract/provider.ts', env)
    await waitForOutput(provider, /contract provider ready/, 5000, 'provider')

    const client = start('client', 'examples/contract/client.ts', env)
    const output = await waitForOutput(client, /"contract":/, 60000, 'client')
    console.log(output.split('\n').find(line => line.includes('"contract":')))
}

try {
    await main()
} finally {
    await stopAll()
}
