import { createTcpTestEnv, start, stopAll, waitForOutput } from './helpers/e2eHarness.js'

async function main() {
    const env = {
        ...createTcpTestEnv(),
        OHAYO_DISCOVERY_PORT: String(26000 + Math.floor(Math.random() * 1000)),
    }

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
