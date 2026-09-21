import { createTcpTestEnv, start, stopAll, waitForOutput } from './helpers/e2eHarness.js'

async function main() {
    const env = createTcpTestEnv()

    const client = start('client', 'examples/tcp-e2e-matrix-client.ts', env)
    await waitForOutput(client, /TCP matrix e2e client connected/, 5000, 'client')

    const provider = start('provider', 'examples/tcp-e2e-matrix-provider.ts', env)
    await waitForOutput(provider, /TCP matrix e2e provider ready/, 5000, 'provider')

    const output = await waitForOutput(client, /"syncValue":"sync:case-sync"/, 15000, 'client')
    console.log(output.trim())
}

try {
    await main()
} finally {
    await stopAll()
}