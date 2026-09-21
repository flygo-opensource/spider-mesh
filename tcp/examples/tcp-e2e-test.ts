import { createTcpTestEnv, start, stopAll, waitForOutput } from './helpers/e2eHarness.js'

async function main() {
    const env = createTcpTestEnv()

    const client = start('client', 'examples/tcp-e2e-client.ts', env)
    await waitForOutput(client, /TCP e2e client connected/, 5000, 'client')

    const provider = start('provider', 'examples/tcp-e2e-provider.ts', env)
    await waitForOutput(provider, /TCP e2e provider ready/, 5000, 'provider')

    const output = await waitForOutput(client, /hello tcp e2e from provider/, 15000, 'client')
    console.log(output.trim())
}

try {
    await main()
} finally {
    await stopAll()
}