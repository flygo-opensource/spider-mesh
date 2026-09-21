import { createTcpTestEnv, start, stopAll, waitForOutput } from './helpers/e2eHarness.js'

async function main() {
    const env = createTcpTestEnv()

    const client = start('client', 'examples/tcp-e2e-round-robin-client.ts', env)
    await waitForOutput(client, /TCP round-robin client connected/, 5000, 'client')

    const providerA = start('provider-a', 'examples/tcp-e2e-provider.ts', {
        ...env,
        PROVIDER_ID: 'provider-a'
    })
    await waitForOutput(providerA, /TCP e2e provider ready/, 5000, 'provider-a')

    const providerB = start('provider-b', 'examples/tcp-e2e-provider.ts', {
        ...env,
        PROVIDER_ID: 'provider-b'
    })
    await waitForOutput(providerB, /TCP e2e provider ready/, 5000, 'provider-b')

    const output = await waitForOutput(client, /"results":\[/, 15000, 'client')
    console.log(output.trim())
}

try {
    await main()
} finally {
    await stopAll()
}