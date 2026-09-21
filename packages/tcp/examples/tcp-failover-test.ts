import { createTcpTestEnv, start, stopAll, waitForOutput } from './helpers/e2eHarness.js'

async function main() {
    const env = createTcpTestEnv()

    const providerA = start('provider-a', 'examples/tcp-e2e-provider.ts', { ...env, PROVIDER_ID: 'provider-a' })
    const providerB = start('provider-b', 'examples/tcp-e2e-provider.ts', { ...env, PROVIDER_ID: 'provider-b' })
    const providerC = start('provider-c', 'examples/tcp-e2e-provider.ts', { ...env, PROVIDER_ID: 'provider-c' })

    await Promise.all([
        waitForOutput(providerA, /TCP e2e provider ready/, 5000, 'provider-a'),
        waitForOutput(providerB, /TCP e2e provider ready/, 5000, 'provider-b'),
        waitForOutput(providerC, /TCP e2e provider ready/, 5000, 'provider-c'),
    ])

    const client = start('client', 'examples/tcp-failover-client.ts', env)
    await waitForOutput(client, /TCP failover client connected/, 5000, 'client')

    // Wait for phase 1 (6 requests across all 3 nodes)
    const phase1Output = await waitForOutput(client, /"phase1UniqueProviders"/, 15000, 'client')
    console.log(phase1Output.trim())

    // Kill provider-c — TCP connection drop triggers offline event in remaining peers
    providerC.kill('SIGTERM')

    // Wait for phase 2 (4 requests across the 2 surviving nodes)
    const phase2Output = await waitForOutput(client, /"phase2UniqueProviders"/, 20000, 'client')
    console.log(phase2Output.trim())
}

try {
    await main()
} finally {
    await stopAll()
}
