import { createTcpTestEnv, start, stopAll, waitForOutput } from './helpers/e2eHarness.js'

async function main() {
    const env = createTcpTestEnv()

    const provider = start('provider', 'examples/tcp-e2e-provider.ts', env)
    await waitForOutput(provider, /TCP e2e provider ready/, 5000, 'provider')

    const client = start('client', 'examples/tcp-offline-client.ts', env)
    await waitForOutput(client, /TCP offline client connected/, 5000, 'client')

    // Wait for client to complete first successful call, then emit that output
    const firstOutput = await waitForOutput(client, /"firstCallOk":true/, 10000, 'client')
    console.log(firstOutput.trim())

    // Kill the provider to simulate a crash — connection drop triggers offline event
    provider.kill('SIGTERM')

    // Client detects offline and reports; emit that output too
    const offlineOutput = await waitForOutput(client, /"offlineDetected":true/, 12000, 'client')
    console.log(offlineOutput.trim())
}

try {
    await main()
} finally {
    await stopAll()
}
