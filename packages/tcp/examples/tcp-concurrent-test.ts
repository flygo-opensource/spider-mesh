import { createTcpTestEnv, start, stopAll, waitForOutput } from './helpers/e2eHarness.js'

async function main() {
    const env = createTcpTestEnv()

    // Reuse existing provider (GreetingService.hello)
    const provider = start('provider', 'examples/tcp-e2e-provider.ts', env)
    await waitForOutput(provider, /TCP e2e provider ready/, 5000, 'provider')

    const client = start('client', 'examples/tcp-concurrent-client.ts', env)
    await waitForOutput(client, /TCP concurrent client connected/, 5000, 'client')

    // All 10 concurrent calls must complete correctly
    const output = await waitForOutput(client, /"concurrentOk":true/, 15000, 'client')
    console.log(output.trim())
}

try {
    await main()
} finally {
    await stopAll()
}
