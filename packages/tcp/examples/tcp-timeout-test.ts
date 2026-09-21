import { createTcpTestEnv, start, stopAll, waitForOutput } from './helpers/e2eHarness.js'

async function main() {
    const env = createTcpTestEnv()

    const provider = start('provider', 'examples/tcp-timeout-provider.ts', env)
    await waitForOutput(provider, /TCP timeout provider ready/, 5000, 'provider')

    const client = start('client', 'examples/tcp-timeout-client.ts', env)
    await waitForOutput(client, /TCP timeout client connected/, 5000, 'client')

    // Client calls a hanging method with timeout:2000 — must get MICROSERVICE_RPC_TIMEOUT
    const output = await waitForOutput(client, /"timeoutDetected":true/, 10000, 'client')
    console.log(output.trim())
}

try {
    await main()
} finally {
    await stopAll()
}
