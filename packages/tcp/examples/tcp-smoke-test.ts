import { createTcpTestEnv, start, stopAll, waitForOutput } from './helpers/e2eHarness.js'

async function main() {
    const env = createTcpTestEnv()

    const provider = start('provider', 'examples/tcp-smoke-provider.ts', env)
    await waitForOutput(provider, /TCP smoke provider ready/, 5000, 'provider')

    const client = start('client', 'examples/tcp-smoke-client.ts', env)
    await waitForOutput(client, /TCP smoke client connected/, 5000, 'client')
    const rpcOutput = await waitForOutput(client, /hello tcp smoke from smoke provider/, 15000, 'client')
    const pubsubOutput = await waitForOutput(provider, /"smokeEvent":/, 15000, 'provider')

    console.log(rpcOutput.trim())
    console.log(pubsubOutput.trim())
    console.log('TCP transporter smoke test passed')
}

try {
    await main()
} finally {
    await stopAll()
}