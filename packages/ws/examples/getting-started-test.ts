/**
 * E2E orchestrator: runs the ws getting-started example as REAL child processes
 * (relay + provider + client), proving the registry-free, relay-routed flow works
 * across processes. Exits 0 on success.
 */
import { createWsTestEnv, randomPort, start, waitForOutput, stopAll } from './helpers/e2eHarness.js'

async function main() {
    const env = createWsTestEnv(randomPort(8900))

    const relay = start('relay', 'examples/getting-started/relay.ts', env)
    await waitForOutput(relay, /relay listening/, 8000, 'relay')

    const provider = start('provider', 'examples/getting-started/provider.ts', env)
    await waitForOutput(provider, /provider online/, 8000, 'provider')

    const client = start('client', 'examples/getting-started/client.ts', env)
    const out = await waitForOutput(client, /hello world/, 15000, 'client')
    console.log(out.trim())
}

try {
    await main()
} finally {
    await stopAll()
}
