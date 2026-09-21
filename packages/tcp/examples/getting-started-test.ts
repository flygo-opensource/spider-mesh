/**
 * E2E orchestrator: runs the tcp getting-started example as REAL child processes
 * (provider + client), proving generic discovery + a transporter-owned Registry across processes.
 * Exits 0 on success.
 */
import { createTcpTestEnv, start, waitForOutput, stopAll } from './helpers/e2eHarness.js'

async function main() {
    const env = createTcpTestEnv()

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
