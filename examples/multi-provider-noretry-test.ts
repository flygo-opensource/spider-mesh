/**
 * E2E orchestrator (tcp): regression for the "half-formed peer routing" race.
 *
 * Provider A is fully up; the client then loops calls with retry:0; Provider B is started
 * mid-loop so it is half-formed (service known, Http2Rpc port not yet bound) while the client
 * is routing. With the readiness filter in pickRpcNode, B is skipped until ready and every
 * call succeeds (ALL_OK). Without it, a call round-robins to half-formed B and fails.
 */
import { createTcpTestEnv, start, waitForOutput, stopAll } from './helpers/e2eHarness.js'

async function main() {
    const env = createTcpTestEnv()

    const providerA = start('providerA', 'examples/getting-started/provider.ts', env)
    await waitForOutput(providerA, /provider online/, 8000, 'providerA')

    const client = start('client', 'examples/multi-provider-noretry-client.ts', env)
    await waitForOutput(client, /CLIENT_LOOPING/, 12000, 'client')

    // B forms while the client hammers calls with retry:0.
    start('providerB', 'examples/getting-started/provider.ts', env)

    const out = await waitForOutput(client, /ALL_OK/, 12000, 'client')
    console.log(out.trim())
}

try {
    await main()
} finally {
    await stopAll()
}
