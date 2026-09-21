import { expect, test } from 'bun:test'
import { runBunScript } from './helpers/runBunScript.js'

// Regression: a no-retry client routing while a SECOND provider is mid-startup must never
// pick the half-formed peer (no Http2Rpc port yet). pickRpcNode's readiness filter guarantees
// this. Proven with real child processes (provider A + late provider B + looping client).
test('tcp multi-provider mid-startup: no-retry client never routes to a half-formed peer', async () => {
    const result = await runBunScript(['run', 'examples/multi-provider-noretry-test.ts'], 30000)

    expect(result.code).toBe(0)
    expect(result.stdout).toContain('ALL_OK')
})
