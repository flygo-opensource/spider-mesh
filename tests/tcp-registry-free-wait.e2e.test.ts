import { expect, test } from 'bun:test'
import { runBunScript } from './helpers/runBunScript.js'

// Regression: with a registry-free core, availability comes from the transporter's
// ServiceDirectory. A client started BEFORE its provider must block on wait() and only
// succeed once discovery actually finds the provider — proven with real child processes.
test('tcp registry-free: client before provider blocks then succeeds', async () => {
    const result = await runBunScript(['run', 'examples/registry-free-wait-test.ts'], 40000)

    expect(result.code).toBe(0)
    expect(result.stdout).toContain('hello world')
})
