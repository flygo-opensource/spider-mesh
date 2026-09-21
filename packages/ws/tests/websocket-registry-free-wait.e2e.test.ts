import { expect, test } from 'bun:test'
import { runBunScript } from './helpers/runBunScript.js'

// Regression guard for the registry-free availability fix: a client started BEFORE any
// provider must NOT fake-resolve wait(). It should block until the relay reports a real
// provider, then succeed. Proven with real child processes (relay + late provider + client).
test('ws registry-free: client before provider blocks then succeeds', async () => {
    const result = await runBunScript(['run', 'examples/registry-free-wait-test.ts'], 40000)

    expect(result.code).toBe(0)
    expect(result.stdout).toContain('hello world')
})
