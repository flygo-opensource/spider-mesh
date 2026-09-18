import { expect, test } from 'bun:test'
import { runBunScript } from './helpers/runBunScript.js'

// Topology starts empty. A client started BEFORE its provider must block on wait() and only
// succeed once Discovery adds a routable HTTP/2 endpoint.
test('tcp optional topology: client before provider blocks then succeeds', async () => {
    const result = await runBunScript(['run', 'examples/registry-free-wait-test.ts'], 40000)

    expect(result.code).toBe(0)
    expect(result.stdout).toContain('hello world')
})
