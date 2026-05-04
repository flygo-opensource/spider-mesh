import { expect, test } from 'bun:test'
import { runBunScript } from './helpers/runBunScript.js'

test('websocket spidermesh rpc e2e', async () => {
    const result = await runBunScript(['run', 'examples/websocket-e2e-test.ts'], 20000)

    expect(result.code).toBe(0)
    expect(result.stderr).toBe('')
    expect(result.stdout).toContain('hello websocket e2e from provider')
})