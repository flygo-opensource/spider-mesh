import { expect, test } from 'bun:test'
import { runBunScript } from './helpers/runBunScript.js'

test('websocket transporter e2e', async () => {
    const result = await runBunScript(['run', 'examples/websocket-smoke-test.ts'], 20000)

    expect(result.code).toBe(0)
    expect(result.stderr).toBe('')
    expect(result.stdout).toContain('WebSocket binary smoke test passed')
})