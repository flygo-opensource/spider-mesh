import { expect, test } from 'bun:test'
import { runBunScript } from './helpers/runBunScript.js'

test('tcp transporters smoke e2e', async () => {
    const result = await runBunScript(['run', 'examples/tcp-smoke-test.ts'], 25000)

    expect(result.code).toBe(0)
    expect(result.stderr).toBe('')
    expect(result.stdout).toContain('TCP transporter smoke test passed')
    expect(result.stdout).toContain('hello tcp smoke from smoke provider')
    expect(result.stdout).toContain('"message":"pubsub-ok"')
})