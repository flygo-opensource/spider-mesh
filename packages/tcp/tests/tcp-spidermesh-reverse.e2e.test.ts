import { expect, test } from 'bun:test'
import { runBunScript } from './helpers/runBunScript.js'

test('tcp spidermesh reverse rpc e2e', async () => {
    const result = await runBunScript(['run', 'examples/tcp-e2e-reverse-test.ts'], 25000)

    expect(result.code).toBe(0)
    expect(result.stderr).toBe('')
    expect(result.stdout).toContain('hello from server from client')
})