import { expect, test } from 'bun:test'
import { runBunScript } from './helpers/runBunScript.js'

test('tcp spidermesh rpc e2e', async () => {
    const result = await runBunScript(['run', 'examples/tcp-e2e-test.ts'], 25000)

    expect(result.code).toBe(0)
    expect(result.stderr).toBe('')
    expect(result.stdout).toContain('hello tcp e2e from provider')
})