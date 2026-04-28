import { expect, test } from 'bun:test'
import { runBunScript } from './helpers/runBunScript.js'

test('tcp spidermesh rpc matrix e2e', async () => {
    const result = await runBunScript(['run', 'examples/tcp-e2e-matrix-test.ts'], 30000)

    expect(result.code).toBe(0)
    expect(result.stderr).toBe('')
    expect(result.stdout).toContain('"syncValue":"sync:case-sync"')
    expect(result.stdout).toContain('"asyncValue":"async:case-async"')
    expect(result.stdout).toContain('"syncObservable":["sync-observable:case-sync-observable:1","sync-observable:case-sync-observable:2"]')
    expect(result.stdout).toContain('"asyncObservable":["async-observable:case-async-observable:1","async-observable:case-async-observable:2"]')
    expect(result.stdout).toContain('"errors":["sync-error","async-error","observable-error"]')
})