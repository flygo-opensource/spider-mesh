import { expect, setDefaultTimeout, test } from 'bun:test'
import { runBunScript } from './helpers/runBunScript.js'

setDefaultTimeout(120000)

type ContractResult = { name: string, pass: boolean, actual?: unknown, expected?: unknown }

// Mọi dạng trả về/lỗi của RPC qua HTTP/2, provider và client tìm nhau qua UDP.
test('tcp rpc contract: every return and error shape', async () => {
    const result = await runBunScript(['run', 'examples/tcp-contract-test.ts'], 100000)
    expect(result.stderr).toBe('')
    expect(result.code).toBe(0)
    const line = result.stdout.split('\n').find(l => l.includes('"contract":'))
    expect(line).toBeDefined()

    const results = (JSON.parse(line!) as { contract: ContractResult[] }).contract
    const failures = results.filter(item => !item.pass)
    expect(failures.map(({ name, actual, expected }) => ({ name, actual, expected }))).toEqual([])
    expect(results.length).toBeGreaterThanOrEqual(74)
})
