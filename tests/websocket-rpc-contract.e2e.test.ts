import { expect, setDefaultTimeout, test } from 'bun:test'
import { runBunScript } from './helpers/runBunScript.js'

setDefaultTimeout(120000)

type ContractResult = { name: string, pass: boolean, actual?: unknown, expected?: unknown }

async function runContract(mode: 'relay' | 'topology') {
    const result = await runBunScript(['run', 'examples/websocket-contract-test.ts', mode], 100000)
    expect(result.stderr).toBe('')
    expect(result.code).toBe(0)
    const line = result.stdout.split('\n').find(l => l.includes('"contract":'))
    expect(line).toBeDefined()
    return (JSON.parse(line!) as { contract: ContractResult[] }).contract
}

function expectAllPass(results: ContractResult[]) {
    const failures = results.filter(result => !result.pass)
    // In ra từng ca sai kèm giá trị thật/mong đợi để dễ đọc khi fail.
    expect(failures.map(({ name, actual, expected }) => ({ name, actual, expected }))).toEqual([])
    expect(results.length).toBeGreaterThanOrEqual(74)
}

// Mọi dạng trả về/lỗi của RPC qua relay tự chọn provider (không Topology).
test('websocket rpc contract: every return and error shape (relay routing)', async () => {
    expectAllPass(await runContract('relay'))
})

// Cùng bộ hợp đồng khi node được chọn qua Topology.
test('websocket rpc contract: every return and error shape (topology routing)', async () => {
    expectAllPass(await runContract('topology'))
})
