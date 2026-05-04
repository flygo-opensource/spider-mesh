import { expect, setDefaultTimeout, test } from 'bun:test'
import { runBunScript } from './helpers/runBunScript.js'

setDefaultTimeout(30000)

test('tcp rpc transporter contract e2e', async () => {
    const result = await runBunScript(['run', 'examples/tcp-rpc-contract-test.ts'], 25000)

    expect(result.code).toBe(0)
    expect(result.stderr).toBe('')
    expect(result.stdout).toContain('TCP RPC contract test passed')
    expect(result.stdout).toContain('"hasRpc":true')
    expect(result.stdout).toContain('"hasMessage":false')
    expect(result.stdout).toContain('"packetKind":"request"')
})

test('tcp discovery transporter contract e2e', async () => {
    const result = await runBunScript(['run', 'examples/tcp-discovery-contract-test.ts'], 25000)

    expect(result.code).toBe(0)
    expect(result.stderr).toBe('')
    expect(result.stdout).toContain('TCP discovery contract test passed')
    expect(result.stdout).toContain('"hasDiscovered":true')
    expect(result.stdout).toContain('"hasRawNodeId":false')
    expect(result.stdout).toContain('"discoveredNodeId":"discovery-contract-sender"')
})