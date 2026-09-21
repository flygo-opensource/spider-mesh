import { expect, setDefaultTimeout, test } from 'bun:test'
import { runBunScript } from './helpers/runBunScript.js'

setDefaultTimeout(180000)

function resilienceEnv(name: string, overrides: Record<string, string> = {}) {
    return {
        SPIDERMESH_NAMESPACE: `${name}-${Date.now()}-${Math.random().toString(36).slice(2)}`,
        SIMPLE_DISCOVERY_PORT: String(27000 + Math.floor(Math.random() * 1000)),
        SPIDERMESH_HTTP2_RECONNECT_ATTEMPTS: '2',
        SPIDERMESH_HTTP2_RECONNECT_DELAY_MS: '50',
        SPIDERMESH_HTTP2_CONNECT_TIMEOUT_MS: '500',
        ...overrides,
    }
}

test('full metadata announcement removes stale services and replaces endpoints', async () => {
    const result = await runBunScript(
        ['run', 'examples/resilience/metadata-shrink-test.ts'],
        10000,
        resilienceEnv('metadata-shrink'),
    )
    expect(result.code).toBe(0)
    expect(result.stderr).toBe('')
    expect(result.stdout).toContain('"metadataShrink":true')
})

test('reconnects on its own after the network recovers, and stops once the node leaves Topology', async () => {
    const result = await runBunScript(
        ['run', 'examples/resilience/reconnect-recovery-test.ts'],
        15000,
        resilienceEnv('reconnect-recovery', {
            SPIDERMESH_HTTP2_CONNECT_TIMEOUT_MS: '200',
            SPIDERMESH_HTTP2_RECONNECT_MAX_DELAY_MS: '300',
        }),
    )
    expect(result.code).toBe(0)
    expect(result.stderr).toBe('')
    expect(result.stdout).toContain('"markedUnreachable":true')
    // Không có discovery nào phát lại: tự nối lại là cách duy nhất để thấy endpoint-recovered.
    expect(result.stdout).toContain('"recoveredWithoutDiscovery":true')
    expect(result.stdout).toContain('"stoppedAfterRemoval":true')
    expect(result.stdout).toContain('endpoint-unreachable')
})

test('soak: registry count stays stable through repeated provider restarts', async () => {
    const result = await runBunScript(
        ['run', 'examples/resilience/soak-test.ts'],
        60000,
        resilienceEnv('soak', {
            SOAK_PROVIDER_COUNT: '10',
            SOAK_DURATION_MS: '30000',
            SOAK_RESTART_EVERY_MS: '1500',
        }),
    )
    expect(result.code).toBe(0)
    expect(result.stderr).toBe('')
    expect(result.stdout).toContain('"soakPassed":true')
    expect(result.stdout).toContain('"minNodes":10')
    expect(result.stdout).toContain('"maxNodes":10')
})

test('SIGKILL keeps topology membership but endpoint recovers after a new discovery snapshot', async () => {
    const result = await runBunScript(
        ['run', 'examples/resilience/sigkill-test.ts'],
        20000,
        resilienceEnv('sigkill'),
    )
    expect(result.code).toBe(0)
    expect(result.stderr).toBe('')
    expect(result.stdout).toContain('"sigkillPassed":true')
    expect(result.stdout).toContain('"topologyMembershipPreserved":true')
    expect(result.stdout).toContain('"rediscovered":true')
    expect(result.stdout).toContain('endpoint-suspect')
    expect(result.stdout).toContain('endpoint-recovered')
})

test('duplicate node_id never merges two processes into one synthetic peer', async () => {
    const result = await runBunScript(
        ['run', 'examples/resilience/duplicate-node-id-test.ts'],
        10000,
        resilienceEnv('duplicate-node-id'),
    )
    expect(result.code).toBe(0)
    expect(result.stderr).toBe('')
    expect(result.stdout).toContain('"duplicateIdSafe":true')
})

test('interrupted RPC stream emits one chunk and exactly one terminal offline error', async () => {
    const result = await runBunScript(
        ['run', 'examples/resilience/stream-interruption-test.ts'],
        15000,
        resilienceEnv('stream-interruption'),
    )
    expect(result.code).toBe(0)
    expect(result.stderr).toBe('')
    expect(result.stdout).toContain('"streamInterruptionPassed":true')
    expect(result.stdout).toContain('"terminalErrorCount":1')
    expect(result.stdout).toContain('"totalEvents":2')
})
