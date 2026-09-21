import { expect, test } from 'bun:test'
import { runBunScript } from './helpers/runBunScript.js'

test('Topology TTL evicts the stale node and discovery adds the restarted node with a new ID', async () => {
    const result = await runBunScript(['run', 'examples/tcp-session-recovery-test.ts'], 15000, {
        SPIDERMESH_NAMESPACE: `tcp-session-recovery-${Date.now()}`,
        SIMPLE_DISCOVERY_PORT: String(25000 + Math.floor(Math.random() * 1000)),
        SPIDERMESH_HTTP2_RECONNECT_ATTEMPTS: '2',
        SPIDERMESH_HTTP2_RECONNECT_DELAY_MS: '100',
        SPIDERMESH_HTTP2_CONNECT_TIMEOUT_MS: '500',
    })

    expect(result.code).toBe(0)
    expect(result.stderr).toBe('')
    expect(result.stdout).toContain('TCP session recovery test passed')
    expect(result.stdout).toContain('"topologyEvictedStalePeer":true')
    expect(result.stdout).toContain('"rpcRecovered":true')
    expect(result.stdout).toContain('"nodeIdChanged":true')
    expect(result.stdout).toContain('"recoveredFromOneShotDiscovery":true')
})
