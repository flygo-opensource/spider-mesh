import { expect, setDefaultTimeout, test } from 'bun:test'
import { runBunScript } from './helpers/runBunScript.js'

setDefaultTimeout(30000)

test('websocket spidermesh round-robin e2e', async () => {
    const result = await runBunScript(['run', 'examples/websocket-e2e-round-robin-test.ts'], 25000)

    expect(result.code).toBe(0)
    expect(result.stderr).toBe('')

    const jsonLine = result.stdout.split('\n').find(line => line.includes('"results":['))
    expect(jsonLine).toBeDefined()

    const payload = JSON.parse(jsonLine!) as { results: string[] }
    const providers = payload.results.map(result => result.split(' from ')[1])

    expect(payload.results).toHaveLength(4)
    expect(new Set(providers).size).toBe(2)
    expect(providers[0]).toContain('provider-')
    expect(providers[1]).toContain('provider-')
    expect(providers[0]).not.toBe(providers[1])
    expect(providers[0]).toBe(providers[2])
    expect(providers[1]).toBe(providers[3])
})