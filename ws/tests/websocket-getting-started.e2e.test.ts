import { expect, test } from 'bun:test'
import { runBunScript } from './helpers/runBunScript.js'

// Spawns the ws getting-started relay + provider + client as real child processes.
test('ws getting-started: relay + provider + client across real processes', async () => {
    const result = await runBunScript(['run', 'examples/getting-started-test.ts'], 30000)

    expect(result.code).toBe(0)
    expect(result.stdout).toContain('hello world')
})
