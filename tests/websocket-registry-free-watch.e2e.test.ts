import { expect, test } from 'bun:test'
import { runBunScript } from './helpers/runBunScript.js'

// With the relay and NO Registry, greeter.watch() must be a live availability stream:
// it emits when a provider appears and again when it disappears. Proven with real child
// processes (relay + observer + provider that is later killed).
test('ws registry-free: watch() streams provider appear and disappear', async () => {
    const result = await runBunScript(['run', 'examples/registry-free-watch-test.ts'], 35000)

    expect(result.code).toBe(0)
    expect(result.stdout).toContain('watch saw provider appear')
    expect(result.stdout).toContain('watch saw provider disappear')
})
