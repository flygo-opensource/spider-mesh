/**
 * E2E orchestrator (ws): proves that with the relay and NO Registry, `watch()` is a live
 * availability stream — it emits when a provider APPEARS and again when it DISAPPEARS.
 *
 *   relay up → observer subscribes watch() → provider appears (WATCH_PRESENT)
 *            → provider killed (WATCH_GONE)
 */
import { createWsTestEnv, randomPort, start, waitForOutput, stopAll } from './helpers/e2eHarness.js'

async function main() {
    const env = createWsTestEnv(randomPort(8900))

    const relay = start('relay', 'examples/getting-started/relay.ts', env)
    await waitForOutput(relay, /relay listening/, 8000, 'relay')

    const observer = start('observer', 'examples/registry-free-watch-observer.ts', env)
    await waitForOutput(observer, /WATCH_READY/, 8000, 'observer')

    // Provider appears → watch() must emit a non-empty list.
    const provider = start('provider', 'examples/getting-started/provider.ts', env)
    await waitForOutput(observer, /WATCH_PRESENT/, 10000, 'observer')
    console.log('watch saw provider appear')

    // Provider disappears → watch() must emit empty.
    provider.kill('SIGTERM')
    await waitForOutput(observer, /WATCH_GONE/, 12000, 'observer')
    console.log('watch saw provider disappear')
}

try {
    await main()
} finally {
    await stopAll()
}
