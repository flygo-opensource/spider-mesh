/**
 * E2E orchestrator (ws): the CLIENT starts BEFORE any provider exists.
 *
 * This is the regression guard for the registry-free availability fix. With a registry-free
 * core, `wait()` must reflect REAL relay state — it must not fake-resolve on subscribe. If
 * `wait()` resolved prematurely, the client would call `hello()` with no provider, the relay
 * would answer MICROSERVICE_OFFLINE, and the client would exit early (no retry/fallback in
 * the getting-started client). We attach the success matcher immediately, then start the
 * provider only after a delay; the client must block and then succeed ("hello world").
 */
import { createWsTestEnv, randomPort, start, waitForOutput, stopAll } from './helpers/e2eHarness.js'

async function main() {
    const env = createWsTestEnv(randomPort(8900))

    const relay = start('relay', 'examples/getting-started/relay.ts', env)
    await waitForOutput(relay, /relay listening/, 8000, 'relay')

    // Client first — no provider yet. An early exit (fake-ready bug) makes this reject.
    const client = start('client', 'examples/getting-started/client.ts', env)
    const result = waitForOutput(client, /hello world/, 25000, 'client')

    // Give the client time to (wrongly) resolve before any provider exists.
    await new Promise(resolve => setTimeout(resolve, 3000))

    // Bring the provider online — only now should the client's wait() resolve.
    const provider = start('provider', 'examples/getting-started/provider.ts', env)
    await waitForOutput(provider, /provider online/, 8000, 'provider')

    const out = await result
    console.log(out.trim())
}

try {
    await main()
} finally {
    await stopAll()
}
