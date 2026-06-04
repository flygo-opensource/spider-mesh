/**
 * E2E orchestrator (tcp): the CLIENT starts BEFORE any provider exists.
 *
 * With a registry-free core, availability comes from the transporter's ServiceDirectory,
 * so the client's `wait(() => mesh.listRpcNodes(...) > 0)` must genuinely BLOCK until a
 * provider is discovered over multicast — it must not fake-resolve. We start the provider
 * only after a delay; the client must still succeed (prints "hello world"). If the client
 * exits early, `waitForOutput` rejects and this orchestrator fails.
 */
import { createTcpTestEnv, start, waitForOutput, stopAll } from './helpers/e2eHarness.js'

async function main() {
    const env = createTcpTestEnv()

    // Client first — no provider yet. Attach the matcher immediately so an early exit fails.
    const client = start('client', 'examples/getting-started/client.ts', env)
    const result = waitForOutput(client, /hello world/, 25000, 'client')

    // Give the client time to (wrongly) resolve before any provider exists.
    await new Promise(resolve => setTimeout(resolve, 3000))

    // Now bring the provider online — discovery should unblock the client.
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
