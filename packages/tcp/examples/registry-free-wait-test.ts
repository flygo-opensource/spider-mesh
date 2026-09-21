/**
 * E2E orchestrator (tcp): the CLIENT starts BEFORE any provider exists.
 *
 * Topology nhận node từ generic UDP Discovery; availability còn lọc HTTP/2 endpoint đã route
 * được. Vì vậy wait() phải thực sự block cho tới khi provider xuất hiện, không fake resolve.
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
