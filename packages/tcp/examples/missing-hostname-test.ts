/**
 * E2E: provider và client KHÔNG đặt SPIDERMESH_NODE_HOSTNAME. Node công bố host rỗng;
 * TopologyDiscoveryAdapter phải lấy địa chỉ nguồn của gói UDP để client gọi được.
 * Exits 0 on success.
 */
import { createTcpTestEnv, start, waitForOutput, stopAll } from './helpers/e2eHarness.js'

async function main() {
    const env = { ...createTcpTestEnv(), SPIDERMESH_NODE_HOSTNAME: '' }

    const providers = [1, 2].map(index => start(`provider-${index}`, 'examples/getting-started/provider.ts', env))
    for (const [index, provider] of providers.entries()) {
        await waitForOutput(provider, /provider online/, 8000, `provider-${index + 1}`)
    }

    const client = start('client', 'examples/getting-started/client.ts', env)
    const out = await waitForOutput(client, /hello world/, 15000, 'client')
    console.log(out.trim())
}

try {
    await main()
} finally {
    await stopAll()
}
