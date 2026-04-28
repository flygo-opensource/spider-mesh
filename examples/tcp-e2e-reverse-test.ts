import { createTcpTestEnv, start, stopAll, waitForOutput } from './helpers/e2eHarness.js'

async function main() {
    const env = createTcpTestEnv()

    const server = start('server', 'examples/tcp-e2e-reverse-server.ts', env)
    await waitForOutput(server, /TCP reverse e2e server connected/, 5000, 'server')

    const client = start('client', 'examples/tcp-e2e-reverse-client.ts', env)
    await waitForOutput(client, /TCP reverse e2e client connected/, 5000, 'client')

    const output = await waitForOutput(server, /hello from server from client/, 15000, 'server')
    console.log(output.trim())
}

try {
    await main()
} finally {
    await stopAll()
}