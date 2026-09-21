import { createWsTestEnv, randomPort, start, stopAll, waitForOutput } from './helpers/e2eHarness.js'

// `stream`: RPC trả Observable dài hạn. `unary`: RPC kiểu Promise không bao giờ trả lời.
const mode = process.argv[2] === 'unary' ? 'unary' : 'stream'

const scenario = mode === 'stream'
    ? {
        provider: 'examples/websocket-stream-provider.ts',
        providerReady: /stream provider ready/,
        client: 'examples/websocket-stream-offline-client.ts',
        inFlight: /"streamStarted":true/,
        terminal: /"streamErrored":true/,
        terminalKey: '"streamErrored"',
    }
    : {
        provider: 'examples/websocket-timeout-provider.ts',
        providerReady: /provider ready/,
        client: 'examples/websocket-relay-loss-unary-client.ts',
        inFlight: /"unaryCalling":true/,
        terminal: /"unaryErrored":true/,
        terminalKey: '"unaryErrored"',
    }

async function main() {
    const port = randomPort(9300)
    const env = createWsTestEnv(port)

    const relay = start('relay', 'examples/websocket-server.ts', env)
    await waitForOutput(relay, /listening on/, 5000, 'relay')

    const provider = start('provider', scenario.provider, env)
    await waitForOutput(provider, scenario.providerReady, 5000, 'provider')

    const client = start('client', scenario.client, env)
    await waitForOutput(client, scenario.inFlight, 15000, 'client')
    // Để request unary kịp rời caller và tới provider trước khi relay chết.
    await new Promise(resolve => setTimeout(resolve, 300))

    // Relay chết: nó không thể tự báo MICROSERVICE_OFFLINE, nên chính transporter phía caller
    // phải kết thúc request đang bay khi socket tới relay đóng.
    const killedAt = Date.now()
    relay.kill('SIGKILL')

    const output = await waitForOutput(client, scenario.terminal, 10000, 'client')
    const line = output.split('\n').find(l => l.includes(scenario.terminalKey))!
    console.log(JSON.stringify({ relayLoss: mode, detectedAfterMs: Date.now() - killedAt, ...JSON.parse(line) }))
}

try {
    await main()
} finally {
    await stopAll()
}
