import { createWsTestEnv, randomPort, start, stopAll, waitForOutput } from './helpers/e2eHarness.js'

async function main() {
    const port = randomPort(9700)
    const env = createWsTestEnv(port)

    const relay = start('relay', 'examples/websocket-server.ts', env)
    await waitForOutput(relay, /listening on/, 5000, 'relay')

    const provider = start('provider', 'examples/websocket-stream-provider.ts', env)
    await waitForOutput(provider, /stream provider ready/, 5000, 'provider')

    const caller = start('caller', 'examples/websocket-stream-leaked-client.ts', env)
    await waitForOutput(caller, /"streamStarted":true/, 15000, 'caller')

    // Caller chết mà không kịp gửi `cancel`: relay là nơi duy nhất biết cặp caller/provider,
    // nên nó phải thay caller đóng stream ở provider.
    caller.kill('SIGKILL')

    await new Promise(resolve => setTimeout(resolve, 2000))

    const checker = start('checker', 'examples/websocket-stream-checker-client.ts', env)
    const output = await waitForOutput(checker, /"activeStreamsChecked":true/, 20000, 'checker')
    console.log(output.split('\n').find(line => line.includes('"activeStreamsChecked"'))?.trim())
}

try {
    await main()
} finally {
    await stopAll()
}
