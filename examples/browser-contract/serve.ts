// Chạy bộ hợp đồng RPC (examples/contract) trong trình duyệt thật, với bản trình duyệt của transporter:
//
//   bun run examples/browser-contract/serve.ts
//
// Script khởi động relay + provider (Node), bundle client cho trình duyệt, rồi in URL cần mở.
// Trang đổi tiêu đề thành PASS/FAIL khi chạy xong. Ctrl+C để dừng tất cả.
import { spawn, type ChildProcess } from 'node:child_process'

const root = new URL('../..', import.meta.url).pathname
const relayPort = 41_000 + Math.floor(Math.random() * 3_000)
const httpPort = 44_000 + Math.floor(Math.random() * 3_000)
const children: ChildProcess[] = []

const run = (file: string, env: Record<string, string>) => {
    const child = spawn('bun', ['run', file], { cwd: root, env: { ...process.env, ...env }, stdio: 'inherit' })
    children.push(child)
}

const stop = () => {
    for (const child of children) child.kill('SIGTERM')
    process.exit(0)
}
process.on('SIGINT', stop)
process.on('SIGTERM', stop)

run('examples/websocket-server.ts', { WS_PORT: String(relayPort) })
await Bun.sleep(800)
run('examples/contract/provider.ts', { WS_URL: `ws://127.0.0.1:${relayPort}` })

const build = await Bun.build({ entrypoints: [`${import.meta.dir}/app.ts`], target: 'browser', format: 'esm' })
if (!build.success) {
    console.error(build.logs)
    stop()
}
const bundle = await build.outputs[0].text()
const page = `<!doctype html><html><head><meta charset="utf-8"><title>running</title></head>
<body><pre id="result">running…</pre><script type="module" src="/app.js"></script></body></html>`

Bun.serve({
    port: httpPort,
    hostname: '127.0.0.1',
    fetch(request) {
        if (new URL(request.url).pathname === '/app.js') {
            return new Response(bundle, { headers: { 'content-type': 'text/javascript' } })
        }
        return new Response(page, { headers: { 'content-type': 'text/html' } })
    },
})

console.log(`Open http://127.0.0.1:${httpPort}/?relay=${relayPort}`)
