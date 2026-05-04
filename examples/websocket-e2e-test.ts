import { spawn } from 'node:child_process'

const cwd = new URL('..', import.meta.url)
const port = 9000 + Math.floor(Math.random() * 200)
const wsUrl = `ws://127.0.0.1:${port}`
type StartedChild = ReturnType<typeof spawn>

type StartedProcess = {
    child: StartedChild
    name: string
}

const started: StartedProcess[] = []

function start(name: string, file: string, env: Record<string, string>) {
    const child = spawn('bun', ['run', file], {
        cwd,
        env: {
            ...process.env,
            ...env,
        },
        stdio: ['ignore', 'pipe', 'pipe'],
    })

    started.push({ child, name })
    return child
}

function waitForOutput(child: StartedChild, matcher: RegExp, timeoutMs: number, name: string) {
    return new Promise<string>((resolve, reject) => {
        const stdoutStream = child.stdout
        const stderrStream = child.stderr
        if (!stdoutStream || !stderrStream) {
            reject(new Error(`${name} is missing piped stdout/stderr`))
            return
        }

        let stdout = ''
        let stderr = ''

        const cleanup = () => {
            clearTimeout(timeout)
            stdoutStream.off('data', onStdout)
            stderrStream.off('data', onStderr)
            child.off('exit', onExit)
        }

        const timeout = setTimeout(() => {
            cleanup()
            reject(new Error(`${name} timed out after ${timeoutMs}ms\nstdout:\n${stdout}\nstderr:\n${stderr}`))
        }, timeoutMs)

        const onStdout = (chunk: Buffer | string) => {
            stdout += chunk.toString()
            if (matcher.test(stdout)) {
                cleanup()
                resolve(stdout)
            }
        }

        const onStderr = (chunk: Buffer | string) => {
            stderr += chunk.toString()
        }

        const onExit = (code: number | null) => {
            cleanup()
            reject(new Error(`${name} exited before expected output, code=${code}\nstdout:\n${stdout}\nstderr:\n${stderr}`))
        }

        stdoutStream.on('data', onStdout)
        stderrStream.on('data', onStderr)
        child.on('exit', onExit)
    })
}

async function stopAll() {
    const pending = [...started]
    started.length = 0

    await Promise.allSettled(pending.map(({ child }) => new Promise<void>(resolve => {
        if (child.exitCode !== null || child.killed) {
            resolve()
            return
        }

        child.once('exit', () => resolve())
        child.kill('SIGTERM')
    })))
}

async function main() {
    const server = start('server', 'examples/websocket-server.ts', { WS_PORT: `${port}` })
    await waitForOutput(server, /listening on/, 5000, 'server')

    const client = start('client', 'examples/websocket-e2e-client.ts', { WS_URL: wsUrl })
    await waitForOutput(client, /client connected/, 5000, 'client')
    const provider = start('provider', 'examples/websocket-e2e-provider.ts', { WS_URL: wsUrl })
    await waitForOutput(provider, /provider ready/, 5000, 'provider')
    const output = await waitForOutput(client, /hello websocket e2e from provider/, 12000, 'client')

    console.log(output.trim())
}

try {
    await main()
} finally {
    await stopAll()
}