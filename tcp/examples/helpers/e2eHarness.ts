import { spawn } from 'node:child_process'

const cwd = new URL('../..', import.meta.url)

type StartedChild = ReturnType<typeof spawn>

type StartedProcess = {
    child: StartedChild
    name: string
}

const started: StartedProcess[] = []
const outputByChild = new WeakMap<StartedChild, { stdout: string; stderr: string }>()

export function createTcpTestEnv() {
    const suffix = `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`
    return {
        SPIDERMESH_NAMESPACE: `tcp-e2e-${suffix}`,
        SPIDERMESH_NODE_HOSTNAME: '127.0.0.1',
        // Keep bounded reconnect tests fast. UDP discovery has no heartbeat/TTL.
        SPIDERMESH_HTTP2_RECONNECT_ATTEMPTS: '3',
        SPIDERMESH_HTTP2_RECONNECT_DELAY_MS: '100',
        SPIDERMESH_HTTP2_CONNECT_TIMEOUT_MS: '1000',
    }
}

export function start(name: string, file: string, env: Record<string, string>) {
    const child = spawn('bun', ['run', file], {
        cwd,
        env: {
            ...process.env,
            ...env,
        },
        stdio: ['ignore', 'pipe', 'pipe'],
    })

    const output = { stdout: '', stderr: '' }
    outputByChild.set(child, output)
    child.stdout?.on('data', chunk => { output.stdout += chunk.toString() })
    child.stderr?.on('data', chunk => {
        output.stderr += chunk.toString()
        if (process.env.SPIDERMESH_TCP_DEBUG) process.stderr.write(`[${name}] ${chunk.toString()}`)
    })
    started.push({ child, name })
    return child
}

export function waitForOutput(child: StartedChild, matcher: RegExp, timeoutMs: number, name: string) {
    return new Promise<string>((resolve, reject) => {
        const stdoutStream = child.stdout
        const stderrStream = child.stderr
        if (!stdoutStream || !stderrStream) {
            reject(new Error(`${name} is missing piped stdout/stderr`))
            return
        }

        const output = outputByChild.get(child) || { stdout: '', stderr: '' }

        if (matcher.test(output.stdout)) {
            resolve(output.stdout)
            return
        }
        if (child.exitCode !== null) {
            reject(new Error(`${name} exited before expected output, code=${child.exitCode}\nstdout:\n${output.stdout}\nstderr:\n${output.stderr}`))
            return
        }

        const cleanup = () => {
            clearTimeout(timeout)
            stdoutStream.off('data', onStdout)
            stderrStream.off('data', onStderr)
            child.off('exit', onExit)
        }

        const timeout = setTimeout(() => {
            cleanup()
            reject(new Error(`${name} timed out after ${timeoutMs}ms\nstdout:\n${output.stdout}\nstderr:\n${output.stderr}`))
        }, timeoutMs)

        const onStdout = (chunk: Buffer | string) => {
            if (matcher.test(output.stdout)) {
                cleanup()
                resolve(output.stdout)
            }
        }

        const onStderr = () => undefined

        const onExit = (code: number | null) => {
            cleanup()
            reject(new Error(`${name} exited before expected output, code=${code}\nstdout:\n${output.stdout}\nstderr:\n${output.stderr}`))
        }

        stdoutStream.on('data', onStdout)
        stderrStream.on('data', onStderr)
        child.on('exit', onExit)
    })
}

export async function stopAll() {
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
