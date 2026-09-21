import { spawn } from 'node:child_process'

export type ScriptRunResult = {
    code: number | null
    stdout: string
    stderr: string
}

export async function runBunScript(args: string[], timeoutMs = 15000, env: Record<string, string> = {}) {
    return await new Promise<ScriptRunResult>((resolve, reject) => {
        const child = spawn('bun', args, {
            cwd: new URL('../..', import.meta.url),
            env: {
                ...process.env,
                ...env,
            },
            stdio: ['ignore', 'pipe', 'pipe'],
        })

        const stdout = child.stdout
        const stderr = child.stderr
        if (!stdout || !stderr) {
            reject(new Error('Missing piped stdout/stderr from bun child process'))
            return
        }

        let stdoutText = ''
        let stderrText = ''

        const cleanup = () => {
            clearTimeout(timer)
            stdout.off('data', onStdout)
            stderr.off('data', onStderr)
            child.off('exit', onExit)
        }

        const onStdout = (chunk: Buffer | string) => {
            stdoutText += chunk.toString()
        }

        const onStderr = (chunk: Buffer | string) => {
            stderrText += chunk.toString()
        }

        const onExit = (code: number | null) => {
            cleanup()
            resolve({ code, stdout: stdoutText, stderr: stderrText })
        }

        const timer = setTimeout(() => {
            cleanup()
            child.kill('SIGTERM')
            reject(new Error(`bun ${args.join(' ')} timed out after ${timeoutMs}ms\nstdout:\n${stdoutText}\nstderr:\n${stderrText}`))
        }, timeoutMs)

        stdout.on('data', onStdout)
        stderr.on('data', onStderr)
        child.on('exit', onExit)
    })
}