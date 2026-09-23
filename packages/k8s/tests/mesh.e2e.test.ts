import { afterEach, expect, test } from 'bun:test'
import { spawn, type ChildProcess } from 'node:child_process'
import { FakeKubernetesApi, freePort, until } from './helpers/fakes.js'

const children: ChildProcess[] = []
const cleanups: (() => void)[] = []
afterEach(() => {
    for (const child of children.splice(0)) child.kill('SIGKILL')
    while (cleanups.length) cleanups.pop()!()
})

function start(role: 'provider' | 'client', env: Record<string, string>) {
    const child = spawn('bun', ['run', 'tests/fixtures/mesh-process.ts'], {
        cwd: new URL('..', import.meta.url),
        env: { ...process.env, ...env, ROLE: role },
        stdio: ['ignore', 'pipe', 'pipe'],
    })
    children.push(child)
    const output = { stdout: '', stderr: '' }
    child.stdout!.on('data', chunk => { output.stdout += chunk })
    child.stderr!.on('data', chunk => { output.stderr += chunk })
    return output
}

test('RPC between two processes discovered through EndpointSlices; not-ready pod leaves at once', async () => {
    const k8s = new FakeKubernetesApi()
    cleanups.push(() => k8s.close())
    const providerPort = await freePort()
    const clientPort = await freePort()
    k8s.setSlice('mesh-provider', [{ address: '127.0.0.1' }], providerPort)
    k8s.setSlice('mesh-client', [{ address: '127.0.0.1' }], clientPort)

    const provider = start('provider', { K8S_API: k8s.url, DISCOVERY_PORT: String(providerPort) })
    await until(() => provider.stdout.includes('ready'), 10_000, `provider ready\n${provider.stderr}`)

    const client = start('client', { K8S_API: k8s.url, DISCOVERY_PORT: String(clientPort) })
    await until(() => client.stdout.includes('hello k8s'), 10_000, `RPC result\n${client.stdout}\n${client.stderr}`)

    k8s.setSlice('mesh-provider', [{ address: '127.0.0.1', ready: false }], providerPort)
    await until(() => client.stdout.includes('removed'), 3000, `removal\n${client.stdout}\n${client.stderr}`)
}, 30_000)
