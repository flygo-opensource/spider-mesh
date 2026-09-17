import { afterEach, describe, expect, test } from 'bun:test'
import { spawn, type ChildProcessWithoutNullStreams } from 'node:child_process'
import { EventEmitter, once } from 'node:events'
import { createInterface } from 'node:readline'

type ChildReadyMessage = { kind: 'ready'; role: string; node_id: string }
type ChildNodeMessage = { kind: 'node-broadcast'; role: string; node: any }
type ChildRpcMessage = { kind: 'rpc-send'; role: string; packet: any; node_id?: string }
type ChildResultMessage = { kind: 'result'; role: string; value?: unknown; error?: string; code?: string }
type ChildMessage = ChildReadyMessage | ChildNodeMessage | ChildRpcMessage | ChildResultMessage

class MeshProcess {
    process: ChildProcessWithoutNullStreams
    events = new EventEmitter()
    messages: ChildMessage[] = []
    stderr = ''

    constructor(role: 'provider' | 'client', serviceName: string) {
        this.process = spawn('bun', ['run', 'tests/fixtures/process-mesh.ts', role, serviceName], {
            cwd: process.cwd(),
            stdio: ['pipe', 'pipe', 'pipe'],
        })

        createInterface({ input: this.process.stdout }).on('line', line => {
            if (!line.trim()) return
            const message = JSON.parse(line) as ChildMessage
            this.messages.push(message)
            this.events.emit(message.kind, message)
            this.events.emit('message', message)
        })

        createInterface({ input: this.process.stderr }).on('line', line => {
            this.stderr += `${line}\n`
        })
    }

    send(message: unknown) {
        this.process.stdin.write(`${JSON.stringify(message)}\n`)
    }

    async waitFor<T extends ChildMessage['kind']>(kind: T, timeoutMs = 4000): Promise<Extract<ChildMessage, { kind: T }>> {
        const existing = this.messages.find(message => message.kind === kind)
        if (existing) return existing as Extract<ChildMessage, { kind: T }>

        const timer = setTimeout(() => {
            this.events.emit(`timeout:${kind}`)
        }, timeoutMs)

        try {
            const result = await Promise.race([
                once(this.events, kind).then(([message]) => message as Extract<ChildMessage, { kind: T }>),
                once(this.events, `timeout:${kind}`).then(() => {
                    throw new Error(`Timed out waiting for ${kind}. stderr:\n${this.stderr}`)
                }),
            ])
            return result
        } finally {
            clearTimeout(timer)
        }
    }

    latest<T extends ChildMessage['kind']>(kind: T) {
        return [...this.messages].reverse().find(message => message.kind === kind) as Extract<ChildMessage, { kind: T }> | undefined
    }

    kill() {
        this.process.kill()
    }
}

const children: MeshProcess[] = []

afterEach(() => {
    for (const child of children.splice(0)) {
        child.kill()
    }
})

describe('process e2e', () => {
    test('routes rpc between isolated processes through local node snapshots and Topology', async () => {
        const serviceName = `ProcessEcho${Date.now().toString(36)}`
        const client = new MeshProcess('client', serviceName)
        const provider = new MeshProcess('provider', serviceName)
        children.push(client, provider)

        const routeNode = (message: ChildNodeMessage) => {
            const target = message.role === 'provider' ? client : provider
            target.send({ kind: 'node-deliver', node: message.node })
        }

        const routeRpc = (message: ChildRpcMessage) => {
            const target = message.role === 'provider' ? client : provider
            target.send({ kind: 'rpc-deliver', packet: message.packet })
        }

        client.events.on('node-broadcast', routeNode)
        provider.events.on('node-broadcast', routeNode)
        client.events.on('rpc-send', routeRpc)
        provider.events.on('rpc-send', routeRpc)

        await client.waitFor('ready')
        await provider.waitFor('ready')

        const providerNode = provider.latest('node-broadcast')
        if (providerNode) {
            client.send({ kind: 'node-deliver', node: providerNode.node })
        }

        client.send({ kind: 'call', value: 'cross-process' })

        const result = await client.waitFor('result')

        expect(result.error).toBeUndefined()
        expect(result.value).toBe('provider:cross-process')
    })
})
