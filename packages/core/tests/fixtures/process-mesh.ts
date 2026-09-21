import { createInterface } from 'node:readline'
import { firstValueFrom, Subject } from 'rxjs'
import { SpiderMesh } from '../../src/SpiderMesh.js'
import { Topology } from '../../src/Topology.js'
import { LOCAL_SERVICES$ } from '../../src/decorators/Microservice.js'
import type {
    RpcCancelPacket,
    RpcEvent,
    RpcRequestPacket,
    RpcResponsePacket,
    RpcTransporter,
    SpiderMeshNode,
} from '../../src/types.js'

type RpcPacket = RpcRequestPacket | RpcResponsePacket | RpcCancelPacket

type HostMessage =
    | { kind: 'ready'; role: string; node_id: string }
    | { kind: 'node-broadcast'; role: string; node: SpiderMeshNode }
    | { kind: 'rpc-send'; role: string; packet: RpcPacket; node_id?: string }
    | { kind: 'result'; role: string; value?: unknown; error?: string; code?: string }

type ChildCommand =
    | { kind: 'node-deliver'; node: SpiderMeshNode }
    | { kind: 'rpc-deliver'; packet: RpcPacket }
    | { kind: 'call'; value: string }

const role = process.argv[2]
const serviceName = process.argv[3]

if (!role || !serviceName) {
    throw new Error('Usage: bun run tests/fixtures/process-mesh.ts <role> <serviceName>')
}

const sendHostMessage = (message: HostMessage) => {
    process.stdout.write(`${JSON.stringify(message)}\n`)
}

class ProcessRpcTransporter extends Subject<RpcEvent> implements RpcTransporter {
    public readonly name = 'process'
    metadata = { mock: true }

    async send(packet: RpcPacket, node_id?: string) {
        sendHostMessage({
            kind: 'rpc-send',
            role,
            packet,
            node_id,
        })
        return { cancel: () => {} }
    }

    // Delegates all routing to the host process bridge, so it is always a candidate.
    canRoute() {
        return true
    }
}

const topology = new Topology()
const mesh = new SpiderMesh({ topology })
const rpcTransporter = new ProcessRpcTransporter()

mesh.registerTransporter(rpcTransporter)
mesh.localNode$.subscribe(node => {
    sendHostMessage({ kind: 'node-broadcast', role, node })
})

if (role === 'provider') {
    LOCAL_SERVICES$.next({
        name: serviceName,
        metadata: {},
        instance: {
            echo(value: string) {
                return `provider:${value}`
            },
        },
    })
}

sendHostMessage({
    kind: 'ready',
    role,
    node_id: mesh.node_id,
})

const rl = createInterface({ input: process.stdin })
rl.on('line', async line => {
    if (!line.trim()) return
    const command = JSON.parse(line) as ChildCommand

    if (command.kind === 'node-deliver') {
        topology.upsertRemote(command.node)
        return
    }

    if (command.kind === 'rpc-deliver') {
        rpcTransporter.next({
            rpc: command.packet,
        })
        return
    }

    if (command.kind === 'call') {
        try {
            const value = await firstValueFrom(mesh.callRemoteService<string, never>({
                service: serviceName,
                method: 'echo',
                args: [command.value],
                timeout: 1500,
            }))

            sendHostMessage({ kind: 'result', role, value })
        } catch (error) {
            sendHostMessage({
                kind: 'result',
                role,
                error: error instanceof Error ? error.message : String(error),
                code: typeof error === 'object' && error && 'code' in error ? String((error as { code?: string }).code) : undefined,
            })
        }
    }
})
