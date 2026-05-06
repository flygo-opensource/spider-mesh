import { createInterface } from 'node:readline'
import { firstValueFrom, Subject } from 'rxjs'
import { Registry } from '../../src/Registry.js'
import { SpiderMesh } from '../../src/SpiderMesh.js'
import { LOCAL_SERVICES$ } from '../../src/decorators/Microservice.js'
import type {
    DiscoveryEvent,
    DiscoveryTransporter,
    MdnsMessage,
    NodeMetadata,
    RpcEvent,
    RpcPacket,
    RpcTransporter,
} from '../../src/types.js'

type HostMessage =
    | { kind: 'ready'; role: string; node_id: string }
    | { kind: 'discovery-broadcast'; role: string; node: any }
    | { kind: 'rpc-send'; role: string; packet: RpcPacket; node_id?: string }
    | { kind: 'result'; role: string; value?: unknown; error?: string; code?: string }

type ChildCommand =
    | { kind: 'discovery-deliver'; node: any }
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
    metadata = { mock: true }

    async send(packet: RpcPacket, node_id?: string) {
        sendHostMessage({
            kind: 'rpc-send',
            role,
            packet,
            node_id,
        })
    }
}

class ProcessDiscoveryTransporter extends Subject<DiscoveryEvent> implements DiscoveryTransporter {
    async broadcast(data: MdnsMessage<NodeMetadata>) {
        sendHostMessage({
            kind: 'discovery-broadcast',
            role,
            node: data.node,
        })
    }
}

const registry = role === 'client' ? new Registry() : undefined
const mesh = new SpiderMesh(registry)
const rpcTransporter = new ProcessRpcTransporter()
const discoveryTransporter = new ProcessDiscoveryTransporter()

mesh.registerTransporter(rpcTransporter, 'ProcessRpcTransporter')
mesh.registerTransporter(discoveryTransporter, 'ProcessDiscoveryTransporter')

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

    if (command.kind === 'discovery-deliver') {
        discoveryTransporter.next({ discovered: command.node })
        return
    }

    if (command.kind === 'rpc-deliver') {
        rpcTransporter.next({
            rpc: {
                node_id: command.packet.source_node_id,
                packet: command.packet,
            },
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