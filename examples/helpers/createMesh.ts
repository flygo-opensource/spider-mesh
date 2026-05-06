import { Registry, SpiderMesh } from '@spider-mesh/core'
import { WebsocketTransporter } from '../../src/node.js'

export type CreateMeshOptions = {
    wsUrl?: string
    heartbeatIntervalMs?: number
    reconnectIntervalMs?: number
    unsubscribeDelayMs?: number
}

export function createMesh(options: CreateMeshOptions = {}) {
    const transporter = new WebsocketTransporter({
        heartbeatIntervalMs: options.heartbeatIntervalMs,
        reconnectIntervalMs: options.reconnectIntervalMs,
        unsubscribeDelayMs: options.unsubscribeDelayMs,
    })

    transporter.connect(options.wsUrl || 'ws://127.0.0.1:8787')

    const registry = new Registry()
    const mesh = new SpiderMesh(registry)
    mesh.registerTransporter(transporter)

    return {
        mesh,
        registry,
        transporter,
    }
}