import { SpiderMesh, Topology } from '@spider-mesh/core'
import { WebsocketTransporter } from '../../src/node.js'

export type CreateMeshOptions = {
    wsUrl?: string
    heartbeatIntervalMs?: number
    reconnectIntervalMs?: number
    unsubscribeDelayMs?: number
    /** Bật khi ví dụ cần enumerate/watch danh sách node thay vì chỉ probe reachability. */
    topology?: boolean
}

export function createMesh(options: CreateMeshOptions = {}) {
    const transporter = new WebsocketTransporter({
        heartbeatIntervalMs: options.heartbeatIntervalMs,
        reconnectIntervalMs: options.reconnectIntervalMs,
        unsubscribeDelayMs: options.unsubscribeDelayMs,
    })

    transporter.connect(options.wsUrl || 'ws://127.0.0.1:8787')

    const topology = options.topology ? new Topology({ discovery: transporter }) : undefined
    const mesh = new SpiderMesh({
        topology,
        transporters: [transporter],
    })

    return {
        mesh,
        transporter,
        topology,
    }
}
