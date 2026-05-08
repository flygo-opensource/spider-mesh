import { decode, encode } from '@msgpack/msgpack'
import type { RpcCancelPacket, RpcRequestPacket, RpcResponsePacket, SpiderMeshNode } from '@spider-mesh/core'

export type RelayRawData = Uint8Array | ArrayBuffer | ArrayBufferView | string | RelayRawData[]

export type RelayRpcFrame = {
    type: 'rpc'
    data: RpcRequestPacket | RpcResponsePacket | RpcCancelPacket
}

export type RelayPublishFrame = {
    type: 'publish'
    topic: string
    payload: Uint8Array
}

export type RelaySubscribeFrame = {
    type: 'subscribe'
    topics: string[]
}

export type RelayUnsubscribeFrame = {
    type: 'unsubscribe'
    topics: string[]
}

export type RelayHelloFrame = {
    type: 'hello'
    me: SpiderMeshNode
    target_id?: string
}

export type RelayOfflineFrame = {
    type: 'offline'
    node_id: string
}

export type RelayFrame = RelayRpcFrame | RelayPublishFrame | RelaySubscribeFrame | RelayUnsubscribeFrame | RelayHelloFrame | RelayOfflineFrame


export function encodeRelayFrame(frame: RelayFrame) {
    return encode(frame)
}

export function decodeRelayFrame(raw: Uint8Array) {
    try {
        return decode(raw) as RelayFrame
    } catch {
        return null
    }
}

export function normalizeRelayRawData(raw: RelayRawData): Uint8Array {
    if (raw instanceof Uint8Array) return raw
    if (Array.isArray(raw)) return concatUint8Arrays(raw.map(chunk => normalizeRelayRawData(chunk)))
    if (raw instanceof ArrayBuffer) return new Uint8Array(raw)
    if (typeof raw === 'string') return new TextEncoder().encode(raw)
    return asUint8Array(raw)
}

function concatUint8Arrays(chunks: Uint8Array[]) {
    const size = chunks.reduce((total, chunk) => total + chunk.length, 0)
    const merged = new Uint8Array(size)
    let offset = 0

    for (const chunk of chunks) {
        merged.set(chunk, offset)
        offset += chunk.length
    }

    return merged
}

function asUint8Array(raw: ArrayBufferLike | ArrayBufferView) {
    if (raw instanceof Uint8Array) return raw
    if (ArrayBuffer.isView(raw)) return new Uint8Array(raw.buffer, raw.byteOffset, raw.byteLength)
    return new Uint8Array(raw)
}