import { decode, encode } from '@msgpack/msgpack'
import type WebSocket from 'ws'
import type { RpcPacket } from '../types.js'

export type RpcRelayHeader = {
    type: RpcPacket['kind']
    node_id: string
    sender_id?: string
}

export type RelayHeader =
    | { type: 'register', node_id: string }
    | RpcRelayHeader
    | { type: 'discovery', sender_id?: string }
    | { type: 'pubsub', sender_id?: string, topic: string }
    | { type: 'offline', sender_id: string }

export type RelayFrame = {
    header: RelayHeader
    payload: Uint8Array
}

export function encodeRelayFrame(header: RelayHeader, payload: Uint8Array = new Uint8Array(0)) {
    const headerBytes = encode(header)
    const envelope = new Uint8Array(4 + headerBytes.length + payload.length)
    new DataView(envelope.buffer, envelope.byteOffset, envelope.byteLength).setUint32(0, headerBytes.length, false)
    envelope.set(headerBytes, 4)
    envelope.set(payload, 4 + headerBytes.length)
    return envelope
}

export function decodeRelayFrame(raw: Uint8Array) {
    if (raw.length < 4) return null

    const headerLength = new DataView(raw.buffer, raw.byteOffset, raw.byteLength).getUint32(0, false)
    const headerEnd = 4 + headerLength
    if (headerLength < 0 || raw.length < headerEnd) return null

    try {
        const header = decode(raw.subarray(4, headerEnd)) as RelayHeader
        const payload = raw.subarray(headerEnd)
        return { header, payload } satisfies RelayFrame
    } catch {
        return null
    }
}

export function normalizeRelayRawData(raw: WebSocket.RawData): Uint8Array {
    if (raw instanceof Uint8Array) return raw
    if (Array.isArray(raw)) return concatUint8Arrays(raw.map(chunk => normalizeRelayRawData(chunk)))
    if (raw instanceof ArrayBuffer) return new Uint8Array(raw)
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