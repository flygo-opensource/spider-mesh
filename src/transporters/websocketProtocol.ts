import { Buffer } from 'node:buffer'
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
    payload: Buffer
}

export function encodeRelayFrame(header: RelayHeader, payload: Buffer = Buffer.alloc(0)) {
    const headerBytes = Buffer.from(encode(header))
    const envelope = Buffer.allocUnsafe(4 + headerBytes.length + payload.length)
    envelope.writeUInt32BE(headerBytes.length, 0)
    headerBytes.copy(envelope, 4)
    payload.copy(envelope, 4 + headerBytes.length)
    return envelope
}

export function decodeRelayFrame(raw: Buffer) {
    if (raw.length < 4) return null

    const headerLength = raw.readUInt32BE(0)
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

export function normalizeRelayRawData(raw: WebSocket.RawData) {
    if (Buffer.isBuffer(raw)) return raw
    if (Array.isArray(raw)) return Buffer.concat(raw)
    return Buffer.from(raw)
}