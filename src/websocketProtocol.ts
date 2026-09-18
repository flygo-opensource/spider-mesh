import { pack, unpack } from 'msgpackr'
import type { RpcCancelPacket, RpcRequestPacket, RpcResponsePacket, SpiderMeshNode } from '@spider-mesh/core'

/** Các dạng raw payload có thể nhận từ WebSocket ở nhiều runtime. */
export type RelayRawData = Uint8Array | ArrayBuffer | ArrayBufferView | string | RelayRawData[]

/** Frame mang packet RPC qua relay. */
export type RelayRpcFrame = {
    type: 'rpc'
    data: RpcRequestPacket | RpcResponsePacket | RpcCancelPacket
}

/** Frame publish một event topic. */
export type RelayPublishFrame = {
    type: 'publish'
    topic: string
    payload: Uint8Array
}

/** Frame đăng ký danh sách event topic. */
export type RelaySubscribeFrame = {
    type: 'subscribe'
    topics: string[]
}

/** Frame hủy đăng ký danh sách event topic. */
export type RelayUnsubscribeFrame = {
    type: 'unsubscribe'
    topics: string[]
}

/** Frame quảng bá snapshot node tới relay và các peer. */
export type RelayHelloFrame = {
    type: 'hello'
    me: SpiderMeshNode
    target_id?: string
}

/** Frame thông báo node đã rời relay. */
export type RelayOfflineFrame = {
    type: 'offline'
    node_id: string
}

/** Union toàn bộ frame của SpiderMesh WebSocket protocol. */
export type RelayFrame = RelayRpcFrame | RelayPublishFrame | RelaySubscribeFrame | RelayUnsubscribeFrame | RelayHelloFrame | RelayOfflineFrame


/**
 * Cùng codec (`msgpackr`, cấu hình mặc định) với `@spider-mesh/tcp`, để mọi transporter truyền dữ liệu
 * giống hệt nhau (ví dụ `undefined` vẫn là `undefined`).
 */
export function encodeRelayFrame(frame: RelayFrame): Uint8Array {
    // `pack()` trả về một view nằm giữa buffer dùng chung của msgpackr. Chép ra mảng riêng để mọi
    // WebSocket (kể cả React Native) chỉ gửi đúng các byte của frame, không kèm phần còn lại.
    return new Uint8Array(pack(frame))
}

export function decodeRelayFrame(raw: Uint8Array) {
    try {
        return unpack(raw) as RelayFrame
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
