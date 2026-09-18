import type { SpiderMeshError } from '../types.js'

export const normalizeRpcError = (
    error: unknown,
): SpiderMeshError | { code?: string; message: string } => {
    if (error && typeof error === 'object') {
        const candidate = error as { code?: unknown; message?: unknown }
        const message = typeof candidate.message === 'string' ? candidate.message : 'Unknown RPC error'
        // Chỉ gửi `code` khi có: một số codec (msgpack của ws) biến `undefined` thành `null`, khiến
        // bên gọi nhận `code: null` trái với kiểu `code?: string`.
        return typeof candidate.code === 'string' ? { code: candidate.code, message } : { message }
    }

    return { message: typeof error === 'string' ? error : 'Unknown RPC error' }
}
