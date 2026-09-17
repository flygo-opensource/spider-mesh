import type { SpiderMeshError } from '../types.js'

export const normalizeRpcError = (
    error: unknown,
): SpiderMeshError | { code?: string; message: string } => {
    if (error && typeof error === 'object') {
        const candidate = error as { code?: unknown; message?: unknown }
        return {
            code: typeof candidate.code === 'string' ? candidate.code : undefined,
            message: typeof candidate.message === 'string' ? candidate.message : 'Unknown RPC error',
        }
    }

    return { message: typeof error === 'string' ? error : 'Unknown RPC error' }
}
