
// Trình duyệt và React Native không có `process`: đọc biến môi trường qua đây để import core không
// ném ReferenceError, và rơi về giá trị mặc định khi không có.
const env: Record<string, string | undefined> = typeof process !== 'undefined' && process.env ? process.env : {}

export const SPIDERMESH_NAMESPACE = env.SPIDERMESH_NAMESPACE || 'default'
export const SPIDERMESH_NODE_HOSTNAME = env.SPIDERMESH_NODE_HOSTNAME || ''
export const SPIDERMESH_BUILD_VERSION = env.APP_VERSION || env.SPIDERMESH_VERSION || undefined
export const SPIDERMESH_BUILD_GIT_TAG = env.GIT_TAG || undefined
export const SPIDERMESH_BUILD_GIT_BRANCH = env.GIT_BRANCH || undefined
export const SPIDERMESH_BUILD_GIT_COMMIT = env.GIT_COMMIT || undefined
export const SPIDERMESH_BUILD_TIME = env.BUILD_TIME ? Number(env.BUILD_TIME) : undefined
export const SPIDERMESH_BUILD_ENVIRONMENT = env.APP_ENV || env.NODE_ENV || undefined
export const SPIDERMESH_BUILD_TAGS = (() => {
    const raw = env.APP_TAGS
    if (!raw) return undefined
    try { return JSON.parse(raw) as Record<string, string> } catch { return undefined }
})()
