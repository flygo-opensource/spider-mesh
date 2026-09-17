
export const SPIDERMESH_NAMESPACE = process.env.SPIDERMESH_NAMESPACE || 'default'
export const SPIDERMESH_NODE_HOSTNAME = process.env.SPIDERMESH_NODE_HOSTNAME || ''
export const SPIDERMESH_BUILD_VERSION = process.env.APP_VERSION || process.env.SPIDERMESH_VERSION || undefined
export const SPIDERMESH_BUILD_GIT_TAG = process.env.GIT_TAG || undefined
export const SPIDERMESH_BUILD_GIT_BRANCH = process.env.GIT_BRANCH || undefined
export const SPIDERMESH_BUILD_GIT_COMMIT = process.env.GIT_COMMIT || undefined
export const SPIDERMESH_BUILD_TIME = process.env.BUILD_TIME ? Number(process.env.BUILD_TIME) : undefined
export const SPIDERMESH_BUILD_ENVIRONMENT = process.env.APP_ENV || process.env.NODE_ENV || undefined
export const SPIDERMESH_BUILD_TAGS = (() => {
    const raw = process.env.APP_TAGS
    if (!raw) return undefined
    try { return JSON.parse(raw) as Record<string, string> } catch { return undefined }
})()
