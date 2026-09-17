import {
    SPIDERMESH_BUILD_GIT_BRANCH,
    SPIDERMESH_BUILD_GIT_COMMIT,
    SPIDERMESH_BUILD_ENVIRONMENT,
    SPIDERMESH_BUILD_GIT_TAG,
    SPIDERMESH_BUILD_TAGS,
    SPIDERMESH_BUILD_TIME,
    SPIDERMESH_BUILD_VERSION,
} from '../../const.js'
import type { BuildInfo } from '../types.js'

export const readBuildInfo = (): BuildInfo | undefined => {
    const runtime = (() => {
        if (typeof Bun !== 'undefined') return `bun@${(Bun as any).version}`
        if (typeof process !== 'undefined') return `node@${process.version}`
        return undefined
    })()

    const info: BuildInfo = {
        version: SPIDERMESH_BUILD_VERSION,
        git_tag: SPIDERMESH_BUILD_GIT_TAG,
        git_branch: SPIDERMESH_BUILD_GIT_BRANCH,
        git_commit: SPIDERMESH_BUILD_GIT_COMMIT,
        build_time: SPIDERMESH_BUILD_TIME,
        environment: SPIDERMESH_BUILD_ENVIRONMENT,
        runtime,
        tags: SPIDERMESH_BUILD_TAGS,
    }

    return Object.values(info).some(value => value != null) ? info : undefined
}
