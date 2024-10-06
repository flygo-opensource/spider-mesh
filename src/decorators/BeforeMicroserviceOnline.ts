import { SpiderMesh } from "../SpiderMesh.js"

export const key = Symbol.for('BeforeMicroserviceOnline')

export type BeforeMicroserviceOnlineMetadata = { method: string }

export const BeforeMicroserviceOnline = () => <T>(
    target: Object,
    method: string | symbol,
    descriptor: TypedPropertyDescriptor<T | ((sm?: SpiderMesh) => Promise<void>)>
) => {
    Object.defineProperty(descriptor.value, key, { value: { method } as BeforeMicroserviceOnlineMetadata })
}

export const listBeforeMicroserviceOnlineMethods = (target: any) => {
    const methods = [] as BeforeMicroserviceOnlineMetadata[]
    for (let f = target; f != null; f = Object.getPrototypeOf(f)) {
        for (const method of Object.getOwnPropertyNames(f)) {
            f[method]?.[key] && methods.push(f[method]?.[key])
        }
    }
    return methods
} 