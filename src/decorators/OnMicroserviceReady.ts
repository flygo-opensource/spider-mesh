
export const key = Symbol.for('OnMicroserviceReadyHook')

export type OnMicroserviceReadyMetadata = { method: string }

export const OnMicroserviceReady = () => <T>(
    target: Object,
    method: string | symbol,
    descriptor: TypedPropertyDescriptor<T>
) => {
    Object.defineProperty(descriptor.value, key, { value: { method } as OnMicroserviceReadyMetadata })
}

export const listReadyHookMethods = (target) => {
    const methods = [] as OnMicroserviceReadyMetadata[]
    for (let f = target; f != null; f = Object.getPrototypeOf(f)) {
        for (const method of Object.getOwnPropertyNames(f)) {
            f[method]?.[key] && methods.push(f[method]?.[key])
        }
    }
    return methods
} 