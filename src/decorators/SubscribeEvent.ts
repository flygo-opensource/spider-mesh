
export const key = Symbol.for('SubscribeEvent')
 

export const SubscribeEvent = (event: string) => <T>(
    target: Object,
    method,
    descriptor: TypedPropertyDescriptor<T>
) => {
    Object.defineProperty(descriptor.value, key, { value: { method, event } })
}

export const listEventSubscribers = (target) => {
    const methods = [] as Array<{method:string, event:string}>
    for (let f = target; f != null; f = Object.getPrototypeOf(f)) {
        for (const method of Object.getOwnPropertyNames(f)) {
            f[method]?.[key] && methods.push(f[method]?.[key])
        }
    }
    return methods
}  