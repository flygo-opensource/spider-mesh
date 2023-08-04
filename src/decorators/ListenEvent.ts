
export const key = Symbol.for('SubscribeEvent')

export type EventMetadata = { method: string, event: string }

export const ListenEvent = <T>(factory: { new(): T }) => (
    target: Object,
    method,
    descriptor: TypedPropertyDescriptor<(event: T) => void>
) => {
    const event = factory.name
    Object.defineProperty(descriptor.value, key, { value: { method, event } as EventMetadata })
}

export const listEventSubscribers = (target) => {
    const methods = [] as EventMetadata[]
    for (let f = target; f != null; f = Object.getPrototypeOf(f)) {
        for (const method of Object.getOwnPropertyNames(f)) {
            f[method]?.[key] && methods.push(f[method]?.[key])
        }
    }
    return methods
}  