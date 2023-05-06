
export const OnMicroserviceReadyHook = Symbol.for('OnMicroserviceReadyHook')

export const OnMicroserviceReady: () => MethodDecorator = () => <T>(target: Object, propertyKey: string | symbol, descriptor: TypedPropertyDescriptor<T>) => {
    Object.defineProperty(descriptor.value, OnMicroserviceReadyHook, { value: true })
}

export const listReadyHookMethods = (target) => {
    const methods = [] as string[]
    for (let f = target; f != null; f = Object.getPrototypeOf(f)) {
        for (const method of Object.getOwnPropertyNames(f)) {
            f[method]?.[OnMicroserviceReadyHook] && methods.push(method)
        }
    }
    return methods
} 