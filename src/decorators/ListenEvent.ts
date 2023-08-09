import { Observable } from 'rxjs'
import { SpiderMeshTransporterEvent } from 'src/index.js'


export const key = Symbol.for('SubscribeEvent')


export type EventMetadata<T = {}> = {
    method: string,
    event: string,
    limit?: number
}

export const ListenEvent = <R = any, T = {}>(factory: { new(): T }, limit?: number) => (
    target: Object,
    method,
    descriptor: TypedPropertyDescriptor<(event: SpiderMeshTransporterEvent<T>) => R>
) => {
    const event = factory.name
    Object.defineProperty(descriptor.value, key, { value: { method, event, limit } as EventMetadata<T> })
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

export function createSpiderMeshEvent<T = {}>() {
    return class { } as {
        new():   {
            publish: (data: T) => Promise<void>,
            listen: () => Observable<T>
        }
    }
}
