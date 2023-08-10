import { Observable } from 'rxjs'
import { SpiderMeshTransporterEvent } from '../interfaces/SpiderMeshTransporter.js'


const key = Symbol.for('SubscribeEvent')


export type EventMetadata<T = {}> = {
    method: string,
    event: string,
    buffer_ms?: number
}

export type EventHub<T> = {
    publish: (data: T) => void,
    listen: () => Observable<SpiderMeshTransporterEvent<T>>
}

export const ListenEvent = <R = any, T = {}>(factory: { new(): EventHub<T> }) => (
    target: Object,
    method,
    descriptor: TypedPropertyDescriptor<(event: SpiderMeshTransporterEvent<T>) => R>
) => {
    const event = factory.name
    Object.defineProperty(descriptor.value, key, { value: { method, event } as EventMetadata<T> })
}

export const ListenEventBatch = <R = any, T = {}>(factory: { new(): EventHub<T> }, buffer_ms: number) => (
    target: Object,
    method,
    descriptor: TypedPropertyDescriptor<(event: Array<SpiderMeshTransporterEvent<T>>) => R>
) => {
    const event = factory.name
    Object.defineProperty(descriptor.value, key, { value: { method, event, buffer_ms } as EventMetadata<T> })
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
        new(): EventHub<T>
    }
}


export type EventDataType<T extends EventHub<any>> = Parameters<T['publish']>[0]
export type E<T extends EventHub<any>> = SpiderMeshTransporterEvent<EventDataType<T>>