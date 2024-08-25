import { Observable } from 'rxjs'
import { SpiderMeshTransporterEvent } from '../interfaces/SpiderMeshTransporter.js' 
import { Encodable } from '../Encoder.js'
import { SpiderMesh } from '../SpiderMesh.js'


const key = Symbol.for('SubscribeEvent')


export type EventMetadata = {
    method: string,
    event: string,
    buffer_ms?: number
}

export type EventHub<T extends Encodable = Encodable> = {
    publish: (data: T) => void,
    listen: () => Observable<SpiderMeshTransporterEvent<T>>
}

export const ListenEvent = <T extends Encodable = Encodable>(factory: { new(): EventHub<T> }) => (
    target: Object,
    method:string,
    descriptor: TypedPropertyDescriptor<(event: SpiderMeshTransporterEvent<T>, sm?: SpiderMesh) => any>
) => {
    const event = factory.name
    Object.defineProperty(descriptor.value, key, { value: { method, event } as EventMetadata })
}

export const ListenEventBatch = <T extends Encodable = Encodable>(factory: { new(): EventHub<T> }, buffer_ms: number) => (
    target: Object,
    method:string,
    descriptor: TypedPropertyDescriptor<(event: Array<SpiderMeshTransporterEvent<T>>, sm?: SpiderMesh) => any>
) => {
    const event = factory.name
    Object.defineProperty(descriptor.value, key, { value: { method, event, buffer_ms } as EventMetadata })
}





export const listEventSubscribers = (target: any) => {
    const methods = [] as EventMetadata[]
    for (let f = target; f != null; f = Object.getPrototypeOf(f)) {
        for (const method of Object.getOwnPropertyNames(f)) {
            f[method]?.[key] && methods.push(f[method]?.[key])
        }
    }
    return methods
}




export function createSpiderMeshEvent<T extends Encodable = Encodable>() {
    return class { } as {
        new(): EventHub<T>
    }
}


export type EventDataType<T extends EventHub<any>> = Parameters<T['publish']>[0]
export type E<T extends EventHub<any>> = SpiderMeshTransporterEvent<EventDataType<T>>