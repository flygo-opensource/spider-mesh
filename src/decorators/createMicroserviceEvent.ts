import { SpiderMesh } from "../SpiderMesh"



export const EventSubscriberHook = Symbol.for('SubscribeEventHook')

export type EventSubscriberMetadata = {
    method: string
    event: string,

}



export const createMicroserviceEvent = <T>(event: string, decorator: () => ClassDecorator = () => (c) => c) => {

    @decorator()
    class C {

        static Payload = {} as T
        constructor(public sm: SpiderMesh) { }
        publish(data: T) {
            this.sm.publish(event, data)
        }
        subscribe(cb: (node: string, data: T) => any) {
            this.sm.subscribe(event, cb)
        }

        static subscribe() {
            return <T>(
                target: Object,
                method: string | symbol,
                descriptor: TypedPropertyDescriptor<T>
            ) => {
                if (typeof method == 'string') {
                    const value: EventSubscriberMetadata = {
                        method,
                        event,
                    }
                    Object.defineProperty(descriptor.value, EventSubscriberHook, { value })
                }
            }

        }

    }
    Object.defineProperty(C, 'name', { value: event })
    return C

}





export const listEventSubscribers = (target) => {
    const list = new Array<EventSubscriberMetadata>()
    for (let f = target; f != null; f = Object.getPrototypeOf(f)) {
        for (const method of Object.getOwnPropertyNames(f)) {
            const metadata: EventSubscriberMetadata = f[method]?.[EventSubscriberHook]
            metadata && list.push(metadata)
        }
    }
    return list
} 