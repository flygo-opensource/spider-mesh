import { PassThrough } from "stream" 

export const EventSubscriberHook = Symbol.for('SubscribeEventHook')

export type EventSubscriberMetadata = {
    method: string
    event: string,
    requests: PassThrough
}

export const createMicroserviceEvent = <T>(event: string) => {

    const requests = new PassThrough()

    const subscribe_event_decorator: () => MethodDecorator = () => <T>(
        target: Object,
        method: string | symbol,
        descriptor: TypedPropertyDescriptor<T>
    ) => {
        if (typeof method == 'string') {
            const value: EventSubscriberMetadata = {
                method,
                event,
                requests
            }
            Object.defineProperty(descriptor.value, EventSubscriberHook, { value })
        }
    }

    const publisher = (data: T) => requests.emit(event, data)

    class EventPublisherClass {
        publish(data: T) {
            requests.emit(event, data)
        }
    }




    return [EventPublisherClass, subscribe_event_decorator, publisher] as [
        typeof EventPublisherClass,
        typeof subscribe_event_decorator,
        typeof publisher
    ]
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