import { Duplex } from "stream"


export const EventSubscriberHook = Symbol.for('SubscribeEventHook')

export type EventSubscriberMetadata = {
    method: string
    event: string,
    requests: Duplex
}

export const createMicroserviceEvent = <T>(event: string) => {

    const requests = new Duplex()

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


    class Publisher {
        publish(data: T) {
            requests.emit('data', data)
        }
    }

    return [Publisher, subscribe_event_decorator] as [
        typeof Publisher,
        typeof subscribe_event_decorator
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