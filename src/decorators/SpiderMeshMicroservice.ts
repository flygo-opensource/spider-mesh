import { listReadyHookMethods } from "./OnMicroserviceReady"
import { EventSubscriberMetadata, listEventSubscribers } from "./createMicroserviceEvent"

export const ServiceInstanceList = new Array<{
    factory: any,
    name: string,
    instance: any,
    ready_hook_methods: string[]
    event_subscribers: EventSubscriberMetadata[]
}>()

export const SpiderMeshMicroservice = () => (factory: { new(...args: any[]): any }) => {

    const C = class extends factory {
        constructor(...args: any[]) {
            super(...args)
            const ready_hook_methods = listReadyHookMethods(factory.prototype)
            const event_subscribers = listEventSubscribers(factory.prototype)
            ServiceInstanceList.push({
                factory,
                name: factory.name,
                instance: this,
                ready_hook_methods,
                event_subscribers
            })
        }
    }
    Object.defineProperty(C, 'name', {
        value: factory.name
    })
    return C as any
} 