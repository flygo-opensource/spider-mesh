import { EventBus } from './EventBus.js'

export const NestJSLinkEvent = (factory: any) => ({
    provide: factory,
    inject: [EventBus],
    useFactory: (events: EventBus) => events.link(factory),
})
