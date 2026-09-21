import {
    BehaviorSubject,
    distinctUntilChanged,
    map,
    merge,
    NEVER,
    Observable,
    share,
    Subscription,
    switchMap,
} from 'rxjs'
import type {
    EventBusOptions,
    EventDeliveryMode,
    EventLinkOptions,
    EventTransporter,
    EventTransporterSelector,
} from './types.js'

const selectorName = (selector?: EventTransporterSelector) => {
    return selector
}

/** Event runtime quản lý topic subscription và các event transporter độc lập RPC. */
export class EventBus {
    readonly #transporters$ = new BehaviorSubject(new Map<string, EventTransporter>())
    readonly #topicReferences = new Map<string, number>()
    readonly #mesh?: EventBusOptions['mesh']
    readonly #mode: EventDeliveryMode

    constructor(options: EventBusOptions = {}) {
        this.#mesh = options.mesh
        this.#mode = options.mode ?? 'single'
    }

    registerTransporter(transporter: EventTransporter) {
        const resolvedName = transporter.name
        if (!resolvedName) throw new Error('EventTransporter.name is required')
        if (this.#transporters$.value.has(resolvedName)) {
            throw new Error(`Event transporter "${resolvedName}" is already registered`)
        }
        const transporters = new Map(this.#transporters$.value)
        transporters.set(resolvedName, transporter)
        this.#transporters$.next(transporters)
        if (transporter.metadata) {
            this.#mesh?.setLocalTransporterMetadata(resolvedName, transporter.metadata)
        }

        const subscription = new Subscription()
        if (transporter.metadata$) {
            subscription.add(transporter.metadata$.subscribe(metadata => {
                this.#mesh?.setLocalTransporterMetadata(resolvedName, metadata)
            }))
        }

        subscription.add(() => {
            if (this.#transporters$.value.get(resolvedName) !== transporter) return
            const next = new Map(this.#transporters$.value)
            next.delete(resolvedName)
            this.#transporters$.next(next)
            if (transporter.metadata || transporter.metadata$) {
                this.#mesh?.setLocalTransporterMetadata(resolvedName, false)
            }
        })
        return subscription
    }

    link<T>(factory: { new(...args: any[]): T }, options: EventLinkOptions = {}) {
        return this.linkTopic<T>(factory.name, options)
    }

    linkTopic<T>(topic: string, options: EventLinkOptions = {}) {
        const listen$ = new Observable<T>(subscriber => {
            this.#acquireTopic(topic)
            const subscription = this.#transporters$.pipe(
                map(transporters => this.#select(transporters, options)),
                distinctUntilChanged((previous, current) => (
                    previous.length === current.length
                    && previous.every((item, index) => item === current[index])
                )),
                switchMap(transporters => {
                    if (transporters.length === 0) return NEVER
                    return merge(...transporters.map(transporter => transporter.listen<T>(topic)))
                }),
            ).subscribe(subscriber)

            return () => {
                subscription.unsubscribe()
                this.#releaseTopic(topic)
            }
        }).pipe(share())

        return {
            publish: async (data: T) => {
                const transporters = this.#select(this.#transporters$.value, options)
                if (transporters.length === 0) {
                    const requested = selectorName(options.transporter)
                    throw new Error(requested
                        ? `Event transporter ${requested} is not registered`
                        : `No event transporter is registered for topic ${topic}`)
                }
                await Promise.all(transporters.map(transporter => transporter.publish(topic, data)))
            },
            listen: () => listen$,
        }
    }

    #select(transporters: Map<string, EventTransporter>, options: EventLinkOptions) {
        const requested = selectorName(options.transporter)
        if (requested) {
            const selected = transporters.get(requested)
            return selected ? [selected] : []
        }

        const values = [...transporters.values()]
        const mode = options.mode ?? this.#mode
        return mode === 'fanout' ? values : values.slice(0, 1)
    }

    #acquireTopic(topic: string) {
        this.#topicReferences.set(topic, (this.#topicReferences.get(topic) ?? 0) + 1)
        this.#publishTopics()
    }

    #releaseTopic(topic: string) {
        const count = this.#topicReferences.get(topic) ?? 0
        if (count <= 1) this.#topicReferences.delete(topic)
        else this.#topicReferences.set(topic, count - 1)
        this.#publishTopics()
    }

    #publishTopics() {
        this.#mesh?.setLocalTopics([...this.#topicReferences.keys()])
    }
}
