import { expect, test } from 'bun:test'
import { firstValueFrom, Subject, timeout } from 'rxjs'
import { EventBus } from '../src/EventBus.js'
import type { EventMeshHost, EventTransporter } from '../src/types.js'

class MockMesh implements EventMeshHost {
    topics: string[] = []
    metadata = new Map<string, unknown>()

    setLocalTopics(topics: string[]) {
        this.topics = topics
    }

    setLocalTransporterMetadata(name: string, metadata: unknown = true) {
        this.metadata.set(name, metadata)
    }
}

class MockEventTransporter implements EventTransporter {
    public readonly name: string = 'mock-events'
    readonly #topics = new Map<string, Subject<unknown>>()
    readonly metadata = { port: 4321 }
    published = 0

    async publish<T>(topic: string, data: T) {
        this.published++
        this.#topic(topic).next(data)
    }

    listen<T>(topic: string) {
        return this.#topic(topic) as Subject<T>
    }

    #topic(topic: string) {
        const current = this.#topics.get(topic)
        if (current) return current
        const created = new Subject<unknown>()
        this.#topics.set(topic, created)
        return created
    }
}

test('tracks subscribed topics through the mesh bridge', () => {
    class UserCreated {}
    const mesh = new MockMesh()
    const events = new EventBus({ mesh })
    events.registerTransporter(new MockEventTransporter())
    const linked = events.link(UserCreated)
    const subscription = linked.listen().subscribe()

    expect(mesh.topics).toEqual(['UserCreated'])
    expect(mesh.metadata.get('mock-events')).toEqual({ port: 4321 })
    subscription.unsubscribe()
    expect(mesh.topics).toEqual([])
})

test('a listener survives registration after subscription', async () => {
    class UserCreated { constructor(readonly id: string) {} }
    const events = new EventBus()
    const linked = events.link(UserCreated)
    const received = firstValueFrom(linked.listen().pipe(timeout(1000)))
    events.registerTransporter(new MockEventTransporter())

    await linked.publish(new UserCreated('42'))
    expect((await received).id).toBe('42')
})

test('single mode avoids duplicate publication and fanout is explicit', async () => {
    class UserCreated {}
    class FirstEventTransporter extends MockEventTransporter {
        override readonly name = 'first'
    }
    class SecondEventTransporter extends MockEventTransporter {
        override readonly name = 'second'
    }
    const first = new FirstEventTransporter()
    const second = new SecondEventTransporter()
    const events = new EventBus()
    events.registerTransporter(first)
    events.registerTransporter(second)

    await events.link(UserCreated).publish(new UserCreated())
    expect(first.published).toBe(1)
    expect(second.published).toBe(0)

    await events.link(UserCreated, { mode: 'fanout' }).publish(new UserCreated())
    expect(first.published).toBe(2)
    expect(second.published).toBe(1)
})

test('rejects duplicate hardcoded event transporter names', () => {
    const events = new EventBus()
    events.registerTransporter(new MockEventTransporter())

    expect(() => events.registerTransporter(new MockEventTransporter()))
        .toThrow('Event transporter "mock-events" is already registered')
})
