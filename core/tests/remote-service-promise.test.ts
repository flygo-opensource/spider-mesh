import { expect, test } from 'bun:test'
import { lastValueFrom, Subject, toArray } from 'rxjs'
import { RemoteServiceLinker } from '../src/RemoteService.js'
import { SpiderMesh } from '../src/SpiderMesh.js'
import { normalizeRpcError } from '../src/helpers/normalizeRpcError.js'
import type { RpcCancelPacket, RpcEvent, RpcRequestPacket, RpcResponsePacket, RpcTransporter } from '../src/types.js'

/** Trả lời mọi request bằng `answer` (hoặc lỗi), và đếm số request đã gửi. */
class ScriptedTransporter extends Subject<RpcEvent> implements RpcTransporter {
    public readonly name = 'scripted'
    requests = 0

    constructor(private readonly reply: Omit<RpcResponsePacket, 'kind' | 'request_id'>) {
        super()
    }

    async send(packet: RpcRequestPacket | RpcResponsePacket | RpcCancelPacket) {
        if (packet.kind === 'request') {
            this.requests++
            queueMicrotask(() => this.next({ rpc: { kind: 'response', request_id: packet.request_id, ...this.reply } }))
        }
        return { cancel: () => {} }
    }

    canRoute() {
        return true
    }
}

type Answer = { get(): Promise<number> }

const link = (reply: Omit<RpcResponsePacket, 'kind' | 'request_id'>) => {
    const transporter = new ScriptedTransporter(reply)
    const service = RemoteServiceLinker.link<Answer>(new SpiderMesh({ transporters: [transporter] }), { service: 'Answer' })
    return { transporter, service }
}

test('a remote call chains like a real Promise', async () => {
    const { service } = link({ data: 42, completed: true })

    expect(await service.get()).toBe(42)
    expect(await service.get().then(value => value + 1)).toBe(43)
    expect(await Promise.resolve(service.get())).toBe(42)

    let finallyRan = false
    expect(await service.get().finally(() => { finallyRan = true })).toBe(42)
    expect(finallyRan).toBe(true)
})

test('a failing remote call can be handled with catch or the second then argument', async () => {
    const { service } = link({ error: { code: 'BOOM', message: 'boom' }, completed: true })

    expect(await service.get().catch((error: { code: string }) => error.code)).toBe('BOOM')
    expect(await service.get().then(() => 'resolved', (error: { code: string }) => `rejected:${error.code}`)).toBe('rejected:BOOM')
    // `expect().rejects` của bun cần một Promise thật; `Promise.resolve` nhận thenable như mọi code dùng Promise.
    await expect(Promise.resolve(service.get())).rejects.toMatchObject({ code: 'BOOM', message: 'boom' })
})

test('awaiting the same call twice sends one request', async () => {
    const { transporter, service } = link({ data: 7, completed: true })

    const call = service.get()
    expect(await call).toBe(7)
    expect(await call).toBe(7)
    expect(await call.then(value => value * 2)).toBe(14)
    expect(transporter.requests).toBe(1)
})

test('the same call still works as an Observable', async () => {
    const { service } = link({ data: 5, completed: true })

    const call = service.get() as unknown as import('rxjs').Observable<number>
    expect(await lastValueFrom(call.pipe(toArray()))).toEqual([5])
})

test('errors without a code carry no code field at all', () => {
    expect(normalizeRpcError(new Error('plain'))).toEqual({ message: 'plain' })
    expect('code' in normalizeRpcError(new Error('plain'))).toBe(false)
    expect(normalizeRpcError({ code: 'E1', message: 'coded' })).toEqual({ code: 'E1', message: 'coded' })
    expect(normalizeRpcError('text')).toEqual({ message: 'text' })
})
