// Bộ kiểm tra hợp đồng RPC dùng chung. File này giống hệt nhau ở @spider-mesh/ws và
// @spider-mesh/tcp; sửa ở một nơi thì chép sang nơi kia.
import { RemoteServiceLinker, type SpiderMesh } from '@spider-mesh/core'
import { lastValueFrom, materialize, take, timeout, toArray, type Observable } from 'rxjs'

export type ContractTransport = 'websocket' | 'http2'
export type ContractResult = { name: string, pass: boolean, actual?: unknown, expected?: unknown }

type ContractApi = {
    valueSync(): Promise<string>
    valueAsync(): Promise<string>
    streamSync(): Observable<number>
    streamSyncDelayed(): Observable<number>
    streamAsync(): Observable<number>
    streamAsyncDelayed(): Observable<number>
    streamEmpty(): Observable<never>
    streamAsyncEmpty(): Observable<never>
    throwSync(): Promise<string>
    throwAsync(): Promise<string>
    streamThrowsBeforeReturn(): Observable<number>
    streamAsyncRejects(): Observable<number>
    streamErrorNow(): Observable<never>
    streamErrorAfterValues(): Observable<number>
    streamErrorInOperator(): Observable<number>
    streamAsyncErrorNow(): Observable<never>
    streamAsyncErrorAfterValues(): Observable<number>
    fail(path: FailurePath, kind: FailureKind): Observable<unknown>
    returnsNull(): Promise<null>
    returnsUndefined(): Promise<undefined>
    noArgs(): Promise<string>
    manyArgs(a: number, b: string, c: boolean, d: null, e: number[]): Promise<unknown>
    echo<T>(value: T): Promise<T>
    bigPayload(size: number): Promise<string>
    longStream(count: number): Observable<number>
    slow(ms: number): Promise<string>
    endless(): Observable<number>
    activeStreams(): Promise<number>
    doesNotExist(): Promise<never>
}

type FailurePath = 'sync-throw' | 'async-throw' | 'observable' | 'observable-after-values'
type FailureKind = 'error' | 'code-object' | 'string' | 'error-with-code' | 'error-subclass'
type StreamEvent = ['next', unknown] | ['error', string | null, string | undefined] | ['complete']

/** Ghi lại một stream thành danh sách sự kiện: ['next', v] | ['error', code, message] | ['complete']. */
const record = (stream: Observable<unknown>) => lastValueFrom(stream.pipe(materialize(), toArray(), timeout(10_000)))
    .then(events => events.map(event => event.kind === 'N'
        ? ['next', event.value]
        : event.kind === 'E'
            ? ['error', (event.error as { code?: string })?.code ?? null, (event.error as { message?: string })?.message]
            : ['complete']))

/** Kết quả của một lời gọi await: { value } hoặc { error: { code?, message } } (chỉ có key thật sự nhận được). */
const settle = async (run: () => Promise<unknown>) => {
    try {
        return { value: await run() }
    } catch (error) {
        const { code, message, name } = error as { code?: string, message?: string, name?: string }
        return { error: { ...('code' in (error as object) ? { code } : {}), message, ...(name === 'EmptyError' ? { name } : {}) } }
    }
}

export async function runContract(mesh: SpiderMesh, transport: ContractTransport): Promise<ContractResult[]> {
    const api = RemoteServiceLinker.link<ContractApi>(mesh, { service: 'ContractService', timeout: 10_000 })
    await api.wait()

    const results: ContractResult[] = []
    const check = async (name: string, run: () => Promise<unknown>, expected: unknown) => {
        let actual: unknown
        try {
            actual = await run()
        } catch (error) {
            actual = { unexpectedThrow: String((error as Error)?.message ?? error) }
        }
        results.push({ name, pass: deepEquals(actual, expected), actual, expected })
    }

    // ===== 1. Ma trận dạng method × cách dùng (subscribe / await) =====
    // Mỗi dạng chỉ khai báo luồng sự kiện khi subscribe; kết quả khi await suy ra từ luồng đó.
    const next = (value: unknown): StreamEvent => ['next', value]
    const failed = (message: string, code: string | null = null): StreamEvent => ['error', code, message]
    const complete: StreamEvent = ['complete']
    const shapes: { name: string, call: () => unknown, events: StreamEvent[] }[] = [
        { name: 'sync method', call: () => api.valueSync(), events: [next('value'), complete] },
        { name: 'async method', call: () => api.valueAsync(), events: [next('value'), complete] },
        { name: 'sync observable', call: () => api.streamSync(), events: [next(1), next(2), next(3), complete] },
        { name: 'sync observable, delayed values', call: () => api.streamSyncDelayed(), events: [next(1), next(2), next(3), complete] },
        { name: 'async observable', call: () => api.streamAsync(), events: [next(1), next(2), next(3), complete] },
        { name: 'async observable, delayed values', call: () => api.streamAsyncDelayed(), events: [next(1), next(2), next(3), complete] },
        { name: 'empty observable', call: () => api.streamEmpty(), events: [complete] },
        { name: 'async empty observable', call: () => api.streamAsyncEmpty(), events: [complete] },
        { name: 'error: sync method throws', call: () => api.throwSync(), events: [failed('sync error')] },
        { name: 'error: async method rejects', call: () => api.throwAsync(), events: [failed('async error')] },
        { name: 'error: sync observable method throws before returning', call: () => api.streamThrowsBeforeReturn(), events: [failed('stream method threw')] },
        { name: 'error: async observable method rejects before returning', call: () => api.streamAsyncRejects(), events: [failed('async stream method rejected')] },
        { name: 'error: sync observable errors immediately', call: () => api.streamErrorNow(), events: [failed('stream error now')] },
        { name: 'error: sync observable errors after values', call: () => api.streamErrorAfterValues(), events: [next(1), next(2), failed('stream error after values')] },
        { name: 'error: sync observable errors inside an operator', call: () => api.streamErrorInOperator(), events: [next(1), failed('operator error')] },
        { name: 'error: async observable errors immediately', call: () => api.streamAsyncErrorNow(), events: [failed('async stream error now')] },
        { name: 'error: async observable errors after values', call: () => api.streamAsyncErrorAfterValues(), events: [next(1), next(2), failed('async stream error after values')] },
    ]
    const awaitResult = (events: StreamEvent[]) => {
        const [first] = events
        if (first[0] === 'next') return { value: first[1] }
        if (first[0] === 'error') return { error: first[1] === null ? { message: first[2] } : { code: first[1], message: first[2] } }
        return { error: { message: 'no elements in sequence', name: 'EmptyError' } }
    }
    for (const shape of shapes) {
        await check(`${shape.name} · subscribe`, () => record(shape.call() as Observable<unknown>), shape.events)
        await check(`${shape.name} · await`, () => settle(() => shape.call() as Promise<unknown>), awaitResult(shape.events))
    }

    // ===== 2. Ma trận loại lỗi × đường lỗi =====
    const kinds: { kind: FailureKind, code: string | null, message: string }[] = [
        { kind: 'error', code: null, message: 'plain error' },
        { kind: 'code-object', code: 'E_OBJECT', message: 'code object' },
        { kind: 'string', code: null, message: 'thrown string' },
        { kind: 'error-with-code', code: 'E_WITH_CODE', message: 'error with code' },
        { kind: 'error-subclass', code: 'E_SUBCLASS', message: 'error subclass' },
    ]
    const paths: { path: FailurePath, events: (error: StreamEvent) => StreamEvent[] }[] = [
        { path: 'sync-throw', events: error => [error] },
        { path: 'async-throw', events: error => [error] },
        { path: 'observable', events: error => [error] },
        { path: 'observable-after-values', events: error => [next('before'), error] },
    ]
    for (const { kind, code, message } of kinds) {
        for (const { path, events } of paths) {
            await check(`error kind ${kind} via ${path}`, () => record(api.fail(path, kind)), events(failed(message, code)))
        }
    }

    // ===== 3. Giá trị và kiểu dữ liệu =====
    await check('returns null', () => settle(() => api.returnsNull()), { value: null })
    await check('returns undefined', () => settle(() => api.returnsUndefined()), { value: undefined })
    await check('no arguments', () => settle(() => api.noArgs()), { value: 'no-args' })
    await check('many arguments', () => settle(() => api.manyArgs(1, 'b', true, null, [1, 2])),
        { value: { a: 1, b: 'b', c: true, d: null, e: [1, 2] } })
    const nested = { a: { b: [1, { c: 'd' }] }, negative: -1.5, maxSafe: Number.MAX_SAFE_INTEGER, empty: {}, list: [] }
    await check('nested object', () => settle(() => api.echo(nested)), { value: nested })
    await check('undefined field', () => settle(() => api.echo({ a: 1, u: undefined })), { value: { a: 1, u: undefined } })
    await check('unicode string', () => settle(() => api.echo('Xin chào 👋 — ñ')), { value: 'Xin chào 👋 — ñ' })
    await check('Date', () => settle(() => api.echo(new Date('2026-01-02T03:04:05.000Z'))),
        { value: new Date('2026-01-02T03:04:05.000Z') })
    await check('Uint8Array', () => settle(() => api.echo(new Uint8Array([0, 1, 255]))), { value: new Uint8Array([0, 1, 255]) })
    // Map không giữ kiểu, nhưng dữ liệu phải còn nguyên (thành object thường).
    await check('Map arrives as a plain object', () => settle(() => api.echo(new Map([['k', 1]]))), { value: { k: 1 } })
    await check('1 MB payload', async () => (await api.bigPayload(1_000_000)).length, 1_000_000)
    await check('500 values keep order', async () => {
        const events = await record(api.longStream(500))
        return { count: events.length - 1, ordered: events.slice(0, -1).every((event, index) => event[1] === index), last: events.at(-1) }
    }, { count: 500, ordered: true, last: ['complete'] })

    // ===== 4. Huỷ stream =====
    await check('unsubscribe stops the provider stream', async () => {
        await lastValueFrom(api.endless().pipe(take(3), toArray()))
        await new Promise(resolve => setTimeout(resolve, 500))
        return api.activeStreams()
    }, 0)
    await check('await takes the first value and stops the provider stream', async () => {
        const first = await api.endless()
        await new Promise(resolve => setTimeout(resolve, 500))
        return { first, active: await api.activeStreams() }
    }, { first: 0, active: 0 })

    // ===== 5. Lỗi hệ thống =====
    await check('method not found', async () => (await settle(() => api.doesNotExist())).error?.code, 'MICROSERVICE_NOT_FOUND')
    await check('service not found', async () => {
        const missing = RemoteServiceLinker.link<{ run(): Promise<void> }>(mesh, { service: 'NoSuchService' })
        return (await settle(() => missing.run())).error?.code
    }, 'MICROSERVICE_OFFLINE')
    await check('timeout', async () => {
        const hurried = RemoteServiceLinker.link<ContractApi>(mesh, { service: 'ContractService', timeout: 300 })
        return (await settle(() => hurried.slow(3_000))).error?.code
    }, 'MICROSERVICE_RPC_TIMEOUT')

    // ===== 6. Dùng như Promise và gọi song song =====
    await check('then chains the value', () => api.valueAsync().then(value => `${value}!`), 'value!')
    await check('catch receives the error', () => api.throwAsync().catch((error: { message: string }) => error.message), 'async error')
    await check('50 concurrent calls', async () => {
        const values = await Promise.all(Array.from({ length: 50 }, () => api.valueAsync()))
        return values.length === 50 && values.every(value => value === 'value')
    }, true)

    return results
}

/**
 * So sánh sâu chạy được ở mọi môi trường (bun, Node, trình duyệt), để file này dùng được cả trong
 * trình duyệt. Phân biệt `{ u: undefined }` với `{}`, so byte cho typed array, so thời điểm cho Date.
 */
function deepEquals(a: unknown, b: unknown): boolean {
    if (Object.is(a, b)) return true
    if (a instanceof Date || b instanceof Date) {
        return a instanceof Date && b instanceof Date && a.getTime() === b.getTime()
    }
    if (ArrayBuffer.isView(a) || ArrayBuffer.isView(b)) {
        if (!ArrayBuffer.isView(a) || !ArrayBuffer.isView(b)) return false
        const left = new Uint8Array(a.buffer, a.byteOffset, a.byteLength)
        const right = new Uint8Array(b.buffer, b.byteOffset, b.byteLength)
        return left.length === right.length && left.every((byte, index) => byte === right[index])
    }
    if (a && b && typeof a === 'object' && typeof b === 'object') {
        if (Array.isArray(a) !== Array.isArray(b)) return false
        const leftKeys = Object.keys(a)
        const rightKeys = Object.keys(b)
        return leftKeys.length === rightKeys.length
            && leftKeys.every(key => key in b && deepEquals((a as Record<string, unknown>)[key], (b as Record<string, unknown>)[key]))
    }
    return false
}
