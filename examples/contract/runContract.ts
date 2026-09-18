// Bộ kiểm tra hợp đồng RPC dùng chung. File này giống hệt nhau ở @spider-mesh/ws và
// @spider-mesh/tcp; sửa ở một nơi thì chép sang nơi kia.
import { RemoteServiceLinker, type SpiderMesh } from '@spider-mesh/core'
import { lastValueFrom, materialize, take, timeout, toArray, type Observable } from 'rxjs'

export type ContractTransport = 'websocket' | 'http2'
export type ContractResult = { name: string, pass: boolean, actual?: unknown, expected?: unknown }

type ContractApi = {
    syncValue(input: string): Promise<string>
    asyncValue(input: string): Promise<string>
    returnsNull(): Promise<null>
    returnsUndefined(): Promise<undefined>
    noArgs(): Promise<string>
    manyArgs(a: number, b: string, c: boolean, d: null, e: number[]): Promise<unknown>
    echo<T>(value: T): Promise<T>
    bigPayload(size: number): Promise<string>
    slow(ms: number): Promise<string>
    syncObservable(): Observable<number>
    asyncObservable(): Observable<string>
    emptyObservable(): Observable<never>
    longStream(count: number): Observable<number>
    endless(): Observable<number>
    activeStreams(): Promise<number>
    syncThrow(): Promise<never>
    asyncThrow(): Promise<never>
    observableThrowNow(): Observable<never>
    streamThenError(): Observable<number>
    asyncObservableError(): Observable<never>
    asyncObservableThenError(): Observable<string>
    syncCodeError(): Promise<never>
    asyncCodeError(): Promise<never>
    observableCodeError(): Observable<never>
    throwString(): Promise<never>
    throwErrorWithCode(): Promise<never>
    doesNotExist(): Promise<never>
}

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
        results.push({ name, pass: Bun.deepEquals(actual, expected, true), actual, expected })
    }

    // Hai transporter mã hoá `undefined` khác nhau: msgpack của ws thành `null`, msgpackr của tcp giữ `undefined`.
    const undefinedOnWire = transport === 'websocket' ? null : undefined

    // --- Giá trị trả về ---
    await check('sync value', () => settle(() => api.syncValue('a')), { value: 'sync:a' })
    await check('async value', () => settle(() => api.asyncValue('a')), { value: 'async:a' })
    await check('returns null', () => settle(() => api.returnsNull()), { value: null })
    await check(`returns undefined (${transport})`, () => settle(() => api.returnsUndefined()), { value: undefinedOnWire })
    await check('no arguments', () => settle(() => api.noArgs()), { value: 'no-args' })
    await check('many arguments', () => settle(() => api.manyArgs(1, 'b', true, null, [1, 2])),
        { value: { a: 1, b: 'b', c: true, d: null, e: [1, 2] } })
    const nested = { a: { b: [1, { c: 'd' }] }, negative: -1.5, maxSafe: Number.MAX_SAFE_INTEGER, empty: {}, list: [] }
    await check('nested object', () => settle(() => api.echo(nested)), { value: nested })
    await check(`undefined field (${transport})`, () => settle(() => api.echo({ a: 1, u: undefined })),
        { value: { a: 1, u: undefinedOnWire } })
    await check('unicode string', () => settle(() => api.echo('Xin chào 👋 — ñ')), { value: 'Xin chào 👋 — ñ' })
    await check('Date', () => settle(() => api.echo(new Date('2026-01-02T03:04:05.000Z'))),
        { value: new Date('2026-01-02T03:04:05.000Z') })
    await check('Uint8Array', () => settle(() => api.echo(new Uint8Array([0, 1, 255]))), { value: new Uint8Array([0, 1, 255]) })
    await check('1 MB payload', async () => (await api.bigPayload(1_000_000)).length, 1_000_000)

    // --- Stream ---
    await check('sync observable', () => record(api.syncObservable()), [['next', 1], ['next', 2], ['next', 3], ['complete']])
    await check('async observable', () => record(api.asyncObservable()), [['next', 'a'], ['next', 'b'], ['complete']])
    await check('empty observable (subscribe)', () => record(api.emptyObservable()), [['complete']])
    await check('empty observable (await)', () => settle(() => api.emptyObservable() as unknown as Promise<never>),
        { error: { message: 'no elements in sequence', name: 'EmptyError' } })
    await check('500 values keep order', async () => {
        const events = await record(api.longStream(500))
        return { count: events.length - 1, ordered: events.slice(0, -1).every((event, index) => event[1] === index), last: events.at(-1) }
    }, { count: 500, ordered: true, last: ['complete'] })
    await check('unsubscribe stops the provider stream', async () => {
        await lastValueFrom(api.endless().pipe(take(3), toArray()))
        await new Promise(resolve => setTimeout(resolve, 500))
        return api.activeStreams()
    }, 0)

    // --- Lỗi ---
    await check('sync throw', () => settle(() => api.syncThrow()), { error: { message: 'sync-error' } })
    await check('async throw', () => settle(() => api.asyncThrow()), { error: { message: 'async-error' } })
    await check('observable error immediately', () => record(api.observableThrowNow()), [['error', null, 'observable-error']])
    await check('observable error after values', () => record(api.streamThenError()),
        [['next', 1], ['next', 2], ['error', null, 'mid-stream-error']])
    await check('async observable error', () => record(api.asyncObservableError()), [['error', null, 'async-observable-error']])
    await check('async observable error after values', () => record(api.asyncObservableThenError()),
        [['next', 'x'], ['error', null, 'async-mid-stream-error']])
    await check('custom code (sync)', () => settle(() => api.syncCodeError()), { error: { code: 'CUSTOM_SYNC', message: 'custom sync' } })
    await check('custom code (async)', () => settle(() => api.asyncCodeError()), { error: { code: 'CUSTOM_ASYNC', message: 'custom async' } })
    await check('custom code (observable)', () => record(api.observableCodeError()), [['error', 'CUSTOM_STREAM', 'custom stream']])
    await check('thrown string', () => settle(() => api.throwString()), { error: { message: 'plain-string' } })
    await check('Error with code', () => settle(() => api.throwErrorWithCode()), { error: { code: 'E_WITH_CODE', message: 'error-with-code' } })
    await check('method not found', async () => (await settle(() => api.doesNotExist())).error?.code, 'MICROSERVICE_NOT_FOUND')
    await check('service not found', async () => {
        const missing = RemoteServiceLinker.link<{ run(): Promise<void> }>(mesh, { service: 'NoSuchService' })
        return (await settle(() => missing.run())).error?.code
    }, 'MICROSERVICE_OFFLINE')
    await check('timeout', async () => {
        const hurried = RemoteServiceLinker.link<ContractApi>(mesh, { service: 'ContractService', timeout: 300 })
        return (await settle(() => hurried.slow(3_000))).error?.code
    }, 'MICROSERVICE_RPC_TIMEOUT')

    // --- Dùng như Promise và gọi song song ---
    await check('then chains the value', () => api.asyncValue('p').then(value => `${value}!`), 'async:p!')
    await check('catch receives the error', () => api.asyncCodeError().catch((error: { code: string }) => error.code), 'CUSTOM_ASYNC')
    await check('50 concurrent calls', async () => {
        const values = await Promise.all(Array.from({ length: 50 }, (_, index) => api.asyncValue(String(index))))
        return values.every((value, index) => value === `async:${index}`)
    }, true)

    return results
}
