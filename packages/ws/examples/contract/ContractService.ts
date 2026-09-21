// Bộ hợp đồng RPC dùng chung cho mọi transporter. File này giống hệt nhau ở @spider-mesh/ws và
// @spider-mesh/tcp; sửa ở một nơi thì chép sang nơi kia.
import { Microservice } from '@spider-mesh/core'
import { concat, EMPTY, interval, map, Observable, of, take, throwError, timer } from 'rxjs'

/** Đường lỗi đi ra khỏi method. */
export type FailurePath = 'sync-throw' | 'async-throw' | 'observable' | 'observable-after-values'
/** Loại giá trị bị ném. */
export type FailureKind = 'error' | 'code-object' | 'string' | 'error-with-code' | 'error-subclass'

class DomainError extends Error {
    code = 'E_SUBCLASS'
    constructor(message: string) {
        super(message)
        this.name = 'DomainError'
    }
}

const failure = (kind: FailureKind): unknown => {
    switch (kind) {
        case 'error': return new Error('plain error')
        case 'code-object': return { code: 'E_OBJECT', message: 'code object' }
        case 'string': return 'thrown string'
        case 'error-with-code': return Object.assign(new Error('error with code'), { code: 'E_WITH_CODE' })
        case 'error-subclass': return new DomainError('error subclass')
    }
}

const delayedError = (error: unknown) => timer(10).pipe(map(() => { throw error }))

@Microservice()
export class ContractService {
    #activeStreams = 0

    // ---------- Ma trận dạng method: thành công ----------
    valueSync() { return 'value' }
    async valueAsync() { return 'value' }
    streamSync() { return of(1, 2, 3) }
    streamSyncDelayed() { return interval(5).pipe(take(3), map(index => index + 1)) }
    async streamAsync() { return of(1, 2, 3) }
    async streamAsyncDelayed() { return interval(5).pipe(take(3), map(index => index + 1)) }
    streamEmpty() { return EMPTY }
    async streamAsyncEmpty() { return EMPTY }

    // ---------- Ma trận dạng method: lỗi ----------
    throwSync(): string { throw new Error('sync error') }
    async throwAsync(): Promise<string> { throw new Error('async error') }
    streamThrowsBeforeReturn(): Observable<number> { throw new Error('stream method threw') }
    async streamAsyncRejects(): Promise<Observable<number>> { throw new Error('async stream method rejected') }
    streamErrorNow() { return throwError(() => new Error('stream error now')) }
    streamErrorAfterValues() { return concat(of(1, 2), delayedError(new Error('stream error after values'))) }
    streamErrorInOperator() {
        return of(1, 2, 3).pipe(map(value => {
            if (value === 2) throw new Error('operator error')
            return value
        }))
    }
    async streamAsyncErrorNow() { return throwError(() => new Error('async stream error now')) }
    async streamAsyncErrorAfterValues() { return concat(of(1, 2), delayedError(new Error('async stream error after values'))) }

    // ---------- Ma trận loại lỗi × đường lỗi ----------
    fail(path: FailurePath, kind: FailureKind): unknown {
        const error = failure(kind)
        switch (path) {
            case 'sync-throw': throw error
            case 'async-throw': return Promise.reject(error)
            case 'observable': return throwError(() => error)
            case 'observable-after-values': return concat(of('before'), delayedError(error))
        }
    }

    // ---------- Giá trị và kiểu dữ liệu ----------
    returnsNull() { return null }
    returnsUndefined() { return undefined }
    noArgs() { return 'no-args' }
    manyArgs(a: number, b: string, c: boolean, d: null, e: number[]) { return { a, b, c, d, e } }
    echo<T>(value: T) { return value }
    bigPayload(size: number) { return 'x'.repeat(size) }
    longStream(count: number) { return interval(1).pipe(take(count)) }
    async slow(ms: number) {
        await new Promise(resolve => setTimeout(resolve, ms))
        return 'slow-done'
    }

    // ---------- Huỷ stream ----------
    endless(): Observable<number> {
        return new Observable<number>(subscriber => {
            this.#activeStreams++
            let index = 0
            const timer = setInterval(() => subscriber.next(index++), 20)
            return () => {
                clearInterval(timer)
                this.#activeStreams--
            }
        })
    }
    activeStreams() { return this.#activeStreams }
}
