// Bộ hợp đồng RPC dùng chung cho mọi transporter. File này giống hệt nhau ở @spider-mesh/ws và
// @spider-mesh/tcp; sửa ở một nơi thì chép sang nơi kia.
import { Microservice } from '@spider-mesh/core'
import { concat, EMPTY, interval, map, Observable, of, take, throwError, timer } from 'rxjs'

@Microservice()
export class ContractService {
    #activeStreams = 0

    // Giá trị trả về
    syncValue(input: string) { return `sync:${input}` }
    async asyncValue(input: string) { return `async:${input}` }
    returnsNull() { return null }
    returnsUndefined() { return undefined }
    noArgs() { return 'no-args' }
    manyArgs(a: number, b: string, c: boolean, d: null, e: number[]) { return { a, b, c, d, e } }
    echo<T>(value: T) { return value }
    bigPayload(size: number) { return 'x'.repeat(size) }
    async slow(ms: number) {
        await new Promise(resolve => setTimeout(resolve, ms))
        return 'slow-done'
    }

    // Stream
    syncObservable() { return of(1, 2, 3) }
    async asyncObservable() { return of('a', 'b') }
    emptyObservable() { return EMPTY }
    longStream(count: number) { return interval(1).pipe(take(count)) }
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

    // Lỗi
    syncThrow() { throw new Error('sync-error') }
    async asyncThrow() { throw new Error('async-error') }
    observableThrowNow() { return throwError(() => new Error('observable-error')) }
    streamThenError() {
        return concat(of(1, 2), timer(20).pipe(map(() => { throw new Error('mid-stream-error') })))
    }
    async asyncObservableError() { return throwError(() => new Error('async-observable-error')) }
    async asyncObservableThenError() {
        return concat(of('x'), timer(20).pipe(map(() => { throw new Error('async-mid-stream-error') })))
    }
    syncCodeError() { throw { code: 'CUSTOM_SYNC', message: 'custom sync' } }
    async asyncCodeError() { throw { code: 'CUSTOM_ASYNC', message: 'custom async' } }
    observableCodeError() { return throwError(() => ({ code: 'CUSTOM_STREAM', message: 'custom stream' })) }
    throwString() { throw 'plain-string' }
    throwErrorWithCode() {
        const error = new Error('error-with-code') as Error & { code: string }
        error.code = 'E_WITH_CODE'
        throw error
    }
}
