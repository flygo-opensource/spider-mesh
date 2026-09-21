import { Microservice } from '@spider-mesh/core'
import { Observable, concat, of, throwError, delay } from './helpers/coreRxjs.js'
import { createMesh } from './helpers/createMesh.js'

@Microservice({ role: 'provider', mode: 'tcp-matrix-e2e' })
class RpcMatrixService {
    syncValue(name: string) {
        return `sync:${name}`
    }

    async asyncValue(name: string) {
        return `async:${name}`
    }

    syncObservable(name: string): Observable<string> {
        return concat(
            of(`sync-observable:${name}:1`),
            of(`sync-observable:${name}:2`).pipe(delay(10)),
        )
    }

    async asyncObservable(name: string) {
        return concat(
            of(`async-observable:${name}:1`).pipe(delay(10)),
            of(`async-observable:${name}:2`).pipe(delay(10)),
        )
    }

    syncError() {
        throw new Error('sync-error')
    }

    async asyncError() {
        throw new Error('async-error')
    }

    observableError() {
        return throwError(() => new Error('observable-error'))
    }
}

new RpcMatrixService()
createMesh()

console.log('TCP matrix e2e provider ready')

setInterval(() => undefined, 1000)