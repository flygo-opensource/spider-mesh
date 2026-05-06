import { Observable, concat, of, throwError } from 'rxjs'
import { delay } from 'rxjs/operators'
import { Microservice } from '@spider-mesh/core'
import { createMesh } from './helpers/createMesh.js'

const wsUrl = process.env.WS_URL || 'ws://127.0.0.1:8787'

@Microservice({ role: 'provider', mode: 'matrix-e2e' })
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
createMesh({
    wsUrl,
    heartbeatIntervalMs: 1000,
    reconnectIntervalMs: 500,
})

console.log(`WebSocket matrix e2e provider ready at ${wsUrl}`)

setInterval(() => undefined, 1000)