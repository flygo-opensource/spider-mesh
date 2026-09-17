import { Observable, firstValueFrom, lastValueFrom, timeout, toArray } from 'rxjs'
import { RemoteServiceLinker } from '@spider-mesh/core'
import { createMesh } from './helpers/createMesh.js'

const wsUrl = process.env.WS_URL || 'ws://127.0.0.1:8787'
console.log('WebSocket matrix e2e client connected')

type RpcMatrixService = {
    syncValue(name: string): Promise<string>
    asyncValue(name: string): Promise<string>
    syncObservable(name: string): Observable<string>
    asyncObservable(name: string): Observable<string>
    syncError(): Promise<never>
    asyncError(): Promise<never>
    observableError(): Observable<never>
}

async function expectError(label: string, run: () => unknown, expectedMessage: string) {
    try {
        const result = run()

        if (result instanceof Observable) {
            await firstValueFrom(result.pipe(timeout(5000)))
        } else {
            await result
        }

        throw new Error(`${label} did not fail`)
    } catch (error) {
        const message = typeof error === 'object' && error && 'message' in error ? String((error as { message: string }).message) : String(error)
        if (message !== expectedMessage) {
            throw new Error(`${label} expected ${expectedMessage} but got ${message}`)
        }
    }
}

async function main() {
    const { mesh } = createMesh({
        wsUrl,
        heartbeatIntervalMs: 1000,
        reconnectIntervalMs: 500,
    })
    const service = RemoteServiceLinker.link<RpcMatrixService>(mesh, {
        service: 'RpcMatrixService',
        timeout: 3000,
        retry: 2,
    })

    const guard = setTimeout(() => {
        console.error('WebSocket matrix e2e client timed out')
        process.exit(1)
    }, 15000)

    try {
        await service.wait()

        const syncValue = await service.syncValue('case-sync')
        const asyncValue = await service.asyncValue('case-async')
        const syncObservable = await lastValueFrom(service.syncObservable('case-sync-observable').pipe(timeout(5000), toArray()))
        const asyncObservable = await lastValueFrom(service.asyncObservable('case-async-observable').pipe(timeout(5000), toArray()))

        await expectError('syncError', () => service.syncError(), 'sync-error')
        await expectError('asyncError', () => service.asyncError(), 'async-error')
        await expectError('observableError', () => firstValueFrom(service.observableError().pipe(timeout(5000))), 'observable-error')

        console.log(JSON.stringify({
            syncValue,
            asyncValue,
            syncObservable,
            asyncObservable,
            errors: ['sync-error', 'async-error', 'observable-error']
        }))

        process.exit(0)
    } catch (error) {
        console.error(error)
        process.exit(1)
    } finally {
        clearTimeout(guard)
    }
}

await main()
