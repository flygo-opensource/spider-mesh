import { expect, test } from 'bun:test'
import { Subject } from 'rxjs'
import { RemoteServiceLinker, SpiderMesh, type RpcEvent, type RpcTransporter, type SpiderMeshError } from '../src/index.js'

// Transporter tối thiểu: có tên, không route được tới đâu.
class NamedTransporter extends Subject<RpcEvent> implements RpcTransporter {
    readonly name = 'websocket'
    async send() { return { cancel: () => {} } }
    canRoute() { return false }
}

const call = (transporter: unknown) => {
    const mesh = new SpiderMesh({ transporters: [new NamedTransporter()] })
    const service = RemoteServiceLinker.link<{ ping(): string }>(mesh, {
        service: 'PingService',
        transporter: transporter as string,
    })
    return Promise.resolve(service.ping()).then(() => undefined, (error: SpiderMeshError) => error)
}

// Bẫy khi nâng từ 2.x: truyền class transporter thay vì tên. Trước đây chỉ báo chung
// "No transporter available", không cho biết option nào sai.
test('passing a transporter class explains that a name is expected', async () => {
    const error = await call(NamedTransporter)
    expect(error?.code).toBe('MICROSERVICE_OFFLINE')
    expect(error?.message).toContain('must be the name of a registered transporter')
    expect(error?.message).toContain("'websocket'")
    expect(error?.message).toContain('class NamedTransporter')
})

test('an unregistered transporter name is reported with the registered names', async () => {
    const error = await call('http2')
    expect(error?.message).toContain('"http2"')
    expect(error?.message).toContain("'websocket'")
})

test('a registered name keeps the usual offline error', async () => {
    const error = await call('websocket')
    expect(error?.code).toBe('MICROSERVICE_OFFLINE')
    expect(error?.message).toBe('No transporter available for service PingService')
})
