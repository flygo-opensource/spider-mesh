import process from 'node:process'
import { firstValueFrom, timeout as rxTimeout, type Observable } from 'rxjs'
import { RemoteServiceLinker } from '@spider-mesh/core'
import { createMesh } from './helpers/createMesh.js'

type ServiceA = { ping(): Observable<string> }
type ServiceB = { ping(): Observable<string> }

const { mesh } = createMesh()
console.log('TCP discovery observer ready')

const linkerA = RemoteServiceLinker.link<ServiceA>(mesh, { service: 'ServiceA', timeout: 3000, retry: 0 })
const linkerB = RemoteServiceLinker.link<ServiceB>(mesh, { service: 'ServiceB', timeout: 3000, retry: 0 })

// Track count per service — print on every change
const state = { serviceA: 0, serviceB: 0 }
const emit = () => console.log(JSON.stringify({ ...state }))

mesh.watchService('ServiceA').subscribe(nodes => {
    if (state.serviceA === nodes.length) return
    state.serviceA = nodes.length
    emit()
})

mesh.watchService('ServiceB').subscribe(nodes => {
    if (state.serviceB === nodes.length) return
    state.serviceB = nodes.length
    emit()
})

// Gọi RPC định kỳ để kiểm tra end-to-end và tạo socket traffic. Http2Rpc không có active ping;
// silent partition được phát hiện khi traffic lỗi hoặc OS đóng session.
const runHealthChecks = async () => {
    const aNodes = mesh.listRpcNodes('ServiceA')
    const bNodes = mesh.listRpcNodes('ServiceB')

    await Promise.allSettled([
        ...aNodes.map(n =>
            firstValueFrom(
                mesh.callRemoteService<string, never>({
                    service: 'ServiceA', method: 'ping', args: [],
                    node_id: n.node_id, timeout: 2000, retry: 0,
                }).pipe(rxTimeout(2000))
            )
        ),
        ...bNodes.map(n =>
            firstValueFrom(
                mesh.callRemoteService<string, never>({
                    service: 'ServiceB', method: 'ping', args: [],
                    node_id: n.node_id, timeout: 2000, retry: 0,
                }).pipe(rxTimeout(2000))
            )
        ),
    ])
}

// Bắt đầu application-level probe ngay — lần đầu có thể fail khi providers chưa online.
const healthInterval = setInterval(runHealthChecks, 600)

setTimeout(() => {
    clearInterval(healthInterval)
    console.error('TCP discovery observer guard exceeded')
    process.exit(1)
}, 45000)
