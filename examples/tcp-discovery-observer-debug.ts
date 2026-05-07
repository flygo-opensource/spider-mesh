import process from 'node:process'
import { firstValueFrom, timeout as rxTimeout, type Observable } from 'rxjs'
import { RemoteServiceLinker } from '@spider-mesh/core'
import { createMesh } from './helpers/createMesh.js'

type ServiceA = { ping(): Observable<string> }
type ServiceB = { ping(): Observable<string> }

const { mesh } = createMesh()
console.log('TCP discovery observer ready')

const linkerA = RemoteServiceLinker.link<ServiceA>(mesh, { service: 'ServiceA', timeout: 5000, retry: 0 })
const linkerB = RemoteServiceLinker.link<ServiceB>(mesh, { service: 'ServiceB', timeout: 5000, retry: 0 })

const hasValidPort = (n: any) => {
    const m = n.transporters['Http2Rpc']
    return m && typeof m === 'object' && typeof m.port === 'number' && m.port > 0
}

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

await linkerA.wait(() => {
    const nodes = mesh.listRpcNodes('ServiceA')
    const ready = nodes.length >= 2 && nodes.every(hasValidPort)
    if (nodes.length > 0) {
        process.stderr.write('wait check: ' + JSON.stringify(nodes.map(n => ({ id: n.node_id.slice(-4), port: n.transporters['Http2Rpc']?.port, valid: hasValidPort(n) }))) + '\n')
    }
    return ready
})

const nodes = mesh.listRpcNodes('ServiceA')
process.stderr.write('warmup nodes: ' + JSON.stringify(nodes.map(n => ({ id: n.node_id.slice(-4), port: n.transporters['Http2Rpc']?.port }))) + '\n')

for (const node of nodes) {
    try {
        const r = await firstValueFrom(
            mesh.callRemoteService<string, never>({
                service: 'ServiceA', method: 'ping', args: [],
                node_id: node.node_id, timeout: 5000, retry: 0,
            }).pipe(rxTimeout(5000))
        )
        process.stderr.write('ping OK: ' + node.node_id.slice(-4) + ' → ' + r + '\n')
    } catch(e: any) {
        process.stderr.write('ping FAIL: ' + node.node_id.slice(-4) + ' → ' + (e.code || e.message) + '\n')
    }
}
process.exit(0)
