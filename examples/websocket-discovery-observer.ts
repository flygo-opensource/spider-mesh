import process from 'node:process'
import { createMesh } from './helpers/createMesh.js'

const wsUrl = process.env.WS_URL || 'ws://127.0.0.1:8787'
const { mesh } = createMesh({ wsUrl, heartbeatIntervalMs: 1000, reconnectIntervalMs: 500 })
console.log('WebSocket discovery observer ready')

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

setTimeout(() => {
    console.error('WebSocket discovery observer guard exceeded')
    process.exit(1)
}, 45000)

setInterval(() => undefined, 1000)
