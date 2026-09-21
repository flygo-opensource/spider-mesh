import { expect, setDefaultTimeout, test } from 'bun:test'
import { runBunScript } from './helpers/runBunScript.js'

setDefaultTimeout(40000)

// RPC Timeout / Hết thời gian RPC
// Input:  Client gọi một method không bao giờ trả lời, timeout: 2000ms
// Output: Nhận lỗi với code = "MICROSERVICE_RPC_TIMEOUT"
test('websocket rpc timeout e2e', async () => {
    const result = await runBunScript(['run', 'examples/websocket-timeout-test.ts'], 35000)

    expect(result.code).toBe(0)
    expect(result.stderr).toBe('')
    expect(result.stdout).toContain('"timeoutDetected":true')
    expect(result.stdout).toContain('"code":"MICROSERVICE_RPC_TIMEOUT"')
})

// Fallback value / Giá trị dự phòng
// Input:  Client gọi service không tồn tại, fallback: "fallback-response", retry: 0
// Output: Nhận được giá trị fallback thay vì throw error
test('websocket rpc fallback e2e', async () => {
    const result = await runBunScript(['run', 'examples/websocket-fallback-test.ts'], 25000)

    expect(result.code).toBe(0)
    expect(result.stderr).toBe('')
    expect(result.stdout).toContain('"fallbackReceived":true')
    expect(result.stdout).toContain('"value":"fallback-response"')
})

// Provider disconnect / offline detection / Phát hiện provider ngắt kết nối
// Input:  Client gọi thành công, sau đó provider bị kill, relay broadcast offline, client gọi lại
// Output: Lần 1 thành công; lần 2 nhận MICROSERVICE_OFFLINE
test('websocket provider offline detection e2e', async () => {
    const result = await runBunScript(['run', 'examples/websocket-offline-test.ts'], 35000)

    expect(result.code).toBe(0)
    expect(result.stderr).toBe('')
    expect(result.stdout).toContain('"firstCallOk":true')
    expect(result.stdout).toContain('"offlineDetected":true')
    expect(result.stdout).toContain('"code":"MICROSERVICE_OFFLINE"')
})

// Concurrent RPC / Gọi đồng thời
// Input:  10 RPC calls song song qua WebSocket relay
// Output: Tất cả 10 kết quả đúng, không mất response nào
test('websocket concurrent rpc e2e', async () => {
    const result = await runBunScript(['run', 'examples/websocket-concurrent-test.ts'], 35000)

    expect(result.code).toBe(0)
    expect(result.stderr).toBe('')
    expect(result.stdout).toContain('"concurrentOk":true')
    expect(result.stdout).toContain('"count":10')
    expect(result.stdout).toContain('"allCorrect":true')
})

// Provider reconnect / Kết nối lại sau khi ngắt
// Input:  provider-v1 kết nối → client gọi → provider-v1 bị kill → provider-v2 kết nối lại → client gọi lại
// Output: Cả 2 lần gọi thành công; relay phát hiện offline/online tự động
test('websocket provider reconnect e2e', async () => {
    const result = await runBunScript(['run', 'examples/websocket-reconnect-test.ts'], 38000)

    expect(result.code).toBe(0)
    expect(result.stderr).toBe('')
    expect(result.stdout).toContain('"firstCallOk":true')
    expect(result.stdout).toContain('"providerOffline":true')
    expect(result.stdout).toContain('"reconnectOk":true')
})

// Failover: 3 nodes → kill 1 → 2 nodes still serve / Dự phòng: 3 node → kill 1 → 2 node còn lại tiếp tục phục vụ
// Input:  3 provider cùng online; gửi 6 request (phase 1); kill provider-c; gửi 4 request (phase 2)
// Output: Phase 1 — cả 3 provider đều nhận request; Phase 2 — chỉ 2 provider còn lại, không có lỗi
test('websocket failover: 3 nodes then 1 offline e2e', async () => {
    const result = await runBunScript(['run', 'examples/websocket-failover-test.ts'], 45000)

    expect(result.code).toBe(0)
    expect(result.stderr).toBe('')

    const phase1Line = result.stdout.split('\n').find(l => l.includes('"phase1UniqueProviders"'))
    expect(phase1Line).toBeDefined()
    const phase1 = JSON.parse(phase1Line!) as { phase1: string[], phase1UniqueProviders: number }
    expect(phase1.phase1).toHaveLength(6)
    expect(phase1.phase1UniqueProviders).toBe(3)

    const phase2Line = result.stdout.split('\n').find(l => l.includes('"phase2UniqueProviders"'))
    expect(phase2Line).toBeDefined()
    const phase2 = JSON.parse(phase2Line!) as { phase2: string[], phase2UniqueProviders: number }
    expect(phase2.phase2).toHaveLength(4)
    expect(phase2.phase2UniqueProviders).toBe(2)
    expect(phase2.phase2.every((r: string) => !r.includes('provider-c'))).toBe(true)
})
