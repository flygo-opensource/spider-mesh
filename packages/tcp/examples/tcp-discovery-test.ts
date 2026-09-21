import { createTcpTestEnv, start, stopAll, waitForOutput } from './helpers/e2eHarness.js'

async function main() {
    const env = createTcpTestEnv()

    // Observer khởi động trước để nhận tất cả discovery events
    const observer = start('observer', 'examples/tcp-discovery-observer.ts', env)
    await waitForOutput(observer, /TCP discovery observer ready/, 5000, 'observer')

    // Tiến trình 1: chỉ có ServiceA
    const p1 = start('provider-1', 'examples/tcp-discovery-provider.ts', {
        ...env,
        SERVICES: 'a',
    })
    await waitForOutput(p1, /TCP discovery provider ready/, 5000, 'provider-1')

    // Tiến trình 2: có cả ServiceA và ServiceB
    const p2 = start('provider-2', 'examples/tcp-discovery-provider.ts', {
        ...env,
        SERVICES: 'a,b',
    })
    await waitForOutput(p2, /TCP discovery provider ready/, 5000, 'provider-2')

    // Milestone 1: cả 2 tiến trình online → ServiceA: 2, ServiceB: 1
    const bothOnline = await waitForOutput(observer, /"serviceA":2,"serviceB":1/, 15000, 'observer')
    console.log(bothOnline.trim())

    // Kill tiến trình 1 (ServiceA only) → ServiceA giảm còn 1, ServiceB giữ nguyên
    p1.kill('SIGTERM')

    // Milestone 2: ServiceA: 1, ServiceB: 1
    const p1Offline = await waitForOutput(observer, /"serviceA":1,"serviceB":1/, 15000, 'observer')
    console.log(p1Offline.trim())

    // Kill tiến trình 2 (ServiceA + ServiceB) → cả 2 về 0
    p2.kill('SIGTERM')

    // Milestone 3: ServiceA: 0, ServiceB: 0
    const p2Offline = await waitForOutput(observer, /"serviceA":0,"serviceB":0/, 15000, 'observer')
    console.log(p2Offline.trim())
}

try {
    await main()
} finally {
    await stopAll()
}
