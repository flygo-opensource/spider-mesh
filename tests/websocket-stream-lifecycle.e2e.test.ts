import { expect, setDefaultTimeout, test } from 'bun:test'
import { runBunScript } from './helpers/runBunScript.js'

setDefaultTimeout(60000)

// Provider offline giữa lúc stream đang chạy / Provider dies mid-stream
// Input:  Client subscribe một RPC trả Observable dài hạn, KHÔNG đặt timeout; provider bị SIGKILL
// Output: Stream error với code = "MICROSERVICE_OFFLINE" trong < 1s kể từ tick cuối cùng
test('websocket in-flight stream errors when the provider goes offline', async () => {
    const result = await runBunScript(['run', 'examples/websocket-stream-offline-test.ts'], 45000)

    expect(result.code).toBe(0)
    expect(result.stderr).toBe('')

    const line = result.stdout.split('\n').find(l => l.includes('"streamErrored"'))
    expect(line).toBeDefined()

    const report = JSON.parse(line!) as {
        streamErrored: boolean
        code: string
        received: number
        sinceLastTickMs: number
    }

    expect(report.streamErrored).toBe(true)
    expect(report.code).toBe('MICROSERVICE_OFFLINE')
    // Stream phải thực sự chạy trước khi provider chết, nếu không thì test không chứng minh gì.
    expect(report.received).toBeGreaterThan(0)
    // Không có timeout nào ở caller: dưới 1s nghĩa là tín hiệu offline đã đóng stream.
    expect(report.sinceLastTickMs).toBeLessThan(1000)
})

// Cancel race / Unsubscribe trước khi send() resolve
// Input:  Client subscribe rồi unsubscribe ngay trong cùng một tick
// Output: Provider không còn stream nào chạy sau 2s (cancel vẫn được gửi bù)
test('websocket cancel is still sent when unsubscribe wins the send race', async () => {
    const result = await runBunScript(['run', 'examples/websocket-stream-cancel-test.ts'], 45000)

    expect(result.code).toBe(0)
    expect(result.stderr).toBe('')

    const line = result.stdout.split('\n').find(l => l.includes('"cancelRaceChecked"'))
    expect(line).toBeDefined()

    const report = JSON.parse(line!) as { cancelRaceChecked: boolean, activeStreams: number }
    expect(report.cancelRaceChecked).toBe(true)
    expect(report.activeStreams).toBe(0)
})

// Caller crash / Caller chết khi stream đang chạy
// Input:  Caller subscribe stream dài hạn rồi bị SIGKILL, không kịp gửi cancel
// Output: Relay đóng stream hộ; provider báo activeStreams = 0
test('websocket provider stream stops when the caller disappears', async () => {
    const result = await runBunScript(['run', 'examples/websocket-caller-crash-test.ts'], 50000)

    expect(result.code).toBe(0)
    expect(result.stderr).toBe('')

    const line = result.stdout.split('\n').find(l => l.includes('"activeStreamsChecked"'))
    expect(line).toBeDefined()

    const report = JSON.parse(line!) as { activeStreamsChecked: boolean, activeStreams: number }
    expect(report.activeStreamsChecked).toBe(true)
    expect(report.activeStreams).toBe(0)
})

type RelayLossReport = { relayLoss: string, detectedAfterMs: number, code: string }

async function runRelayLoss(mode: 'stream' | 'unary') {
    const result = await runBunScript(['run', 'examples/websocket-relay-loss-test.ts', mode], 45000)
    expect(result.code).toBe(0)
    expect(result.stderr).toBe('')

    const line = result.stdout.split('\n').find(l => l.includes('"relayLoss"'))
    expect(line).toBeDefined()
    return JSON.parse(line!) as RelayLossReport
}

// Relay chết giữa stream / Relay dies mid-stream
// Input:  Client subscribe stream dài hạn, KHÔNG đặt timeout; relay bị SIGKILL
// Output: Stream error MICROSERVICE_OFFLINE trong < 1s — relay không thể tự báo, transporter phải báo
test('websocket in-flight stream errors when the relay connection drops', async () => {
    const report = await runRelayLoss('stream')

    expect(report.relayLoss).toBe('stream')
    expect(report.code).toBe('MICROSERVICE_OFFLINE')
    expect(report.detectedAfterMs).toBeLessThan(1000)
})

// Relay chết khi unary đang chờ / Relay dies while a unary call is pending
// Input:  Client await một method không bao giờ trả lời, KHÔNG đặt timeout; relay bị SIGKILL
// Output: Promise reject MICROSERVICE_OFFLINE trong < 1s thay vì treo vô hạn
test('websocket pending unary rpc rejects when the relay connection drops', async () => {
    const report = await runRelayLoss('unary')

    expect(report.relayLoss).toBe('unary')
    expect(report.code).toBe('MICROSERVICE_OFFLINE')
    expect(report.detectedAfterMs).toBeLessThan(1000)
})
