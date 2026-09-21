import { expect, test } from 'bun:test'
import { runInNewContext } from 'node:vm'

// Trình duyệt và React Native không có `process`. Core đọc biến môi trường ngay khi import, nên nếu
// không kiểm tra `process` trước thì chỉ cần import là ứng dụng chết với ReferenceError.
test('core loads and starts a SpiderMesh where `process` does not exist', async () => {
    const build = await Bun.build({
        entrypoints: [new URL('./fixtures/browser-entry.ts', import.meta.url).pathname],
        target: 'browser',
        format: 'iife',
    })
    expect(build.success).toBe(true)
    const bundle = await build.outputs[0].text()

    // Chỉ những gì một trình duyệt có; cố ý không có `process`, `Buffer`, `require`.
    const sandbox: Record<string, unknown> = {
        console, setTimeout, clearTimeout, setInterval, clearInterval, queueMicrotask,
        TextEncoder, TextDecoder, Promise, Symbol, Reflect, Date, Math, JSON, crypto,
    }
    sandbox.globalThis = sandbox
    sandbox.window = sandbox

    expect(() => runInNewContext(bundle, sandbox)).not.toThrow()
    expect('process' in sandbox).toBe(false)
    // Không có biến môi trường thì dùng giá trị mặc định.
    expect(sandbox.__browserMesh).toMatchObject({ namespace: 'default' })
    expect(typeof (sandbox.__browserMesh as { node_id: string }).node_id).toBe('string')
})
