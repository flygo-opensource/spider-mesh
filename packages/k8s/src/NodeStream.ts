import { request, type ClientRequest } from 'node:http'
import type { SpiderMeshNode } from '@spider-mesh/core'

const RETRY_MIN_MS = 250
const RETRY_MAX_MS = 10_000
const CONNECT_TIMEOUT_MS = 3000

export type NodeStreamHandlers = {
    onNode(node: SpiderMeshNode): void
}

const isNode = (value: unknown): value is SpiderMeshNode => {
    const node = value as Partial<SpiderMeshNode> | null
    return !!node
        && typeof node.node_id === 'string' && node.node_id.length > 0
        && typeof node.version === 'number'
        && typeof node.services === 'object' && node.services !== null
        && typeof node.transporters === 'object' && node.transporters !== null
}

/**
 * Kéo `GET /node` của một pod và giữ stream mở: mỗi dòng là một snapshot `SpiderMeshNode` mới. Stream
 * đứt thì nối lại với thời gian chờ tăng dần, cho tới khi `close()`: pod còn trong membership nghĩa là
 * Kubernetes vẫn coi nó ready, nên đứt stream không phải lý do để xoá node.
 */
export class NodeStream {

    /** Node id học được từ snapshot đầu tiên; dùng để `removeRemote` khi pod rời membership. */
    node_id?: string
    /** Pod này chính là process hiện tại. */
    self = false

    #closed = false
    #request?: ClientRequest
    #idleTimer?: ReturnType<typeof setTimeout>
    #wake = () => {}

    constructor(
        readonly target: string,
        private readonly handlers: NodeStreamHandlers,
        private readonly idleTimeoutMs: number,
    ) {
        void this.#run()
    }

    close() {
        this.#closed = true
        clearTimeout(this.#idleTimer)
        this.#request?.destroy()
        this.#wake()
    }

    async #run() {
        let delay = RETRY_MIN_MS
        while (!this.#closed) {
            const received = await this.#pull().catch(() => false)
            if (this.#closed) return
            if (received) delay = RETRY_MIN_MS
            await new Promise<void>(resolve => {
                const timer = setTimeout(resolve, delay)
                this.#wake = () => {
                    clearTimeout(timer)
                    resolve()
                }
            })
            delay = Math.min(delay * 2, RETRY_MAX_MS)
        }
    }

    /** Một lần kéo; resolve khi stream kết thúc, `true` nếu đã nhận được ít nhất một snapshot. */
    #pull() {
        return new Promise<boolean>((resolve, reject) => {
            let received = false
            const connectTimer = setTimeout(() => req.destroy(new Error('connect timeout')), CONNECT_TIMEOUT_MS)
            const armIdle = () => {
                clearTimeout(this.#idleTimer)
                this.#idleTimer = setTimeout(() => req.destroy(new Error('idle timeout')), this.idleTimeoutMs)
                this.#idleTimer.unref?.()
            }

            const req = request(`http://${this.target}/node`, { headers: { accept: 'application/x-ndjson' } }, response => {
                clearTimeout(connectTimer)
                if (response.statusCode !== 200) {
                    response.resume()
                    reject(new Error(`HTTP ${response.statusCode}`))
                    return
                }

                armIdle()
                let buffer = ''
                response.setEncoding('utf8')
                response.on('data', chunk => {
                    armIdle()
                    buffer += chunk
                    let newline: number
                    while ((newline = buffer.indexOf('\n')) >= 0) {
                        const line = buffer.slice(0, newline).trim()
                        buffer = buffer.slice(newline + 1)
                        if (!line) continue // heartbeat
                        let node: unknown
                        try {
                            node = JSON.parse(line)
                        } catch {
                            continue
                        }
                        if (!isNode(node) || this.#closed) continue
                        received = true
                        this.handlers.onNode(node)
                    }
                })
                const done = () => {
                    clearTimeout(this.#idleTimer)
                    resolve(received)
                }
                response.once('end', done)
                response.once('close', done)
                response.once('error', done)
            })
            req.once('error', error => {
                clearTimeout(connectTimer)
                clearTimeout(this.#idleTimer)
                if (received) resolve(true)
                else reject(error)
            })
            req.end()
            this.#request = req
        })
    }
}
