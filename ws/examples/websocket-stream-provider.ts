import { Observable } from 'rxjs'
import { Microservice } from '@spider-mesh/core'
import { createMesh } from './helpers/createMesh.js'

const wsUrl = process.env.WS_URL || 'ws://127.0.0.1:8787'
const providerId = process.env.PROVIDER_ID || 'stream-provider'
const tickIntervalMs = Number(process.env.TICK_INTERVAL_MS || 100)

@Microservice({ role: 'provider', mode: 'stream-e2e' })
class TickService {
    #active = 0

    /**
     * Stream dài hạn, không bao giờ complete. Teardown đếm ngược `#active` nên caller có thể
     * hỏi lại provider xem stream đã thực sự bị đóng hay còn rò rỉ.
     */
    ticks(): Observable<string> {
        return new Observable<string>(subscriber => {
            this.#active++
            let index = 0
            const timer = setInterval(() => subscriber.next(`${providerId}:${index++}`), tickIntervalMs)

            return () => {
                clearInterval(timer)
                this.#active--
            }
        })
    }

    /** Số stream `ticks()` đang còn chạy ở phía provider. */
    activeStreams() {
        return this.#active
    }
}

new TickService()
createMesh({
    wsUrl,
    heartbeatIntervalMs: 1000,
    reconnectIntervalMs: 500,
})

console.log(`WebSocket stream provider ready at ${wsUrl} (${providerId})`)

setInterval(() => undefined, 1000)
