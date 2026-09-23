import { existsSync, readFileSync } from 'node:fs'
import { request as httpRequest, type ClientRequest, type IncomingMessage } from 'node:http'
import { request as httpsRequest } from 'node:https'
import { resolve4, resolve6 } from 'node:dns/promises'
import { Observable } from 'rxjs'
import { SERVICE_ACCOUNT_DIR } from './const.js'

/**
 * Tập địa chỉ `host:port` của các pod đang ready. `null` nghĩa là tạm thời không biết (mất kết nối tới
 * API server, DNS lỗi): bên nhận giữ nguyên membership cũ, không được coi là "không còn pod nào".
 */
export type MembershipSnapshot = ReadonlySet<string> | null

/** `host:port`, bọc IPv6 trong ngoặc vuông. */
export const formatTarget = (host: string, port: number) => `${host.includes(':') ? `[${host}]` : host}:${port}`

/** Phần host của một target do `formatTarget` tạo, đã bỏ ngoặc vuông của IPv6. */
export const targetHost = (target: string) => target.startsWith('[')
    ? target.slice(1, target.indexOf(']'))
    : target.slice(0, target.lastIndexOf(':'))

/** API server từ chối (401/403) hoặc không có EndpointSlice API (404): lý do để rơi về DNS. */
export class KubernetesApiUnavailableError extends Error {
    constructor(message: string, readonly status?: number) {
        super(message)
        this.name = 'KubernetesApiUnavailableError'
    }
}

class HttpStatusError extends Error {
    constructor(readonly status: number, body: string) {
        super(`HTTP ${status}${body ? `: ${body.slice(0, 200)}` : ''}`)
    }
}

/** Cách nói chuyện với API server. `inClusterConfig()` dựng nó từ ServiceAccount của pod. */
export type KubernetesApiConfig = {
    /** Ví dụ `https://10.43.0.1:443`. */
    server: string
    /** Đọc lại mỗi request: token của ServiceAccount được kubelet xoay vòng định kỳ. */
    token?: () => string | undefined
    /** CA của API server (PEM). */
    ca?: string | Buffer
}

/** Cấu hình API server khi chạy trong pod; `undefined` nếu không có ServiceAccount mount vào. */
export function inClusterConfig(): (KubernetesApiConfig & { namespace?: string }) | undefined {
    const host = process.env.KUBERNETES_SERVICE_HOST
    const tokenFile = `${SERVICE_ACCOUNT_DIR}/token`
    if (!host || !existsSync(tokenFile)) return undefined
    const port = Number(process.env.KUBERNETES_SERVICE_PORT || 443)
    const caFile = `${SERVICE_ACCOUNT_DIR}/ca.crt`
    return {
        server: `https://${formatTarget(host, port)}`,
        token: () => readFileSync(tokenFile, 'utf8').trim(),
        ca: existsSync(caFile) ? readFileSync(caFile) : undefined,
        namespace: readServiceAccountNamespace(),
    }
}

/** Namespace của pod hiện tại, đọc từ ServiceAccount. */
export function readServiceAccountNamespace() {
    const file = `${SERVICE_ACCOUNT_DIR}/namespace`
    return existsSync(file) ? readFileSync(file, 'utf8').trim() || undefined : undefined
}

type EndpointSlice = {
    metadata: { name: string; resourceVersion?: string }
    ports?: { name?: string; port?: number }[] | null
    endpoints?: { addresses?: string[]; conditions?: { ready?: boolean | null } }[] | null
}

type WatchEvent = {
    type: 'ADDED' | 'MODIFIED' | 'DELETED' | 'BOOKMARK' | 'ERROR'
    object: EndpointSlice & { code?: number; message?: string }
}

/**
 * Target của các endpoint ready trong một EndpointSlice. Cổng lấy theo tên (`portName`) từ chính slice,
 * không có thì dùng `defaultPort`. `ready` rỗng được coi là ready, theo quy ước của Kubernetes.
 */
export function endpointSliceTargets(slice: EndpointSlice, portName: string, defaultPort: number): string[] {
    const port = slice.ports?.find(entry => entry.name === portName)?.port ?? defaultPort
    return (slice.endpoints ?? [])
        .filter(endpoint => endpoint.conditions?.ready !== false)
        .flatMap(endpoint => endpoint.addresses ?? [])
        .map(address => formatTarget(address, port))
}

export type KubernetesApiMembershipOptions = KubernetesApiConfig & {
    namespace: string
    /** Tên Service; EndpointSlice được chọn theo label `kubernetes.io/service-name`. */
    service: string
    portName: string
    defaultPort: number
}

const RETRY_MIN_MS = 500
const RETRY_MAX_MS = 30_000
/** API server đóng watch sau khoảng này; ngắn hơn giới hạn mặc định để tự chủ việc mở lại. */
const WATCH_TIMEOUT_SECONDS = 290

/**
 * Membership từ EndpointSlice: `list` một lần, rồi `watch` tiếp từ `resourceVersion`. Watch hết hạn
 * (410) thì list lại; mất kết nối thì phát `null` và thử lại với thời gian chờ tăng dần.
 *
 * Observable lỗi với `KubernetesApiUnavailableError` khi API server từ chối hoặc không có API: đó là
 * lỗi cấu hình, thử lại không giúp được, nên bên gọi quyết định rơi về DNS hay không.
 */
export function kubernetesApiMembership(options: KubernetesApiMembershipOptions): Observable<MembershipSnapshot> {
    return new Observable<MembershipSnapshot>(subscriber => {
        let closed = false
        let current: ClientRequest | undefined
        let wake = () => {}

        const path = `/apis/discovery.k8s.io/v1/namespaces/${encodeURIComponent(options.namespace)}/endpointslices`
            + `?labelSelector=${encodeURIComponent(`kubernetes.io/service-name=${options.service}`)}`

        const open = (url: string) => new Promise<IncomingMessage>((resolve, reject) => {
            const target = new URL(url, options.server)
            const token = options.token?.()
            const send = target.protocol === 'https:' ? httpsRequest : httpRequest
            const request = send(target, {
                method: 'GET',
                headers: {
                    accept: 'application/json',
                    ...(token ? { authorization: `Bearer ${token}` } : {}),
                },
                ...(options.ca ? { ca: options.ca } : {}),
            }, resolve)
            request.once('error', reject)
            request.end()
            current = request
        })

        const readBody = (response: IncomingMessage) => new Promise<string>((resolve, reject) => {
            let body = ''
            response.setEncoding('utf8')
            response.on('data', chunk => { body += chunk })
            response.once('end', () => resolve(body))
            response.once('error', reject)
        })

        const expectOk = async (response: IncomingMessage) => {
            const status = response.statusCode ?? 0
            if (status === 200) return
            const body = await readBody(response).catch(() => '')
            if (status === 401 || status === 403 || status === 404) {
                throw new KubernetesApiUnavailableError(`HTTP ${status} ${response.statusMessage ?? ''}`.trim(), status)
            }
            throw new HttpStatusError(status, body)
        }

        const sleep = (ms: number) => new Promise<void>(resolve => {
            const timer = setTimeout(resolve, ms)
            wake = () => {
                clearTimeout(timer)
                resolve()
            }
        })

        const emit = (slices: Map<string, string[]>) => {
            if (!closed) subscriber.next(new Set([...slices.values()].flat()))
        }

        /** Một lần watch; trả `expired` khi phải list lại, `ended` khi API server đóng stream bình thường. */
        const watchOnce = async (slices: Map<string, string[]>, resourceVersion: { value?: string }) => {
            const url = `${path}&watch=true&allowWatchBookmarks=true&timeoutSeconds=${WATCH_TIMEOUT_SECONDS}`
                + (resourceVersion.value ? `&resourceVersion=${encodeURIComponent(resourceVersion.value)}` : '')
            const response = await open(url)
            if (response.statusCode === 410) {
                response.resume()
                return 'expired' as const
            }
            await expectOk(response)

            return await new Promise<'expired' | 'ended'>((resolve, reject) => {
                let buffer = ''
                response.setEncoding('utf8')
                response.on('data', chunk => {
                    buffer += chunk
                    let newline: number
                    while ((newline = buffer.indexOf('\n')) >= 0) {
                        const line = buffer.slice(0, newline).trim()
                        buffer = buffer.slice(newline + 1)
                        if (!line) continue

                        let event: WatchEvent
                        try {
                            event = JSON.parse(line) as WatchEvent
                        } catch (error) {
                            response.destroy()
                            reject(error)
                            return
                        }

                        if (event.type === 'ERROR') {
                            response.destroy()
                            if (event.object?.code === 410) resolve('expired')
                            else reject(new Error(`Watch error ${event.object?.code ?? '?'}: ${event.object?.message ?? ''}`))
                            return
                        }

                        resourceVersion.value = event.object?.metadata?.resourceVersion ?? resourceVersion.value
                        if (event.type === 'BOOKMARK') continue
                        if (event.type === 'DELETED') slices.delete(event.object.metadata.name)
                        else slices.set(event.object.metadata.name, endpointSliceTargets(event.object, options.portName, options.defaultPort))
                        emit(slices)
                    }
                })
                response.once('end', () => resolve('ended'))
                response.once('close', () => resolve('ended'))
                response.once('error', reject)
            })
        }

        const run = async () => {
            let delay = RETRY_MIN_MS
            while (!closed) {
                try {
                    const response = await open(path)
                    await expectOk(response)
                    const list = JSON.parse(await readBody(response)) as {
                        metadata?: { resourceVersion?: string }
                        items?: EndpointSlice[]
                    }
                    const slices = new Map((list.items ?? []).map(slice => [
                        slice.metadata.name,
                        endpointSliceTargets(slice, options.portName, options.defaultPort),
                    ]))
                    emit(slices)
                    delay = RETRY_MIN_MS

                    const resourceVersion = { value: list.metadata?.resourceVersion }
                    while (!closed) {
                        const startedAt = Date.now()
                        const outcome = await watchOnce(slices, resourceVersion)
                        if (outcome === 'expired') break
                        // API server đóng ngay lập tức nhiều lần: đừng quay vòng liên tục.
                        if (Date.now() - startedAt < 1000) await sleep(1000)
                    }
                } catch (error) {
                    if (closed) return
                    if (error instanceof KubernetesApiUnavailableError) {
                        subscriber.error(error)
                        return
                    }
                    subscriber.next(null)
                    await sleep(delay)
                    delay = Math.min(delay * 2, RETRY_MAX_MS)
                }
            }
        }

        void run()

        return () => {
            closed = true
            current?.destroy()
            wake()
        }
    })
}

/** Trả mọi địa chỉ IPv4 và IPv6 của một tên. */
export type Resolver = (hostname: string) => Promise<string[]>

/** Mã lỗi DNS nghĩa là "tên không có bản ghi": headless Service chưa có pod ready nào. */
const NO_RECORDS = new Set(['ENOTFOUND', 'ENODATA'])

/**
 * Phân giải đủ mọi bản ghi A và AAAA. Không dùng `dns.lookup`: nó chỉ trả một địa chỉ, trong khi headless
 * Service trả một bản ghi cho mỗi pod.
 */
export const resolveAll: Resolver = async hostname => {
    const results = await Promise.allSettled([resolve4(hostname), resolve6(hostname)])
    const addresses = results.flatMap(result => result.status === 'fulfilled' ? result.value : [])
    if (addresses.length > 0) return addresses
    const failure = results.find(result => result.status === 'rejected'
        && !NO_RECORDS.has((result.reason as { code?: string })?.code ?? ''))
    if (failure?.status === 'rejected') throw failure.reason
    return []
}

export type DnsMembershipOptions = {
    /** Tên DNS của headless Service, ví dụ `spider-mesh.apps.svc.cluster.local`. */
    hostname: string
    port: number
    intervalMs: number
    resolve?: Resolver
}

/** Membership bằng cách phân giải headless Service theo chu kỳ. DNS lỗi thì phát `null`. */
export function dnsMembership(options: DnsMembershipOptions): Observable<MembershipSnapshot> {
    const resolve = options.resolve ?? resolveAll
    return new Observable<MembershipSnapshot>(subscriber => {
        let closed = false
        let timer: ReturnType<typeof setTimeout> | undefined

        const tick = async () => {
            try {
                const addresses = await resolve(options.hostname)
                if (!closed) subscriber.next(new Set(addresses.map(address => formatTarget(address, options.port))))
            } catch {
                if (!closed) subscriber.next(null)
            }
            if (!closed) timer = setTimeout(tick, options.intervalMs)
        }
        void tick()

        return () => {
            closed = true
            clearTimeout(timer)
        }
    })
}
