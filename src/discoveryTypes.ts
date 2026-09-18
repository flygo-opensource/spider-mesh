import type { Observable } from 'rxjs'

// Chép từ gói `@spider-mesh/discovery` cũ (đã bỏ) để `ws` không phụ thuộc gói nào chỉ vì vài type.
// Giữ nguyên cấu trúc: mọi discovery (kể cả `@ohayo/udp` và `TopologyDiscoveryAdapter`) khớp
// nhau theo structural typing, không qua import.

/** Envelope chuẩn để một discovery implementation trao đổi payload node generic. */
export type DiscoveryMessage<T> = {
    node_id: string
    namespace: string
    tags: string[]
    version: string
    created_at: number
    seq: number
    data: T
    remote_host?: string
}

/** Event nhận vào hiện dùng cùng cấu trúc với discovery message. */
export type DiscoveryEvent<T> = DiscoveryMessage<T>

/** Kết quả xác minh membership, tương thích structural với Core Topology. */
export type DiscoveryNodeVerificationResult = 'alive' | 'dead' | 'unknown'

/** Contract transport generic của Discovery, không phụ thuộc SpiderMesh Core. */
export type DiscoveryTransporter<T> = Observable<DiscoveryEvent<T>> & {
    broadcast(message: DiscoveryMessage<T>): Promise<void>
    verify?(node_id: string): Promise<DiscoveryNodeVerificationResult>
    close?: () => void
}
