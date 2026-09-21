import type { Observable } from 'rxjs'

/** Metadata endpoint mà event transporter quảng bá trong local node. */
export type EventTransporterMetadata = Record<string, string | boolean | number>

/** Transporter của EventBus với tên ổn định do implementation tự khai báo. */
export type EventTransporter = {
    readonly name: string
    publish<T>(topic: string, data: T): Promise<void>
    listen<T>(topic: string): Observable<T>
    metadata?: EventTransporterMetadata | null
    metadata$?: Observable<EventTransporterMetadata>
    close?: () => void
}

/** Event transporter được chọn bằng wire name ổn định. */
export type EventTransporterSelector = string

/** Structural bridge để EventBus cập nhật metadata mà không phụ thuộc core runtime. */
export type EventMeshHost = {
    setLocalTopics(topics: string[]): void
    setLocalTransporterMetadata(name: string, metadata?: unknown): void
}

/** Cách EventBus chọn một hay toàn bộ transporter cho một event. */
export type EventDeliveryMode = 'single' | 'fanout'

/** Cấu hình EventBus độc lập với RPC. */
export type EventBusOptions = {
    mesh?: EventMeshHost
    mode?: EventDeliveryMode
}

/** Tùy chọn publish/listen của một event topic. */
export type EventLinkOptions = {
    transporter?: EventTransporterSelector
    mode?: EventDeliveryMode
}
