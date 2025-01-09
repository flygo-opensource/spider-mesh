import { Observable } from "rxjs"


export type RpcOptions = {
    service: string
    method: string
    args: any[]
    node_id?: string
    ip?: string
    fallback?: any
    timeout?: number
    retry?: number
}

export type PublishOptions<T> = {
    event: string
    data: T
}

export type SpiderMeshRpcTransporter = {
    rpc: <T>(options: RpcOptions) => Observable<T>
}

export type SpiderMeshPubsubTransporter = {
    $nodes: Observable<{ node_id: string, status: 'online' | 'offline', services: string[] }>
    listen: <T>(topic: string) => Observable<T>
    publish: <T>(options: PublishOptions<T>) => Promise<void>
}

export type SpiderMeshTransporterInitOptions = { node_id: string, services: string[], events: string[] }