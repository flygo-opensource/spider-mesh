import { Observable } from "rxjs"

export type RpcRoutingOptions = { node_id: string, ip: string, hostname: string, port: number }

export type RpcOptions = {
    service: string
    method: string
    args: any[]
    ip?: string
    fallback?: any
    timeout?: number
    retry?: number
    routing?: RpcRoutingOptions
}

export type PublishOptions<T> = {
    event: string
    data: T
}

export type SpiderMeshRpcTransporter = {
    rpc: <T>(options: RpcOptions) => Observable<T>
}

export type SpiderMeshPubsubTransporter = { 
    listen: <T>(topic: string) => Observable<T>
    publish: <T>(options: PublishOptions<T>) => Promise<void>
}

export type SpiderMeshTransporterInitOptions = { node_id: string, services: string[], events: string[] }