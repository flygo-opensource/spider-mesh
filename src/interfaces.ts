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
    init: (options: SpiderMeshTransporterInitOptions) => void
    $requests: Observable<RpcOptions & { reply: (o: Observable<any> | Promise<any>) => void }>
    rpc: <T>(options: RpcOptions) => Observable<T>
}

export type SpiderMeshPubsubTransporter = {
    init: (options: SpiderMeshTransporterInitOptions) => void
    $nodes: Observable<{ node_id: string, status: 'online' | 'offline' }>
    listen: <T>(topic: string) => Observable<T>
    publish: <T>(options: PublishOptions<T>) => Promise<void>
}

export type SpiderMeshTransporterInitOptions = {node_id:string, services:string[]}