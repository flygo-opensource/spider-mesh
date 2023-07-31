import { Observable } from "rxjs"

export type PublishMetadata<T> = {
    event: string,
    node_id?: string,
    data: T,
    routing_key?: string
} 

export type SpiderMeshTransporterEvent<T> = {
    data: T,
    sender_node_id: string
}

export type SpiderMeshTransporter = {

    readonly node_id: string
    readonly namespace: string
    $nodes_status: Observable<{
        node_id: string,
        online: boolean
    }>

    start: () => Promise<void>
    listen: <T = any>(topic: string) => Observable<SpiderMeshTransporterEvent<T>>
    publish<T = any>(config: PublishMetadata<T>): Promise<void>
}


export type SpiderMeshTransporterFactory = { new(...args: any[]): SpiderMeshTransporter }