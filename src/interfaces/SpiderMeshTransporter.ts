import { Observable } from "rxjs"
import { Encodeable } from "src/Encoder.js"

export type PublishMetadata = {
    event: string,
    node_id?: string,
    data: Encodeable
}

export type SpiderMeshTransporterEvent<T extends Encodeable = Encodeable> = {
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
    listen: <T extends Encodeable>(topic: string) => Observable<SpiderMeshTransporterEvent<T>>
    publish(config: PublishMetadata): Promise<void>
}


export type SpiderMeshTransporterFactory = { new(...args: any[]): SpiderMeshTransporter }