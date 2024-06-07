import { Observable } from "rxjs"
import { Encodable } from "../Encoder.js"

export type SpiderMeshTransporterEventMetadata = {
    [key: string]: Encodable
}

export type SpiderMeshTransporterEvent<T extends Encodable = Encodable, Metadata = SpiderMeshTransporterEventMetadata> = {
    id: string
    topic: string
    created_at: number
    received_at: number
    payload: T,
    metadata: Metadata

    /** Sender transporter id */
    sti: string
}

export type PublishMetadata<T extends Encodable = Encodable, Metadata = SpiderMeshTransporterEventMetadata> = {
    event: string,
    payload: T
    metadata: Metadata

    /** Receiver trasporter id */
    rti?: string
}


export type TcpNodeStatus = { remote_transporter_id: string, online: boolean }

export type SpiderMeshTransporter = {

    readonly transporter_id: string

    $nodes_status: Observable<TcpNodeStatus>

    listen: <T extends Encodable, Metadata extends SpiderMeshTransporterEventMetadata>(topic: string) => Observable<SpiderMeshTransporterEvent<T, Metadata>>
    publish<T extends Encodable, Metadata extends SpiderMeshTransporterEventMetadata>(config: PublishMetadata<T, Metadata>): Promise<void>
}


export type SpiderMeshTransporterFactory = { new(...args: any[]): SpiderMeshTransporter }