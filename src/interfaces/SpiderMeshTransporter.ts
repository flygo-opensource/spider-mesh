import { Observable } from "rxjs"
import { Encodeable } from "src/Encoder.js"

export type PublishMetadata<T extends Encodeable = Encodeable> = {
    event: string,
    remote_transporter_id?: string
    data: T
}

export type SpiderMeshTransporterEvent<T extends Encodeable = Encodeable> = {
    data: T,
    received_transporter_id: string
    sender_transporter_id: string
}

export type TcpNodeStatus = {
    local_transporter_id: string
    remote_transporter_id: string
    online: boolean
}

export type SpiderMeshTransporter = {

    readonly transporter_id: string

    $nodes_status: Observable<TcpNodeStatus>

    listen: <T extends Encodeable>(topic: string) => Observable<SpiderMeshTransporterEvent<T>>
    publish<T extends Encodeable>(config: PublishMetadata<T>): Promise<void>
}


export type SpiderMeshTransporterFactory = { new(...args: any[]): SpiderMeshTransporter }