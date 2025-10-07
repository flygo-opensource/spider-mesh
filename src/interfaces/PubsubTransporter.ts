import { Observable } from "rxjs"
import { NodesMap } from "src/SpiderMesh.js"


export type PubsubTransporterEvent = {
    metadata: Record<string, string | boolean | number>
}


export abstract class PubsubTransporter {
    abstract link(nodes$:  Observable<NodesMap>): Observable<PubsubTransporterEvent>
    abstract publish<T>(topic: string, data: T): Promise<void>
    abstract listen<T>(topic: string): Observable<T>
}