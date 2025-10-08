import { Observable } from "rxjs"
import { NodesMap } from "src/SpiderMesh.js"
import { SpiderMeshNode } from "./SpiderMeshNode.js"


export type PubsubTransporterEvent = {
    metadata: Record<string, string | boolean | number>
}


export abstract class PubsubTransporter {
    abstract link(metadata: Observable<SpiderMeshNode>, nodes$: Observable<NodesMap>): Observable<PubsubTransporterEvent>
    abstract publish<T>(topic: string, data: T): Promise<void>
    abstract listen<T>(topic: string): Observable<T>
}