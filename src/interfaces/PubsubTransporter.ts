import { Observable } from "rxjs"
import { SpiderMeshNode } from "./SpiderMeshNode.js"

export type PubsubTransporterEvent = Partial<{
    metadata: Record<string, string | boolean | number>
}>


export class PubsubTransporter extends Observable<PubsubTransporterEvent> {
    publish: <T>(topic: string, data: T, context: Map<string, SpiderMeshNode>) => Promise<void>
    listen: <T>(topic: string) => Observable<T>
}