import { Observable, ReplaySubject } from "rxjs"
import { SpiderMeshNode } from "./SpiderMeshNode.js"

export type PubsubTransporter = {
    metadata$: ReplaySubject<{
        [name: string]: string | number | boolean
    }>
    link?: (node: SpiderMeshNode) => any
    publish: <T>(topic: string, data: T) => Promise<void>
    listen: <T>(topic: string) => Observable<T>
}