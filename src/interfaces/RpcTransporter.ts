import { BehaviorSubject, Observable, Subject } from "rxjs"
import { SpiderMeshNode } from "./SpiderMeshNode.js"
import { NodesMap } from "src/SpiderMesh.js"

export type RpcRoutingOptions = { [key: string]: string | number | boolean }


export type RpcOptions<T> = {
    service: string
    method: string
    args: any[]
    fallback?: T
    timeout?: number
    retry?: number
    node_id?: string
    ip?: string
}

export type RpcEvent = Partial<{
    rpc: RpcOptions<any> & {
        callback: (o: any | Promise<any> | Observable<any>) => void
    }
    offline: string
    metadata: Record<string, string | boolean | number>
}>

export class RpcTransporter extends Observable<RpcEvent> {
    rpc: <T>(r: RpcOptions<T>, context: SpiderMeshNode[]) => Observable<T>
}

