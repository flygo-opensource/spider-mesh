import { Observable } from "rxjs"
import { SpiderMeshNode } from "./SpiderMeshNode.js"

export type RpcRoutingOptions = { [key: string]: string | number | boolean }


export type RpcOptions<T = any> = {
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
    online: string
    offline: string
    metadata: Record<string, string | boolean | number>
}>

export abstract class RpcTransporter {
    abstract link(
        metadata: Observable<SpiderMeshNode>,
        nodes$: Observable<{
            nodes: Map<string, SpiderMeshNode>,
            last_updated_node_id: string
        }>
    ): Observable<RpcEvent>
    abstract rpc<R, T>(r: RpcOptions<T>, node: SpiderMeshNode, force: boolean): Observable<R>
}

