import { Observable, ReplaySubject } from "rxjs"
import { SpiderMeshNode } from "./SpiderMeshNode.js"

export type RpcRoutingOptions = { [key: string]: string | number | boolean }


export type RpcOptions = {
    service: string
    method: string
    args: any[]
    fallback?: any
    timeout?: number
    retry?: number
    node_id?: string
    ip?: string
}


export type RpcTransporter = {
    metadata$: ReplaySubject<{
        [name: string]: string | number | boolean
    }>
    rpc: <T>(r: RpcOptions) => Observable<T | undefined>
    link?: (node: SpiderMeshNode) => any
    check(service: string): SpiderMeshNode[]
}

