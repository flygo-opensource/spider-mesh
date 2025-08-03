import { BehaviorSubject, Observable, Subject } from "rxjs"
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
    name: `rpc-${string}`
    offline$: Observable<string>
    requests$: Observable<RpcOptions & {
        callback: (o: any | Promise<any> | Observable<any>) => void
    }>
    rpc: <T>(r: RpcOptions, node: SpiderMeshNode) => Observable<T | undefined>
}

