import { Observable } from "rxjs"
import { SpiderMeshNode } from "./SpiderMeshNode.js"

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
    selfLoadBalancing?: boolean = false
    rpc: <T>(r: RpcOptions<T>, node?: SpiderMeshNode) => Observable<T>
}

