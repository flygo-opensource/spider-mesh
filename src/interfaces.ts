import { Observable } from "rxjs"


export type RpcOptions = {
    service: string
    method: string
    args: any[]
    node_id?: string
    ip?: string
    fallback?: any
    timeout?: number
    retry?: number
}

export type PublishOptions<T> = {
    event: string
    data: T
} 