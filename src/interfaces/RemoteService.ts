import { Observable } from "rxjs";
import { RPCOptions } from "../RPCOptions.js";
import { SpiderMeshNode } from "./SpiderMeshNode.js";


export type RemoteService<T = { [key: string]: any }> = (
    {
        $wait_service_online: (fn?: (nodes: SpiderMeshNode[]) => boolean | Promise<boolean>) => Promise<void>,
        $safe_mode: () => {
            [K in keyof T as (T[K] extends (...args: any) => any ? K : '')]: T[K] extends (...args: any) => any ? ((...args: Parameters<T[K]>) => [
                Error | null,
                Awaited<ReturnType<T[K]>> | null
            ]) : null
        }
    } & {
        [K in keyof T as (T[K] extends (...args: any) => any ? K : '')]: T[K] extends (...args: any) => any ? ((...args: Parameters<T[K]>) => ReturnType<T[K]> extends (Observable<any> | Promise<Observable<any>>) ? Awaited<ReturnType<T[K]>> : Promise<Awaited<ReturnType<T[K]>>>) : null
    } & {
        [key in keyof RPCOptions as `${key}`]: (value: RPCOptions[key]) => RemoteService<T>
    } & {
        $list_nodes: () => SpiderMeshNode[]
    } & {
        [key in keyof T as (key extends string ? `$batch_${key}` : string)]: T[key] extends ((...args: any) => any) ? (
            (...args: Parameters<T[key]>) => Observable<{
                node: SpiderMeshNode,
                data: Awaited<ReturnType<T[key]>>
                error: any
            }>
        ) : T[key]
    } & {
        $watch: () => Observable<SpiderMeshNode>
    }

) 