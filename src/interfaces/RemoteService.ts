import { Observable, Subject } from "rxjs";
import { SpiderMeshNode } from "./SpiderMeshNode.js";
import { RpcOptions } from "./RpcTransporter.js";


export type RemoteService<T = { [key: string]: any }> = (
    {
        wait$: (fn?: (nodes: SpiderMeshNode[]) => boolean | Promise<boolean>) => Promise<void>,
        set: (options: Partial<RpcOptions<T>>) => RemoteService<T>,
        nodes: SpiderMeshNode[],
        watch$: () => Subject<void>
    } & {
        [K in keyof T as (T[K] extends (...args: any) => any ? K : '')]: T[K] extends (...args: any) => any ? ((...args: Parameters<T[K]>) => ReturnType<T[K]> extends (Observable<any> | Promise<Observable<any>>) ? Awaited<ReturnType<T[K]>> : Promise<Awaited<ReturnType<T[K]>>>) : null
    } & {
        [key in keyof T as (key extends string ? `__batch__${key}` : string)]: T[key] extends ((...args: any) => any) ? (
            (...args: Parameters<T[key]>) => Observable<{
                node: SpiderMeshNode,
                data: Awaited<ReturnType<T[key]>>
            } | {
                node: SpiderMeshNode,
                error: Error
            }>
        ) : T[key]
    }

)
