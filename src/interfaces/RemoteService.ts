import { Observable, Subject } from "rxjs";
import { SpiderMeshNode } from "./SpiderMeshNode.js";
import { RpcOptions } from "./RpcTransporter.js";


export type RemoteService<T = { [key: string]: any }, Fallback = null> = (
    {
        wait$: (fn?: (nodes: SpiderMeshNode[]) => boolean | Promise<boolean>) => Promise<void>,
        set: <F>(options: Partial<RpcOptions<F>>) => RemoteService<T, F>,
        nodes: SpiderMeshNode[],
        watch$: () => Subject<void>
    } & {
        [K in keyof T as (T[K] extends (...args: any) => any ? K : '')]: T[K] extends (...args: any) => any ? ((...args: Parameters<T[K]>) => ReturnType<T[K]> extends (Observable<any> | Promise<Observable<any>>) ? Awaited<ReturnType<T[K]> | Fallback> : Promise<Awaited<ReturnType<T[K]> | Fallback>>) : null
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
