import { Observable, Subject } from "rxjs";
import { RpcOptions } from "./SpiderMeshTransporter.js";
import { SpiderMeshNode } from "./SpiderMeshNode.js";


export type RemoteService<T = { [key: string]: any }> = (
    {
        $wait: (fn?: (nodes: SpiderMeshNode[]) => boolean | Promise<boolean>) => Promise<void>,
        $set: (options: RpcOptions) => RemoteService<T>,
        $nodes: SpiderMeshNode[],
        $watch: () => Subject<SpiderMeshNode & { status: 'online' | 'offline' }>
    } & {
        [K in keyof T as (T[K] extends (...args: any) => any ? K : '')]: T[K] extends (...args: any) => any ? ((...args: Parameters<T[K]>) => ReturnType<T[K]> extends (Observable<any> | Promise<Observable<any>>) ? Awaited<ReturnType<T[K]>> : Promise<Awaited<ReturnType<T[K]>>>) : null
    } & {
        [key in keyof T as (key extends string ? `__batch__${key}` : string)]: T[key] extends ((...args: any) => any) ? (
            (...args: Parameters<T[key]>) => Observable<{
                node: SpiderMeshNode,
                data: Awaited<ReturnType<T[key]>>
                error: any
            }>
        ) : T[key]
    }

)
 