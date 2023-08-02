import { Observable, Subject, lastValueFrom, takeUntil, timer, toArray } from "rxjs";
import { RPCOptions } from "./RPCOptions";
import { SpiderMeshNode } from "./SpiderMeshNode";


export type RemoteService<T = { [key: string]: any }> = (
    {
        [K in keyof T as (T[K] extends (...args: any) => any ? K : '')]: T[K] extends (...args: any) => any ? ((...args: Parameters<T[K]>) => Promise<Awaited<ReturnType<T[K]>>>) : null
    } & {
        [key in keyof RPCOptions as `$set_${key}`]: (value: RPCOptions[key]) => RemoteService<T>
    } & {
        $list_nodes: () => SpiderMeshNode[]
    } & {
        [key in keyof T as (key extends string ? `$batch_${key}` : string)]: T[key] extends ((...args: any) => any) ? (
            (...args: Parameters<T[key]>) => Observable<{ node: SpiderMeshNode, data: Awaited<ReturnType<T[key]>> }>
        ) : T[key]
    }

)