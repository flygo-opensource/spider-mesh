import { RPCOptions } from "./RPCOptions";
import { SpiderMeshNode } from "./SpiderMeshNode";
import { ServiceNodeMonitor } from "./SpiderMesh";

export type RemoteService<T> = (
    {
        [K in keyof T as (T[K] extends (...args: any) => any ? K : '')]: T[K] extends (...args: any) => any ? ((...args: Parameters<T[K]>) => Promise<Awaited<ReturnType<T[K]>>>) : null
    } & {
        [key in keyof RPCOptions as `$set_${key}`]: (value: RPCOptions[key]) => RemoteService<T>
    } & {
        $list_nodes: () => SpiderMeshNode[]
        $monitor: (cb: ServiceNodeMonitor) => {
            unsubscribe: Function
        },
    }
)