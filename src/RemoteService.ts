import { catchError, EMPTY, firstValueFrom, from, map, mergeMap, Observable, of } from "rxjs";
import { ServiceChecker, SpiderMesh } from "./SpiderMesh.js";
import { RpcOptions, SpiderMeshNode } from "./types.js";



const InvaildMethodList = new Set([
    'caller',
    'callee',
    'arguments',
    'constructor',
    '__defineGetter__',
    '__defineSetter__',
    'hasOwnProperty',
    '__lookupGetter__',
    '__lookupSetter__',
    'isPrototypeOf',
    'propertyIsEnumerable',
    'toString',
    'valueOf',
    'toLocaleString',
    '__proto__',
    'onModuleInit',
    'onApplicationBootstrap',
    'onModuleDestroy',
    'beforeApplicationShutdown',
    'onApplicationShutdown'
])

/** Type helper dùng để nhận biết generic đang giữ giá trị `unknown`. */
export type IsUnknown<T, A, B> = unknown extends T ? ([T] extends [unknown] ? A : B) : B;

/** Cấu hình mặc định của proxy gọi một remote service. */
export type RemoteServiceOptions = Partial<RpcOptions<any>> & { service: string }

/** Chuẩn hóa Promise/Observable/value thành kiểu trả về mà proxy cung cấp cho caller. */
export type Unwrap<T> = Awaited<T> extends Observable<infer U> ? Observable<U> : (T extends Promise<infer V> ? Promise<V> : Promise<T>)

/** Thêm fallback type vào kết quả của một remote method. */
export type Fallbackable<Fn, FallbackValue = unknown> = Fn extends (...args: infer A) => infer R ? (
    (...args: A) => IsUnknown<FallbackValue, Unwrap<R>, FallbackValue | Unwrap<R>>
) : undefined

/** Chỉ giữ property là function khi ánh xạ service interface. */
export type FunctionOnly<T, Fallback> = T extends (...args: any[]) => any ? Fallback : never




/** Tạo proxy typed để gọi, chờ, theo dõi hoặc batch một remote service. */
export class RemoteServiceLinker<Service> {

    constructor(
        private sm: SpiderMesh,
        private options: RemoteServiceOptions
    ) { }



    watch() {
        return this.sm.watchService(this.options.service)
    }

    get nodes() {
        return this.sm.listRpcNodes(this.options.service)
    }

    set<Fallback>(options: Omit<Partial<RpcOptions<Fallback>>, 'service' | 'method' | 'args'>) {
        return RemoteServiceLinker.link<Service, Fallback>(this.sm, {
            ...this.options,
            ...options
        })
    }

    wait(checker: ServiceChecker = (nodes => nodes.length > 0), stop$: Observable<any> = EMPTY) {
        return this.sm.waitForService(this.options.service, checker, stop$)
    }

    static link<Service, Fallback = never>(sm: SpiderMesh, options: RemoteServiceOptions) {
        const target = new this<Service>(sm, options)
        const handler: ProxyHandler<any> = {
            get(_, prop) {
                const method = prop.toString()
                if (method == 'then') return null
                if (InvaildMethodList.has(method)) return () => { }
                const fn = (target as Service)[prop as keyof Service]
                if (fn) return (typeof fn === 'function') ? fn.bind(target) : fn;

                if (method.startsWith('__batch__')) {
                    const real_metod = method.split('__batch__')?.[1]

                    return (...args: any[]) => from(target.sm.listRpcNodes(options.service)).pipe(
                        mergeMap(node => (
                            target.sm.callRemoteService({
                                ...options,
                                // Batch phải pin từng RPC vào đúng node đang được lặp.
                                node_id: node.node_id,
                                method: real_metod,
                                args
                            }).pipe(
                                map(data => ({ node, data })),
                                catchError(error => of({ node, error }))
                            )
                        ))
                    )
                }

                return (...args: any[]) => {

                    const Service = target.sm.callRemoteService({
                        ...options,
                        method,
                        args
                    })

                    // Vừa là Observable (subscribe để nhận stream), vừa dùng được như Promise.
                    // Promise được tạo lười một lần: `await` nhiều lần chỉ gửi một RPC, và
                    // then/catch/finally trả về Promise thật để chain được như Promise thường.
                    let settled: Promise<unknown> | undefined
                    const asPromise = () => settled ??= firstValueFrom(Service)
                    return Object.assign(Service, {
                        then: (onFulfilled?: (value: any) => unknown, onRejected?: (reason: any) => unknown) =>
                            asPromise().then(onFulfilled, onRejected),
                        catch: (onRejected?: (reason: any) => unknown) => asPromise().catch(onRejected),
                        finally: (onFinally?: () => void) => asPromise().finally(onFinally),
                    })

                }

            }
        }
        return new Proxy(target, handler) as Mapper<Service, Fallback>
    }
}

/** Kiểu proxy cuối cùng gồm các remote method và biến thể `__batch__`. */
export type Mapper<Service, Fallback = unknown> = RemoteServiceLinker<Service> & ({
    [K in keyof Service as  FunctionOnly<Service[K], K>]: Fallbackable<Service[K], Fallback>
} & {
    [key in keyof Service as FunctionOnly<Service[key], key extends string ? `__batch__${key}` : never>]: Service[key] extends ((...args: any) => any) ? (
        (...args: Parameters<Service[key]>) => Observable<{
            node: SpiderMeshNode,
            data: Awaited<ReturnType<Service[key]>>
        } | {
            node: SpiderMeshNode,
            error: Error
        }>
    ) : never
})


/** Alias ngắn cho một remote service proxy đã được map type. */
export type RemoteService<Service> = Mapper<Service>
