import { RpcOptions, SpiderMeshNode } from "@spider-mesh/types";
import { catchError, EMPTY, filter, takeUntil, firstValueFrom, from, map, mergeMap, Observable, of, timer } from "rxjs";
import { ServiceChecker, SpiderMesh } from "./SpiderMesh.js";



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

export type IsUnknown<T, A, B> = unknown extends T ? ([T] extends [unknown] ? A : B) : B;

export type RemoteServiceOptions = Partial<RpcOptions<any>> & { service: string }

export type Unwrap<T> = Awaited<T> extends Observable<infer U> ? Observable<U> : (T extends Promise<infer V> ? Promise<V> : Promise<T>)

export type Fallbackable<Fn, FallbackValue = unknown> = Fn extends (...args: infer A) => infer R ? (
    (...args: A) => IsUnknown<FallbackValue, Unwrap<R>, FallbackValue | Unwrap<R>>
) : undefined

export type FunctionOnly<T, Fallback> = T extends (...args: any[]) => any ? Fallback : never




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
        return firstValueFrom(this.watch().pipe(
            takeUntil(stop$),
            filter(nodes => {
                if (checker(nodes)) return true
                return false
            })
        ), { defaultValue: null })
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

                    return Object.assign(Service, {
                        then: async (s: Function, r: Function) => {
                            try {
                                s(await firstValueFrom(Service))
                            } catch (e) {
                                r(e)
                            }
                        }
                    })

                }

            }
        }
        return new Proxy(target, handler) as Mapper<Service, Fallback>
    }
}

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


export type RemoteService<Service> = Mapper<Service>
