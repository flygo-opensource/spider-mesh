import { from, mergeMap, ReplaySubject, tap, toArray } from "rxjs"
import { NAMEPSACE } from "../const.js"
import { ServiceMetadata } from "../../src/interfaces/SpiderMeshNode.js"
import { listBeforeMicroserviceOnlineMethods } from "./BeforeMicroserviceOnline.js"

export const $services = new ReplaySubject<{ instance: any, namespace: string, metadata: ServiceMetadata }>()


export const Microservice = (namespace: string = NAMEPSACE, metadata: ServiceMetadata = {}) => {
    return (
        (target: { new(...args: any[]): {} }) => {
            class C extends target {
                constructor(...args: any[]) {
                    super(...args)
                    from(listBeforeMicroserviceOnlineMethods(this)).pipe(
                        mergeMap(async method => {
                            const fn = (this as any)[method]
                            typeof fn == 'function' && await fn()
                        }, 1),
                        toArray(),
                        tap(() => $services.next({
                            instance: this,
                            namespace,
                            metadata
                        }))
                    ).subscribe()

                }
            }
            Object.defineProperty(C, 'name', { value: target.name })
            return C as any
        }


    ) as any
}