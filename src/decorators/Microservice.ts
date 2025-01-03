import { from, mergeMap, ReplaySubject, tap, toArray } from "rxjs"
import { NAMEPSACE } from "../const.js"
import { listBeforeMicroserviceOnlineMethods } from "./BeforeMicroserviceOnline.js"

export const $services = new ReplaySubject<{ instance: any, namespace: string  }>()


export const Microservice = (namespace: string = NAMEPSACE ) => {
    return (
        (target: { new(...args: any[]): {} }) => {
            class C extends target {
                constructor(...args: any[]) {
                    super(...args)
                    from(listBeforeMicroserviceOnlineMethods(this)).pipe(
                        mergeMap(async method => {
                            typeof (this as any)[method] == 'function' && await (this as any)[method]()
                        }, 1),
                        toArray(),
                        tap(() => $services.next({
                            instance: this,
                            namespace 
                        }))
                    ).subscribe()

                }
            }
            Object.defineProperty(C, 'name', { value: target.name })
            return C as any
        }


    ) as any
}