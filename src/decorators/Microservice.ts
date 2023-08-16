import { ReplaySubject } from "rxjs"
import { NAMEPSACE } from "../const.js"

export const serviceInstanceList = new ReplaySubject<{ instance: any, namespace: string }>()


export const Microservice = (namespace: string = NAMEPSACE) => {
    return (
        (target: { new(...args: any[]): {} }) => {
            class C extends target {
                constructor(...args: any[]) {
                    super(...args)
                    serviceInstanceList.next({
                        instance: this,
                        namespace
                    })
                }
            }
            Object.defineProperty(C, 'name', { value: target.name })
            return C as any
        }


    ) as any
}