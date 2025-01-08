import { ReplaySubject } from "rxjs"
import { NAMEPSACE } from "../const.js"

export const $services = new ReplaySubject<{
    name: string,
    instance: any,
    namespace: string,
    metadata: object
}>()


export const Microservice = (namespace: string = NAMEPSACE, metadata: object = {}) => {
    return (
        (target: { new(...args: any[]): {} }) => {
            class C extends target {
                constructor(...args: any[]) {
                    super(...args)
                    $services.next({
                        instance: this,
                        namespace,
                        name: target.name,
                        metadata
                    })

                }
            }
            Object.defineProperty(C, 'name', { value: target.name })
            return C as any
        }


    ) as any
}