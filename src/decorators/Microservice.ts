import { ReplaySubject } from "rxjs"
import { NAMEPSACE } from "../const.js"
import { ServiceMetadata } from "src/interfaces/SpiderMeshNode.js"

export const serviceInstanceList = new ReplaySubject<{ instance: any, namespace: string, metadata: ServiceMetadata }>()


export const Microservice = (namespace: string = NAMEPSACE, metadata: ServiceMetadata = {}) => {
    return (
        (target: { new(...args: any[]): {} }) => {
            class C extends target {
                constructor(...args: any[]) {
                    super(...args)
                    serviceInstanceList.next({
                        instance: this,
                        namespace,
                        metadata
                    })
                }
            }
            Object.defineProperty(C, 'name', { value: target.name })
            return C as any
        }


    ) as any
}