import { BehaviorSubject, ReplaySubject } from "rxjs"
import { NAMEPSACE } from "../const.js"
import { SpiderMeshNode } from "../interfaces/SpiderMeshNode.js"

export const services$ = new BehaviorSubject<{
    [name: string]: {
        name: string,
        instance: any,
        metadata: object | (() => Promise<object>)
    }
}>({})


export const MicroserviceList: SpiderMeshNode['services'] = {}

export const Microservice = (metadata: object = {}) => {
    return (
        (target: { new(...args: any[]): {} }) => {
            class C extends target {
                constructor(...args: any[]) {
                    super(...args)
                    services$.next({
                        ...services$.value,
                        [target.name]: {
                            instance: this,
                            name: target.name,
                            metadata
                        }
                    })

                }
            }
            MicroserviceList[target.name] = metadata
            Object.defineProperty(C, 'name', { value: target.name })
            return C as any
        }


    ) as any
}