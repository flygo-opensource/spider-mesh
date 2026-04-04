import { SpiderMeshNode } from "@spider-mesh/types"
import { BehaviorSubject } from "rxjs/internal/BehaviorSubject"
import { ReplaySubject } from "rxjs/internal/ReplaySubject"

export const LOCAL_SERVICES$ = new ReplaySubject<{
    name: string,
    instance: any,
    metadata: object | (() => Promise<object>)
}>()


export const MicroserviceList: SpiderMeshNode['services'] = {}

export const Microservice = (metadata: any = {}) => {
    return (
        (target: { new(...args: any[]): {} }) => {
            class C extends target {
                constructor(...args: any[]) {
                    super(...args)
                    LOCAL_SERVICES$.next({
                        instance: this,
                        name: target.name,
                        metadata
                    })
                }
            }
            MicroserviceList[target.name] = metadata
            Object.defineProperty(C, 'name', { value: target.name })
            return C as any
        }


    ) as any
}
