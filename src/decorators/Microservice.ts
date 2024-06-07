import { ReplaySubject } from "rxjs"
import { NAMEPSACE } from "../const.js"
import { ServiceMetadata } from "../../src/interfaces/SpiderMeshNode.js"
import { SpiderMesh } from "../../src/SpiderMesh.js"

export const $services = new ReplaySubject<{ instance: any, namespace: string, metadata: ServiceMetadata }>()



export const NestJSExposeMicroservice = (factory: any, metadata: ServiceMetadata = {}, namespace: string = NAMEPSACE) => ({
    provide: Symbol(),
    inject: [factory],
    useFactory: (instance: any) =>  $services.next({
        instance,
        namespace,
        metadata
    })
})

export const NestJSLinkMicroservice = (factory: any, wait_service_online?: boolean) => ({
    provide: factory,
    inject: [SpiderMesh],
    useFactory: (sm: SpiderMesh) => sm.link_remote_service(factory, wait_service_online)
})

export const NestJSLinkEvent = (factory: any, publish_buffer_ms?: number) => ({
    provide: factory,
    inject: [SpiderMesh],
    useFactory: (sm: SpiderMesh) => sm.link_event(factory, publish_buffer_ms)
})

export const Microservice = (namespace: string = NAMEPSACE, metadata: ServiceMetadata = {}) => {
    return (
        (target: { new(...args: any[]): {} }) => {
            class C extends target {
                constructor(...args: any[]) {
                    super(...args)
                    $services.next({
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