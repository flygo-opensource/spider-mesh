import { SpiderMesh } from "../SpiderMesh"

const MicroserviceList = [] as any[]

export const Microservice = () => (target: { new(...args: any[]): {} }) => {
    MicroserviceList.push(target)
    class C extends target {
        constructor(...args: any[]) {
            super(...args)
            SpiderMesh.spider_mesh_instances.forEach(
                sm => sm.active_local_service(this)
            )

        }
    }
    Object.defineProperty(C, 'name', { value: target.name })
    return C as any
}


export const listMicroserviceFactories = () => [...MicroserviceList]