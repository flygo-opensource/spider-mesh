 
import { NAMEPSACE } from "../const.js";
import { ServiceMetadata } from "../interfaces/SpiderMeshNode.js";
import { $services } from "./Microservice.js";

export const NestJSExposeMicroservice = (factory: any, metadata: ServiceMetadata = {}, namespace: string = NAMEPSACE) => ({
    provide: Symbol(),
    inject: [factory],
    useFactory: (instance: any) =>  $services.next({
        instance,
        namespace,
        metadata
    })
})

