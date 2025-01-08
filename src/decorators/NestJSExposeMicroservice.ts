
import { NAMEPSACE } from "../const.js";
import { $services } from "./Microservice.js";

export const NestJSExposeMicroservice = (factory: any, namespace: string = NAMEPSACE, metadata: object = {}) => ({
    provide: Symbol(),
    inject: [factory],
    useFactory: (instance: any) => $services.next({
        instance,
        namespace,
        name: factory.name,
        metadata
    })
})

