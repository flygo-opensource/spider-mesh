
import { NAMEPSACE } from "../const.js";
import { $services } from "./Microservice.js";

export const NestJSExposeMicroservice = (factory: any, namespace: string = NAMEPSACE) => ({
    provide: Symbol(),
    inject: [factory],
    useFactory: (instance: any) => $services.next({
        instance,
        namespace
    })
})

