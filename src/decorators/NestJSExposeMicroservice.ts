
import { LOCAL_SERVICES$ } from "./Microservice.js";

export const NestJSExposeMicroservice = (factory: any, metadata: object = {}) => ({
    provide: Symbol(),
    inject: [factory],
    useFactory: (instance: any) => {
        LOCAL_SERVICES$.next({
            instance: instance,
            name: factory.name,
            metadata
        })
    }
})

