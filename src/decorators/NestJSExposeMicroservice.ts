
import { services$ } from "./Microservice.js";

export const NestJSExposeMicroservice = (factory: any, metadata: object = {}) => ({
    provide: Symbol(),
    inject: [factory],
    useFactory: (instance: any) => {
        services$.next({
            ...services$.value,
            [factory.name]: {
                instance,
                name: factory.name,
                metadata
            }
        })
    }
})

