import { SpiderMesh } from "../SpiderMesh.js";

export const NestJSLinkMicroservice = (factory: any) => ({
    provide: factory,
    inject: [SpiderMesh],
    useFactory: (sm: SpiderMesh) => sm.service(factory)
})
