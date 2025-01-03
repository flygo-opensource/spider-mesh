import { SpiderMesh } from "../SpiderMesh.js";

export const NestJSLinkMicroservice = (factory: any, wait_service_online?: boolean) => ({
    provide: factory,
    inject: [SpiderMesh],
    useFactory: (sm: SpiderMesh) => sm.linkRemoteService(factory)
})
