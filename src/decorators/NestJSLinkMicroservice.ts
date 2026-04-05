import { RemoteServiceLinker } from "../RemoteService.js";
import { SpiderMesh } from "../SpiderMesh.js";

export const NestJSLinkMicroservice = (factory: any) => ({
    provide: factory,
    inject: [SpiderMesh],
    useFactory: async (sm: SpiderMesh) => {
        const service = RemoteServiceLinker.link(sm, { service: factory.name })
        return service
    }
})
