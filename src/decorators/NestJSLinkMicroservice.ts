import { RemoteServiceLinker } from "../RemoteService.js";
import { SpiderMesh } from "../SpiderMesh.js";

export const NestJSLinkMicroservice = (factory: any, transporter: string | { name?: string | undefined; }) => ({
    provide: factory,
    inject: [SpiderMesh],
    useFactory: async (sm: SpiderMesh) => {
        const service = RemoteServiceLinker.link(sm, { service: factory.name, transporter })
        return service
    }
})
