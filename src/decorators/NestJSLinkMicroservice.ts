import { RemoteService, RemoteServiceLinker } from "src/abstracts/RemoteService.js";
import { SpiderMesh } from "../SpiderMesh.js";

export const NestJSLinkMicroservice = (factory: any) => ({
    provide: factory,
    inject: [SpiderMesh],
    useFactory: (sm: SpiderMesh) => RemoteServiceLinker.link(sm, { service: factory.name })
})
