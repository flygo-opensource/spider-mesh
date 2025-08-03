import { SpiderMesh } from "../SpiderMesh.js";

export const NestJSLinkEvent = (factory: any) => ({
    provide: factory,
    inject: [SpiderMesh],
    useFactory: (sm: SpiderMesh) => sm.linkEvent(factory)
})