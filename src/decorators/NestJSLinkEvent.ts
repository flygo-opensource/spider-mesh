import { SpiderMesh } from "../SpiderMesh.js";

export const NestJSLinkEvent = (factory: any, publish_buffer_ms?: number) => ({
    provide: factory,
    inject: [SpiderMesh],
    useFactory: (sm: SpiderMesh) => sm.link_event(factory, publish_buffer_ms)
})