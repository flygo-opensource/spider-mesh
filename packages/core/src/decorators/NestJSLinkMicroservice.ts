import { RemoteServiceLinker } from "../RemoteService.js";
import { SpiderMesh } from "../SpiderMesh.js";
import type { TransporterSelector } from '../types.js'

export const NestJSLinkMicroservice = (factory: any, transporter?: TransporterSelector) => ({
    provide: factory,
    inject: [SpiderMesh],
    useFactory: (sm: SpiderMesh) => RemoteServiceLinker.link(sm, { service: factory.name, transporter })
})
