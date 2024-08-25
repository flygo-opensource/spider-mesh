import { ServiceMetadata } from "../interfaces/SpiderMeshNode.js"
import { SpiderMesh } from "../SpiderMesh.js"

export const NestJSInjectMetadata = (fn: () => Promise<ServiceMetadata>) => ({
    provide: Symbol.for('NestJSInjectMetadata'),
    inject: [SpiderMesh],
    useFactory: async (sm: SpiderMesh) => {
        try {
            const m = await fn()
            sm.set_metadata(m, true)
        } catch (e) {

        }
    }
})

