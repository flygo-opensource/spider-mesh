import { ServiceMetadata } from "../interfaces/SpiderMeshNode.js"
import { SpiderMesh } from "../SpiderMesh.js"

export const CustomSpiderMesh = (fn: () => Promise<ServiceMetadata>) => ({
    provide: Symbol.for('NestJSInjectMetadata'),
    useFactory: async () => {
        const m = await fn()
        return new SpiderMesh(m)
    }
})

