import { Registry, SpiderMesh } from '@spider-mesh/core'
import { createTransporters } from './createTransporters.js'

export function createMesh() {
    const registry = new Registry()
    const mesh = new SpiderMesh()

    for (const transporter of createTransporters(registry)) {
        mesh.registerTransporter(transporter)
    }

    return { mesh, registry }
}