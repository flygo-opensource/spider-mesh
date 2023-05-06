import { SpiderMeshTransporter } from './SpiderMeshTransporter'
import { SpiderMesh } from './SpiderMesh'
import { ServiceInstanceList } from './SpiderMeshMicroservice'
import { BuiltinTransporter } from './BuiltinTransporter'

export const SpiderMeshNestJSProvider = (
    transporters: Array<{ new(...arsg: any): SpiderMeshTransporter }> = [BuiltinTransporter]
) => ({
    provide: [],
    useFactory: async (ms: SpiderMesh) => {
        for (const transporter of transporters) {
            await ms.add_transporter(transporter)
        }
        for (const { name, instance } of ServiceInstanceList) {
            await ms.active_local_service(name, instance)
        }
    },
    inject: [SpiderMesh]
})