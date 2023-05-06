import { SpiderMeshTransporter } from './SpiderMeshTransporter'
import { SpiderMesh } from './SpiderMesh'
import { ServiceInstanceList } from './decorators/SpiderMeshMicroservice'
import { BuiltinTransporter } from './BuiltinTransporter'

export const SpiderMeshNestJSProvider = (
    transporters: Array<{ new(...arsg: any): SpiderMeshTransporter }> = [BuiltinTransporter]
) => ({
    provide: [],
    useFactory: async (ms: SpiderMesh) => {
        for (const transporter of transporters) {
            await ms.add_transporter(transporter)
        }

        // Active event requester
        for (const { instance, event_subscribers } of ServiceInstanceList) {
            for (const { event, method, requests } of event_subscribers) {
                requests.on('data', data => ms.publish(event, data))
            }
        }

        // Active local services
        for (const { name, instance } of ServiceInstanceList) {
            await ms.active_local_service(name, instance)
        }

        // Active event subscribers
        for (const { instance, event_subscribers } of ServiceInstanceList) {
            for (const { event, method, requests } of event_subscribers) {
                ms.subscribe(event, (_, data) => instance[method]?.(data))
            }
        }

        // Active ready hook
        for (const { instance, ready_hook_methods } of ServiceInstanceList) {
            for (const method of ready_hook_methods) {
                await instance[method]?.()
            }
        }
    },
    inject: [SpiderMesh]
})