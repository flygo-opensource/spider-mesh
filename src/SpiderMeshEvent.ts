import { Observable } from "rxjs"
import { SpiderMeshTransporterEvent } from "./SpiderMeshTransporter.js"

export type EventHub<T> = {
    publish: (data: T) => Promise<void>,
    listen: () => Observable<SpiderMeshTransporterEvent<T>>
}

export function createSpiderMeshEvent<T = {}>() {
    return class { } as {
        new(): EventHub<T>
    }
}


export type EventDataType<T extends EventHub<any>> = Parameters<T['publish']>[0] 