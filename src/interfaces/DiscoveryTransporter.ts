import { Observable } from "rxjs"
import { SpiderMeshNode } from "./SpiderMeshNode.js"

export type DiscoveryTransporter = {
    broadcast: (metadata: SpiderMeshNode, host?: string) => any
}