import { Subject } from "rxjs"
import { SpiderMeshNode } from "./SpiderMeshNode.js"

export type DiscoveryTransporter = Subject<SpiderMeshNode> & {
    name: `discover-${string}` 
    broadcast: <T>(data: T, ip?: string) => any
}