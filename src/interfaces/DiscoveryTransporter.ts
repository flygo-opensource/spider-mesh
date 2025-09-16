import { Subject } from "rxjs"
import { SpiderMeshNode } from "./SpiderMeshNode.js"


export class DiscoveryTransporter extends Subject<SpiderMeshNode> {
    broadcast: (data: SpiderMeshNode, target?: string) => Promise<void>
}