import { BehaviorSubject, Observable, Subject } from "rxjs"
import { SpiderMeshNode } from "./SpiderMeshNode.js"


export abstract class DiscoveryTransporter {
    abstract link(metadata$: BehaviorSubject<SpiderMeshNode>): Observable<SpiderMeshNode> 
}