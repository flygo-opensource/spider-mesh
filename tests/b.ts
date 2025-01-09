import { SpiderMesh } from "@spider-mesh/core";
import { A } from "./serviceA.js";
import { SpiderMeshTcpTransporter } from "../src/SpiderMeshTcpTransporter.js";



const sm = new SpiderMesh()
new SpiderMeshTcpTransporter(sm)
const a = sm.linkRemoteService(A)
await a.$wait(nodes => {
    console.log(`Total ${nodes.length} node`)
    return nodes.length >= 2
})
console.log('Service online');
const d = await a.__batch__sum(1, 2)
console.log(JSON.stringify(d, null, 2))