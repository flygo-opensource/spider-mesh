import { SpiderMesh } from "@spider-mesh/core";
import { A } from "./serviceA.js";
import { SpiderMeshTcpTransporter } from "../src/SpiderMeshTcpTransporter.js";
import { firstValueFrom, interval, timer } from "rxjs";

console.log(`Master`)

const sm = new SpiderMesh()
new SpiderMeshTcpTransporter(sm)
const a = sm.linkRemoteService(A)

// await a.$watch().subscribe(() => {
//     const nodes = a.$nodes
//     console.log(`Total ${nodes.length} node`)
//     return nodes.length >= 1
// })

while(true){
    await firstValueFrom(timer(1000))
    const s = await a.who()
    console.log({ from: s })
}
// console.log('Service online');
// a.xxx().subscribe(console.log)



// const d = await a.__batch__sum(1, 2)
// console.log(JSON.stringify(d, null, 2))