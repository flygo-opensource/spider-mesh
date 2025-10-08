import { SpiderMesh } from "@spider-mesh/core";
import { A } from "./serviceA.js";
import { Mdns } from "../src/Mdns.js";
import { Http2Rpc } from "../src/Http2Rpc.js";

console.log(`Master`)

const sm = new SpiderMesh()
new Http2Rpc()
new Mdns()
const service = sm.linkRemoteService(A)
console.log(service)
service.watch$().subscribe(() => {
    console.log(`Total ${service.nodes.length} nodes online of A`)
})

await service.wait$()
console.log('Ready') 