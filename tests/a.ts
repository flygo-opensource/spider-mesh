import { SpiderMeshTcpTransporter } from "../src/SpiderMeshTcpTransporter.js";
import { SpiderMesh } from "@spider-mesh/core";

console.log(`Running`)
const sm = new SpiderMesh()
const t = new SpiderMeshTcpTransporter(sm)
// t.init({ node_id: randomUUID(), services: [], events: ['Abc'] })


setInterval(() => { }, 1000) 