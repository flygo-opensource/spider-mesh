import { SpiderMesh } from "@spider-mesh/core"; 
import { A } from "./serviceA.js";
import { SpiderMeshTcpTransporter } from "../src/SpiderMeshTcpTransporter.js";



const sm = new SpiderMesh()
sm.$nodes_monitor.subscribe(console.log)
new SpiderMeshTcpTransporter( )
const a = await sm.link_remote_service(A)
console.log({a})
await a.$wait_service_online()
console.log('Service online')
const result = await a.sum(1, 2)
console.log({result})