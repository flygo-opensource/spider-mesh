import { randomUUID } from "crypto";
import { SpiderMeshTcpTransporter } from "../src/SpiderMeshTcpTransporter.js";

console.log(`Running`)

const t = new SpiderMeshTcpTransporter()
t.init({ node_id: randomUUID(), services: [] })


setInterval(() => { }, 1000) 