import { randomUUID } from "crypto";
import { SpiderMeshTcpTransporter } from "../src/SpiderMeshTcpTransporter.js";


const t = new SpiderMeshTcpTransporter()
t.init({ node_id: randomUUID() }) 


setInterval(() => {}, 1000) 