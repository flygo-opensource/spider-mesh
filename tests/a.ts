import { SpiderMeshTcpTransporter } from "../src/SpiderMeshTcpTransporter.js";
import { Microservice, SpiderMesh } from "@spider-mesh/core";
import { A } from "./serviceA.js";

console.log(`Worker`)
const sm = new SpiderMesh()
new SpiderMeshTcpTransporter(sm)
const a = new A()

setInterval(() => { }, 1000) 