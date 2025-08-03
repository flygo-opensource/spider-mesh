import { SpiderMesh } from "@spider-mesh/core";
import { A } from "./serviceA.js";
import { Rpc } from "../src/Rpc.js";
import { Mdns } from "../src/Mdns.js";

console.log(`Worker`)
const sm = new SpiderMesh()
new Rpc(sm)
new Mdns()
new A()


setInterval(() => { }, 1000) 