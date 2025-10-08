import { SpiderMesh } from "@spider-mesh/core";
import { A } from "./serviceA.js";
import { Http2Rpc } from "../src/Http2Rpc.js";
import { Mdns } from "../src/Mdns.js";

console.log(`Worker`)
const sm = new SpiderMesh()
new Http2Rpc()
new Mdns()




new A()


setInterval(() => { }, 1000) 