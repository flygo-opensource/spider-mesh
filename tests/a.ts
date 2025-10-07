import { SpiderMesh } from "@spider-mesh/core";
import { A } from "./serviceA.js";
import { Http2Rpc } from "../src/Http2Rpc.js";
import { Mdns } from "../src/Mdns.js";

console.log(`Worker`)
const sm = new SpiderMesh()
new Http2Rpc(sm)
new Mdns(sm)


new A()


setInterval(() => { }, 1000) 