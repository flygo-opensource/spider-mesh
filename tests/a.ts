import { SpiderMesh } from "@spider-mesh/core";
import { A } from "./serviceA.js";
import { Http2Rpc } from "../src/Http2Rpc.js";
import { UdpDiscovery } from "../src/UdpDiscovery.js";

console.log(`Worker`)
const sm = new SpiderMesh()
new Http2Rpc()
new UdpDiscovery()




new A()


setInterval(() => { }, 1000) 