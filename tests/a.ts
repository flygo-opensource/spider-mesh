import { Microservice, SpiderMesh } from "@spider-mesh/core";
import { SpiderMeshTcpTransporter } from "../src/SpiderMeshTcpTransporter.js";
import { A } from "./serviceA.js";




const sm = new SpiderMesh()
const tspt = new SpiderMeshTcpTransporter( ) 
new A()