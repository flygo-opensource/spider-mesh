import { MicroserviceException, SpiderMesh } from "@spider-mesh/core";
import { A } from "./serviceA.js"; 
import { lastValueFrom } from "rxjs/internal/lastValueFrom";
import { tap } from "rxjs";
import { Mdns } from "../src/Mdns.js";
import { Rpc } from "../src/Rpc.js";
import path from "path";
import { fileURLToPath } from "url";


const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

console.log(`${path.basename(__dirname)}/${path.basename(__filename)}`);

console.log(`Master`)

const sm = new SpiderMesh()
new Rpc(sm)
new Mdns(sm)
const service = sm.service(A)

service.watch$().subscribe(() => {
    console.log(`Total ${service.nodes.length} nodes online of A`)
})

await service.wait$()
console.log('Ready')

 

// console.log('Running')
// service.__batch__sum(1,2).subscribe(n => {
//     console.log({n})
// })
let i = 1
setInterval(async () => {
    try{
        const res = await service.sum(1,i++)
        console.log({res})
    }catch(e){
        if(e instanceof MicroserviceException){
            e.stack = 'VKL'
            console.error('???', e)
        } 
        console.error({e})
    }
}, 2000)

// service.set({ fallback: -1 }).xxx().subscribe(console.log)
// service.set({ fallback: -1 }).xxx().subscribe(console.log)
// service.set({ fallback: -1 }).xxx().subscribe(console.log)

//  service.xxx().subscribe(console.log) 


// for (let i = 1; i <= 3; i++) {
//     lastValueFrom(
//         service.limitTest(i).pipe(
//             tap(console.log)
//         )
//     )
// }