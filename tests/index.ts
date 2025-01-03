// import { filter, firstValueFrom, from, last, lastValueFrom, map, merge, mergeMap, take, tap, timer, toArray } from 'rxjs'
// import { SpiderMesh } from '../src/SpiderMesh.js'
// import { ExampleService } from './ExampleService.js'
// import { before, it, describe } from 'node:test'
// import { strictEqual, deepStrictEqual, equal } from 'assert'
// import { RemoteService } from '../src/index.js'

import { SpiderMesh } from "../src/SpiderMesh.js";









const a = new SpiderMesh()

// if (process.argv[2] == 'a') {
//     describe('Test RPC running service', {}, () => {
//         const sm = new SpiderMesh()
//         const service = new ExampleService(sm)
//         strictEqual(service.online, true, 'Service must online')
//     })
// }

// if (process.argv[2] == 'b') {




//     describe('Test RPC requester', undefined, () => {
//         let service: RemoteService<ExampleService>

//         before(async () => {
//             const sm = new SpiderMesh()
//             service = await sm.link_remote_service(ExampleService)
//             // console.log(`Listen new nodes`)
//             // service.$watch().pipe(
//             //     tap(node => console.log({ id: node.node_id, online: node.online })),
//             // ).subscribe() 
//         })



//         it(`Wait service online`, async () => {
//             await service.$wait_service_online()
//             strictEqual(service.$list_nodes().length >= 1, true)
//         })

//         it(`Wait 2 service online`, async () => {

//             await service.$wait_service_online()
//             strictEqual(service.$list_nodes().length >= 2, true)

//         })

//         it(`Test not found`, async () => {
//             let i = 0
//             try {
//                 await (service as any).sumdfd(1, 2)
//                 i = 1
//             } catch (e) {
//                 i = 2
//             }
//             strictEqual(i, 2)
//         })

//         it(`Call sync method`, async () => {
//             strictEqual(await service.sum(1, 2), 3)
//         })

//         it(`Call async sync method`, async () => {
//             strictEqual(await service.asyncSum(1, 2), 3)
//         })

//         it(`Interator`, async () => {
//             let i = 1
//             service.interator().subscribe(n => {
//                 strictEqual(i++, n)
//             })
//         })

//         it(`Async interator`, async () => {
//             let i = 1
//             service.interator().subscribe(n => {
//                 strictEqual(i++, n)
//             })
//         })

//         it(`Buffer`, async () => {
//             strictEqual((await service.buffer()).toString('utf8'), 'ahihihi')
//         })

//         it(`Interator with buffer`, async () => {
//             const items = [
//                 null,
//                 undefined,
//                 { v: new Set([1, 2,]) },
//                 { a: 1, b: "s", c: Buffer.from('abc') },
//             ]
//             await lastValueFrom(service.interatorBufferMix().pipe(
//                 mergeMap(async (data, i) => {
//                     deepStrictEqual(data, items[i])
//                 })
//             ))
//         })

//         it(`Throw test obj error`, async () => {
//             try {
//                 await service.errror_test()
//             } catch (e) {
//                 deepStrictEqual(e, { a: { b: "c" } })
//                 return
//             }
//             deepStrictEqual("Result", "Not throw error")
//         })

//         it(`Throw test message error`, async () => {
//             try {
//                 await service.obj_errror_test()
//             } catch (e) {
//                 deepStrictEqual((e as Error).message, 'AHIHI')
//                 return
//             }
//             deepStrictEqual("Result", "Not throw error")
//         })

//         it(`Retry`, async () => {

//             const d = await service.$retry(5).retry()
//             strictEqual(d, 3)

//         })

//         it(`RRR`, async () => {
//             const ids = service.$list_nodes().map((a, i) => a.node_id)
//             const called = ids.map(() => 0)
//             for (let i = 1; i <= ids.length * 10; i++) {
//                 const id = await service.get_id()
//                 const j = ids.findIndex(a => a == id)
//                 called[j]++
//             }
//             console.log({called})
//             for (let i = 1; i < ids.length; i++) {
//                 strictEqual(called[i - 1], called[i])
//             }
//         })

//         it(`ID request`, async () => {
//             await firstValueFrom(timer(5000))
//             for (const node of service.$list_nodes()) {
//                 await service.$node_id(node.node_id).get_id()
//             }
//         })
//     })




// }


// if (process.argv[2] == 'c') {

//     const sm = new SpiderMesh()

// }