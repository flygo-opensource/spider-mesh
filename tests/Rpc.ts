import { firstValueFrom, from, lastValueFrom, map, mergeMap, timer, toArray } from 'rxjs'
import { Microservice } from '../src/decorators/Microservice.js'
import { SpiderMesh } from '../src/SpiderMesh.js'


@Microservice()
class ExampleService {

    constructor() {
        console.log(`Example service running`)
    }

    sum(a: number, b: number) {
        return a + b
    }

    async asyncSum(a: number, b: number) {
        await firstValueFrom(timer(1000))
        return a + b
    }

    interator() {
        return from([1, 2, 3, 4, 5])
    }

    async asyncInterator() {
        return from([1, 2, 3, 4, 5]).pipe(
            mergeMap(async d => {
                await firstValueFrom(timer(1000))
                return d
            },1)
        )
    }

    async buffer() {
        return Buffer.from('ahihihi', 'utf-8')
    }

    async nulla(){

    }

    async interatorBufferMix(){
        return from([
            {a:1, b:"s", c: Buffer.from('abc')},
            null,
            undefined,
            {v: new Set([1,2,])}
        ]).pipe(
            mergeMap(async d => {
                await firstValueFrom(timer(1000))
                return {d}
            }, 1)
        )
    }

    errror_test(){
        throw {a: {b:"c"}}
    }

    obj_errror_test(){
        throw new Error('AHIHI') 
    }


    get_time(){
        return {time: Date.now()}
    }
}

if (process.argv[2] == 'a') {
    new ExampleService()
    const sm = new SpiderMesh()
}

if (process.argv[2] == 'b') {
    console.log('Testing')
    const sm = new SpiderMesh()
    const service = await sm.link_remote_service(ExampleService, true)
    console.log(`Service online`)

    console.log(`Not found`)
    console.log(await (service as any).xxxx())

    console.log(`Batch`)
    console.log(await firstValueFrom(service.$batch_get_time().pipe(toArray())))

    console.log(`Timeout`)
    const data  =await service.$timeout(500).$fallback(7).asyncSum(1,2)
    console.log({data})


    console.log('sync sum')
    console.log({
        'sum: 1+2': await service.sum(1, 2)
    })
    console.log({
        'asyncSum: 1+2': await service.asyncSum(1, 2)
    })
    console.log('Interator')
    await lastValueFrom(service.interator().pipe(
        map(n => console.log({ n }))
    ))

    console.log('ERROR')
    try{
        await service.errror_test()
    }catch(e){
        console.log(JSON.stringify(e))
    }

    console.log('ERROR OBJ')
    try{
        await service.obj_errror_test()
    }catch(e){
        console.log(e)
    }

    console.log('Async interator')
    await lastValueFrom(service.asyncInterator().pipe(
        map(n => console.log({ n }))
    ))

    console.log('Buffer')
    const buffer = await service.buffer()
    console.log({ buffer, v: buffer.toString('utf8') })

    console.log('interatorBufferMix')
    await lastValueFrom(service.interatorBufferMix().pipe(
        map(n => console.log({ n }))
    ))
}