import { firstValueFrom, timer, from, mergeMap } from "rxjs"
import { Microservice } from "../src/index.js"

@Microservice()
export class ExampleService {
    online = true

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
            }, 1)
        )
    }

    async buffer() {
        return Buffer.from('ahihihi', 'utf-8')
    }

    async nulla() {

    }

    async interatorBufferMix() {
        return from([
            null,
            undefined,
            { v: new Set([1, 2,]) },
            { a: 1, b: "s", c: Buffer.from('abc') },
        ]).pipe(
            mergeMap(async d => {
                await firstValueFrom(timer(100))
                return d
            }, 1)
        )
    }

    errror_test() {
        throw { a: { b: "c" } }
    }

    obj_errror_test() {
        throw new Error('AHIHI')
    }


    i = 1
    retry() {
        this.i++
        console.log({ i: this.i })
        if (this.i % 3 == 0) {
            const a = this.i
            this.i = 0
            return a
        }
        throw {e: 1}
    }

    get_id(){
        const id = process.argv[3]
        console.log(`Call me `, {id})
        return id 
    }
}
