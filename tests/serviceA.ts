import { LimitConcurrency, Microservice, MicroserviceException } from "@spider-mesh/core";
import { randomUUID } from "crypto";
import { interval, take, tap, map, firstValueFrom, timer } from "rxjs";


const UUID = randomUUID().split('-').pop()

@Microservice()
export class A {
      sum(a: number, b: number) {
        console.log({ a, b, n: Date.now() })  
        // throw new MicroserviceException({ a: 'str' + (a + b), code: 'INVAILD_KEYWORD' })
        return a + b
    }

    async asyncSUm(a: number, b: number) {
        await firstValueFrom(timer(1000))
        return a + b
    }

    who() {
        return { UUID }
    }


     xxx() {
        return interval(1000).pipe(
            tap(n => console.log({ n })),
            map((n, i) => {
                const str = {
                    n,
                    v: new Array(1 + n).fill(0).map(a => `a`)
                }
                return str
            }),
            take(4)
        )
    }

    async interator() {
        return interval(1000).pipe(
            tap(n => console.log({ n })),
            map((n, i) => {
                const str = {
                    n,
                    v: new Array(1 + n).fill(0).map(a => `a`)
                }
                return str
            }),
            take(4)
        )
    }

    @LimitConcurrency(1)
    async limitTest(a: number) {
        return interval(1000).pipe(
            take(a),
            tap(v => console.log({ a, v: v + 1 })),
            map(v => ({ a, v: v + 1 }))
        )
    }

}
