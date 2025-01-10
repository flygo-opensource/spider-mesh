import { Microservice } from "@spider-mesh/core";
import { randomUUID } from "crypto";
import { from, interval, take } from "rxjs";


const UUID = randomUUID().split('-').pop()

@Microservice()
export class A {
    sum(a: number, b: number) {
        console.log({ a, b, n: Date.now() })
        return a + b
    }

    who() {
        return { UUID }
    }

    xxx() {
        return interval(1000).pipe(
            take(10)
        )
    }
}