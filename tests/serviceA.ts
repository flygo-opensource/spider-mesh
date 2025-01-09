import { Microservice } from "@spider-mesh/core";
import { from, interval, take } from "rxjs";

@Microservice()
export class A {
    sum(a: number, b: number) {
        console.log({a,b,n:Date.now()})
        return a + b
    }

    xxx() {
        return interval(1000).pipe(
            take(10)
        )
    }
}