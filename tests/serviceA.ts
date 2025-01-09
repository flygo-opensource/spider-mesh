import { Microservice } from "@spider-mesh/core";
import { from } from "rxjs";

@Microservice()
export class A {
    sum(a: number, b: number) {
        return a + b
    }

    xxx() {
        return from([1, 2, 3, 4, 5])
    }
}