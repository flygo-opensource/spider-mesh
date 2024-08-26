import { Microservice } from "@spider-mesh/core";

@Microservice()
export class A {
    sum(a: number, b: number) {
        return a + b
    }
}