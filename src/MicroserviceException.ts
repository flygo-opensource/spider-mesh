import { SpiderMeshError } from "node_modules/@spider-mesh/types/build/src/errors.js";



export class MicroserviceException extends Error {
    constructor(public code: SpiderMeshError, public readonly message: string = code) {
        super(message)
        super.name = code
    }
}