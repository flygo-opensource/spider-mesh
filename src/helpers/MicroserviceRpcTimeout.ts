import { MicroserviceException } from "./MicroserviceException.js"

export class MicroserviceRpcTimeout extends MicroserviceException {
    public static readonly code = 'MicroserviceRpcTimeout'
    constructor() {
        super({ code: MicroserviceRpcTimeout.code })
    }
}