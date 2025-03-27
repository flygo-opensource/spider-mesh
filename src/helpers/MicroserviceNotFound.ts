import { MicroserviceException } from "./MicroserviceException.js"

export class MicroserviceNotFound extends MicroserviceException {
    public static readonly code = 'MicroserviceNotFound'
    constructor() {
        super({ code: MicroserviceNotFound.code })
    }
}