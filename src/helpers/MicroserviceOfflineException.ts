import { MicroserviceException } from "./MicroserviceException.js"

export class MicroserviceOfflineException extends MicroserviceException {
    public static readonly code = 'Microservice Offline Exception'
    constructor() {
        super(MicroserviceOfflineException.code, undefined)
    }
}