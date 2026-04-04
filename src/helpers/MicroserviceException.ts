import { SpiderMeshError } from "@spider-mesh/types"



export class MicroserviceException extends Error {
    constructor(public code: SpiderMeshError, public message: string = code) {
        super(message)
        super.name = code
    }
}


export class MicroserviceOfflineException extends MicroserviceException {
    constructor() {
        super('MICROSERVICE_OFFLINE')
    }
}

export class MicroserviceNotFound extends MicroserviceException {
    constructor() {
        super('MICROSERVICE_NOT_FOUND')
    }
}

export class MicroserviceRpcTimeout extends MicroserviceException {
    constructor() {
        super('MICROSERVICE_RPC_TIMEOUT')
    }
}