
export type MicroserviceError = {
    code: 'MICROSERVICE_OFFLINE' | 'MICROSERVICE_NOT_FOUND' | 'MICROSERVICE_RPC_TIMEOUT',
    message?: string
}
