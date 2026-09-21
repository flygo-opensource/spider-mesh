
/** Payload lỗi do code service chủ động ném và truyền về caller. */
export type MicroserviceError = {
    code: 'MICROSERVICE_OFFLINE' | 'MICROSERVICE_NOT_FOUND' | 'MICROSERVICE_RPC_TIMEOUT',
    message?: string
}
