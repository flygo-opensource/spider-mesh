
export type SpiderMeshNode = {
    namespace: string
    node_id: string
    ip: string
    hostname: string
    port: number
    services: {
        [service: string]: object
    }
}