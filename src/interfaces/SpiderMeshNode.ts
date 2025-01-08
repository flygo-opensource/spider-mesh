
export type SpiderMeshNode = {
    namespace: string
    node_id: string
    ip: string
    hostname: string
    services: {
        [service: string]: object
    }
}