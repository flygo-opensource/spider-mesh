

export type SpiderMeshNode = {
    ips: string[]
    host: string
    namespace: string
    version: number
    node_id: string
    transporters: {
        [method: string]: string | number | boolean
    }
    online?: boolean
    topics: string[]
    services: {
        [name: string]: any
    }
    nodes: { [node_id: string]: number }
}


