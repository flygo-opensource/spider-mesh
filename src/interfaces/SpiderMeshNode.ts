export type ServiceMetadata = {
    [key: string]: string | number | boolean
}

export type SpiderMeshNodeMetadata = {
    node_id: string
    local_transporter_id: string
    remote_transporter_id: string
    name: string
    version: string,
    path: string
    uptime: number,
    plaform: string,
    hostname: string,
    node_version: string
    namespace: string
    public_ip: string | null,
    ip_addresses: string[]
    last_online: number
    online: boolean
    services: { [service_id: string]: { metadata: any } }
    linked: string[]
    isolated_nodes: string[]
    isolated: boolean
}

export type SpiderMeshNode = SpiderMeshNodeMetadata