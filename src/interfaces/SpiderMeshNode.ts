
export type SpiderMeshNodeMetadata = {
    id: string 
    namespace: string
    public_ip: string | null,
    ip_addresses: string[]
    last_online: number
    active: boolean
    offline: boolean
    services: string[]
    linked: string[]
    isolate: boolean
    revalidate_on_join?: boolean
}

export type SpiderMeshNode = SpiderMeshNodeMetadata