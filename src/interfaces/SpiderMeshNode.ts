export type ServiceMetadata = {
    [key: string]: string | number | boolean
}

export type SpiderMeshNodeMetadata = {
    id: string
    namespace: string
    public_ip: string | null,
    ip_addresses: string[]
    last_online: number
    active: boolean
    online: boolean
    services: { [service_id: string]: { instance: any, metadata: ServiceMetadata } }
    linked: string[]
    isolate: boolean
    revalidate_on_join?: boolean

}

export type SpiderMeshNode = SpiderMeshNodeMetadata