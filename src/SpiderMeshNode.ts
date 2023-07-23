import { SpiderMeshTransporter } from "./SpiderMeshTransporter"

export type SpiderMeshNodeMetadata = {
    id: string
    namespace: string
    public_ip: string,
    ip_addresses: string[]
    last_online: number
    active: boolean
    offline: boolean
    services: string[]
    linked: string[]
    isolate: boolean
}

export type SpiderMeshNode = SpiderMeshNodeMetadata