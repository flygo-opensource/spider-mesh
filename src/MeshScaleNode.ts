import { MeshScaleTransporter } from "./MeshScaleTransporter"

export type MeshScaleNodeMetadata = {
    id: string
    namespace: string
    ip_addresses: string[]
    last_online: number
    active: boolean
    offline: boolean
    services: string[]
    linked: string[]
}

export type MeshScaleNode = MeshScaleNodeMetadata & {
    transporters?: Map<string, MeshScaleTransporter>

}