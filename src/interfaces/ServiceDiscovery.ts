import { Observable } from "rxjs";

export type ServiceDiscoveryMetadata = {
    ip: string,
    hostname: string,
    port: number,
    node_id: string,
    status: 'online' | 'offline'
}

export interface ServiceDiscovery {
    $nodes: Observable<ServiceDiscoveryMetadata>
}