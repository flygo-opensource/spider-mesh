
import { Observable } from "rxjs"
import type { Registry } from './Registry.js'


export type SpiderMeshErrorCode = ('MICROSERVICE_OFFLINE' | 'MICROSERVICE_NOT_FOUND' | 'MICROSERVICE_RPC_TIMEOUT');

export type SpiderMeshError = {
    code: SpiderMeshErrorCode;
    message: string;
};



export type SpiderMeshNode = {
    host: string;
    namespace: string;
    version: number;
    node_id: string;
    topics: string[];
    services: {
        [name: string]: any;
    };
    nodes: {
        [node_id: string]: number;
    };
    transporters: {
        [name: string]: any;
    };
};

export type NodesMap = {
    nodes: Map<string, SpiderMeshNode>;
    last_updated_node_id: string;
};


export type NodeMetadata<T = {}> = T & {
    host: string;
    node_id: string;
    namespace: string;
};
export type MdnsMessage<T extends NodeMetadata> = {
    hi: boolean;
    node: T;
    sender_id: string;
    forwarder_id?: string;
    receiver_id?: string;
};

export type DiscoveryEvent = {
    discovered: SpiderMeshNode;
};

export type DiscoveryTransporter = Observable<DiscoveryEvent> & {
    linkRegistry?(registry: Registry): void;
    broadcast(data: MdnsMessage<NodeMetadata>): Promise<void>;
};


export type PubsubEvent = {
    endpoints: Record<string, string | boolean | number>;
};

export type PubsubTransporter = Observable<PubsubEvent> & {
    publish<T>(topic: string, data: T): Promise<void>;
    listen<T>(topic: string): Observable<T>;
    linkRegistry?(registry: Registry): void;
};

export type RpcRoutingOptions = {
    [key: string]: string | number | boolean;
};
export type RpcOptions<T = any> = {
    service: string;
    method: string;
    args: any[];
    fallback?: T;
    timeout?: number;
    retry?: number;
    node_id?: string;
    transporter?: string |  { name?: string } 
};

export type RpcRequestPacket = {
    kind: 'request'
    request_id: string
    source_node_id: string
    target_node_id: string
    service: string
    method: string
    args: any[]
}

export type RpcResponsePacket = {
    kind: 'response'
    request_id: string
    source_node_id: string
    target_node_id: string
    data?: any
    error?: SpiderMeshError | { code?: string, message: string }
    completed?: boolean
}

export type RpcCancelPacket = {
    kind: 'cancel'
    request_id: string
    source_node_id: string
    target_node_id: string
}

export type RpcPacket = RpcRequestPacket | RpcResponsePacket | RpcCancelPacket

export type RpcMessage = {
    node_id: string;
    packet: RpcPacket;
};

export type RpcEvent = Partial<{
    rpc: RpcMessage;
    offline: string;
    endpoints: Record<string, string | boolean | number>;
}>;
export type RpcTransporter = Observable<RpcEvent> & {
    linkRegistry?(registry: Registry): void;
    send(data: RpcPacket, node_id?: string): Promise<void>;
};

export type MeshTransporter = RpcTransporter | PubsubTransporter | DiscoveryTransporter;

 