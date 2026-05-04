
import { Observable } from "rxjs"


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
    broadcast(data: MdnsMessage<NodeMetadata>): Promise<void>;
};

export type PubsubTransporter = {
    publish<T>(topic: string, data: T): Promise<void>;
    listen<T>(topic: string): Observable<T>;
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
    send(data: RpcPacket, node: SpiderMeshNode): Promise<void>;
};

 