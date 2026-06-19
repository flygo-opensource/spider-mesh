
import { Observable } from "rxjs"


export type SpiderMeshErrorCode = ('MICROSERVICE_OFFLINE' | 'MICROSERVICE_NOT_FOUND' | 'MICROSERVICE_RPC_TIMEOUT');

export type SpiderMeshError = {
    code: SpiderMeshErrorCode;
    message: string;
};



export type BuildInfo = {
    version?: string
    git_tag?: string
    git_branch?: string
    git_commit?: string
    build_time?: number
    environment?: string
    runtime?: string
    tags?: Record<string, string>
}

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
    build?: BuildInfo;
};

export type NodesMap = {
    nodes: Map<string, SpiderMeshNode>;
    last_updated_node_id: string;
};

/**
 * Minimal node identity used for availability/enumeration queries.
 * `SpiderMeshNode` is structurally assignable to `NodeRef`, so any peer table
 * (e.g. tcp's `Registry`) can satisfy a `ServiceDirectory` without conversion.
 */
export type NodeRef = Pick<SpiderMeshNode, 'node_id' | 'host' | 'services' | 'transporters'>;

/**
 * Availability/discovery surface a transporter MAY expose so core can answer
 * `wait()` / `watch()` / `nodes` without owning a registry. A transporter that
 * implements this declares itself the source of truth for "which nodes serve a
 * service". Core detects it structurally (`typeof t.watchService === 'function'`)
 * and merges across all transporters that provide it.
 */
export type ServiceDirectory = {
    /** Stream the set of nodes serving `service`. Emit `[]` when none/unknown. */
    watchService(service: string): Observable<NodeRef[]>;
    /** Synchronous snapshot of nodes serving `service`. Return `[]` if the transport cannot enumerate. */
    listNodes(service: string): NodeRef[];
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


export type PubsubEvent = {
    endpoints: Record<string, string | boolean | number>;
};

export type PubsubTransporter = Observable<PubsubEvent> & {
    publish<T>(topic: string, data: T): Promise<void>;
    listen<T>(topic: string): Observable<T>;
};

export type RpcRoutingOptions = {
    [key: string]: string | number | boolean;
};
export type TransporterSelector = string | { name?: string } | (abstract new (...args: any[]) => any)
export type RpcOptions<T = any> = {
    service: string;
    method: string;
    args: any[];
    fallback?: T;
    timeout?: number;
    retry?: number;
    node_id?: string;
    transporter?: TransporterSelector
};

export type RpcRequestPacket = {
    kind: 'request'
    request_id: string
    service: string
    method: string
    args: any[]
    sender_node_id: string
    destination_node_id?: string
}

export type RpcResponsePacket = {
    kind: 'response'
    request_id: string
    data?: any
    error?: SpiderMeshError | { code?: string, message: string }
    completed?: boolean
    destination_node_id?: string
}

export type RpcCancelPacket = {
    kind: 'cancel'
    request_id: string
    destination_node_id?: string
}


export type RpcEvent = Partial<{
    rpc: RpcRequestPacket | RpcResponsePacket | RpcCancelPacket;
    offline: string;
    endpoints: Record<string, string | boolean | number>;
}>;
export type RpcTransporter = Observable<RpcEvent> & {
    send(data: RpcRequestPacket | RpcResponsePacket): Promise<{ cancel: () => void }>;
    /**
     * Reachability probe (required). Returns `true` when this transporter can currently
     * deliver an RPC for `service` (and, when given, to the specific `node_id`).
     *
     * Core consults this in `#selectRpcTransport` so that — when more than one RPC
     * transporter is registered — a default RPC is dispatched through a transporter
     * that can actually reach the provider, instead of blindly through the
     * first-registered one. MUST be side-effect free (no round-robin advancing etc.).
     * A transporter that cannot enumerate reachability may return `true` to remain a
     * candidate (it then relies on `send` to surface the real error).
     */
    canRoute(service: string, node_id?: string): boolean;
};

export type MeshTransporter = RpcTransporter | PubsubTransporter | DiscoveryTransporter;

