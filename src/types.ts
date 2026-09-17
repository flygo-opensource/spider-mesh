
import { Observable } from "rxjs"
import type { Topology } from './Topology.js'


/** Các mã lỗi RPC chuẩn mà caller có thể xử lý ổn định. */
export type SpiderMeshErrorCode = ('MICROSERVICE_OFFLINE' | 'MICROSERVICE_NOT_FOUND' | 'MICROSERVICE_RPC_TIMEOUT');

/** Dạng lỗi được truyền qua wire giữa hai SpiderMesh node. */
export type SpiderMeshError = {
    code: SpiderMeshErrorCode;
    message: string;
};



/** Thông tin build tùy chọn được quảng bá cùng node để quan sát và debug. */
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

/** Snapshot đầy đủ mô tả một node, service và endpoint mà node đang cung cấp. */
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

/** Cấu trúc map node tương thích với API cũ. */
export type NodesMap = {
    nodes: Map<string, SpiderMeshNode>;
    last_updated_node_id: string;
};

/** View tối thiểu của node trả về từ API availability/enumeration. */
export type NodeRef = Pick<SpiderMeshNode, 'node_id' | 'host' | 'services' | 'transporters'>;

/**
 * Nguồn availability cũ, chỉ giữ lại để các package ở phiên bản chuyển tiếp còn biên dịch.
 * Code mới nên đọc danh sách node trực tiếp từ `Topology`.
 *
 * @deprecated Dùng `Topology` thay thế.
 */
export type AvailabilitySource = {
    watchService(service: string): Observable<NodeRef[]>;
    listNodes(service: string): NodeRef[];
};

/** Cấu hình chọn node của riêng một RPC request. */
export type RpcRoutingOptions =
    | {
        strategy: 'round-robin';
        /** Tách state round-robin thành nhiều nhóm độc lập. */
        state_key?: string;
    }
    | {
        strategy: 'random';
    }
    | {
        strategy: 'consistent-hash';
        /** Khóa affinity, ví dụ user_id hoặc tenant_id. */
        key: string;
        /** Tách hash-ring thành nhiều nhóm độc lập. */
        state_key?: string;
    }
    | {
        strategy: 'least-active';
        /** Tách bộ đếm request đang chạy thành nhiều nhóm độc lập. */
        state_key?: string;
    };

/** Tên ổn định do chính transporter khai báo. */
export type TransporterSelector = string

/** Các tùy chọn của một lần gọi RPC. */
export type RpcOptions<T = any> = {
    service: string;
    method: string;
    args: any[];
    fallback?: T;
    timeout?: number;
    retry?: number;
    node_id?: string;
    routing?: RpcRoutingOptions;
    transporter?: TransporterSelector
};

/** Packet yêu cầu RPC được transporter truyền đến node đích. */
export type RpcRequestPacket = {
    kind: 'request'
    request_id: string
    service: string
    method: string
    args: any[]
    sender_node_id: string
    destination_node_id?: string
    routing?: RpcRoutingOptions
}

/** Packet phản hồi RPC; một request streaming có thể nhận nhiều packet này. */
export type RpcResponsePacket = {
    kind: 'response'
    request_id: string
    data?: any
    error?: SpiderMeshError | { code?: string, message: string }
    completed?: boolean
    destination_node_id?: string
    /**
     * Node đã thực sự trả lời. Với request round-robin (relay tự chọn provider), đây là
     * cách duy nhất caller biết phải đóng stream nào khi node đó offline giữa chừng.
     */
    sender_node_id?: string
}

/** Packet hủy request đang chạy ở node đích. */
export type RpcCancelPacket = {
    kind: 'cancel'
    request_id: string
    destination_node_id?: string
}


/** Các tín hiệu mà RPC transporter phát ngược về Core. */
export type RpcEvent = Partial<{
    rpc: RpcRequestPacket | RpcResponsePacket | RpcCancelPacket;
    offline: string;
    endpoints: Record<string, string | boolean | number>;
}>;

/** Yêu cầu kiểm tra một service có thể nhận RPC qua transporter hay không. */
export type RpcProbeRequest = {
    service: string;
    node_id?: string;
    timeout?: number;
};

/** Kết quả probe chỉ chứng minh reachability, không đại diện cho toàn bộ topology. */
export type RpcProbeResult = {
    reachable: boolean;
    node_id?: string;
    latency?: number;
};

/** Context Core truyền cho transporter khi đăng ký vào SpiderMesh. */
export type RpcTransporterContext = {
    topology?: Topology;
    localNode$: Observable<SpiderMeshNode>;
    hasLocalService(service: string): boolean;
};

/** Kết quả teardown tối thiểu mà Discovery có thể trả về cho Topology. */
export type TopologyDiscoveryBinding = {
    unsubscribe(): void;
};

/** Context hai chiều giữa một Discovery implementation và Topology. */
export type TopologyDiscoveryContext = {
    localNode$: Observable<SpiderMeshNode>;
    upsertRemote(node: SpiderMeshNode): void;
    removeRemote(node_id: string): void;
};

/** Kết quả Discovery xác minh một node bị nghi ngờ offline. */
export type TopologyNodeVerificationResult = 'alive' | 'dead' | 'unknown';

/**
 * Port generic mà Topology dùng để kết nối Discovery.
 * Core chỉ biết contract này, không phụ thuộc UDP, WebSocket hay Kubernetes.
 */
export type TopologyDiscovery = {
    bind(context: TopologyDiscoveryContext): TopologyDiscoveryBinding | void;
    /**
     * Xác minh membership khi một transporter báo endpoint unreachable.
     * Discovery không chắc chắn phải trả `unknown`, không được đoán node đã chết.
     */
    verify?(node_id: string): Promise<TopologyNodeVerificationResult>;
    close?(): void | Promise<void>;
};

/** Reachability của một endpoint transporter, tách biệt với membership của node. */
export type TopologyReachabilityStatus = 'reachable' | 'suspect' | 'unreachable';

/** Báo cáo reachability do transporter gửi vào Topology. */
export type TopologyReachabilityReport = {
    node_id: string;
    transporter: string;
    status: TopologyReachabilityStatus;
    reason?: string;
    observed_at?: number;
};

/** Event trạng thái realtime mà Topology phát cho monitoring và availability. */
export type TopologyEvent =
    | {
        type: 'node-online';
        node: SpiderMeshNode;
        observed_at: number;
    }
    | {
        type: 'node-offline';
        node: SpiderMeshNode;
        reason: 'discovery' | 'verification' | 'stale';
        observed_at: number;
    }
    | {
        type: 'endpoint-suspect' | 'endpoint-unreachable' | 'endpoint-recovered';
        node_id: string;
        transporter: string;
        reason?: string;
        observed_at: number;
    };

/** Yêu cầu Topology chọn node cho một transporter cụ thể. */
export type TopologyRouteRequest = {
    service: string;
    transporter: string;
    node_id?: string;
    routing?: RpcRoutingOptions;
    exclude_node_ids?: string[];
};

/** Route đã được Topology resolve từ logical service sang một node cụ thể. */
export type TopologyRoute = {
    node: SpiderMeshNode;
    endpoint: unknown;
    state_key?: string;
};

/** Feedback của transporter để Topology cập nhật state phục vụ các request sau. */
export type TopologyRouteResult = {
    status: 'success' | 'failure';
    latency?: number;
};

/**
 * Transporter RPC chỉ chịu trách nhiệm truyền packet.
 * `name` là wire identifier ổn định, không được suy luận từ tên class.
 */
export type RpcTransporter = Observable<RpcEvent> & {
    readonly name: string;
    start?(context: RpcTransporterContext): void | Promise<void>;
    stop?(): void | Promise<void>;
    /**
     * `destination_node_id` trả về là node mà transporter đã thực sự route tới (khi nó tự
     * resolve bằng Topology). Core dùng nó để đóng stream khi node đó offline.
     */
    send(data: RpcRequestPacket | RpcResponsePacket | RpcCancelPacket): Promise<{ cancel: () => void, destination_node_id?: string }>;
    probe?(request: RpcProbeRequest): Promise<RpcProbeResult>;
    /**
     * Kiểm tra nhanh transporter có route sẵn hay không mà không gây side effect.
     * Đây không phải network probe và không được làm thay đổi routing state.
     */
    canRoute?(service: string, node_id?: string): boolean;
};
