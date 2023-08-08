export type RPCOptions<T = any> = {
    $node_id: string
    $ip: string
    $load_balance_mode: 'round-robin' | 'least-connections' | 'weight'
    $offline_cut: boolean
    $timeout: number
    $nevermind: boolean
    $fallback: T
    $retry: number
    $retry_delay: number
    $require_ack: boolean
    $ack_timeout: number
    $queue: boolean
}

export const RPCOptionsList = new Set([
    '$node_id',
    '$ip',
    '$load_balance_mode',
    '$offline_cut',
    '$timeout',
    '$nevermind',
    '$fallback',
    '$retry',
    '$retry_delay',
    '$require_ack',
    '$ack_timeout',
    '$safe_mode'
])