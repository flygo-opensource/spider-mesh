export type RPCOptions<T = any> = {
    $node_id: string
    $ip: string
    $load_balance_mode: 'round-robin' | 'least-connections' | 'weight'
    $timeout: number
    $nevermind: boolean
    $fallback: T
    $retry: number
    $retry_delay: number
}

export const RPCOptionsList = new Set([
    '$node_id',
    '$ip',
    '$load_balance_mode',
    '$timeout',
    '$nevermind',
    '$fallback',
    '$retry',
    '$retry_delay',
    '$safe_mode'
])