export type RPCOptions<T = any> = {
    $node_id: string
    $timeout: number
    $forgot: boolean
    $fallback: T
    $retry: number
    $retry_delay: number,
    $safe_mode: boolean,
    $ip: string
}

export const RPCOptionsList = new Set([
    '$node_id',
    '$timeout',
    '$forgot',
    '$fallback',
    '$retry',
    '$retry_delay',
    '$safe_mode',
    '$ip'
])