export const SPIDERMESH_HTTP2_RECONNECT_ATTEMPTS = Number(process.env.SPIDERMESH_HTTP2_RECONNECT_ATTEMPTS || 3)
export const SPIDERMESH_HTTP2_RECONNECT_DELAY_MS = Number(process.env.SPIDERMESH_HTTP2_RECONNECT_DELAY_MS || 250)
export const SPIDERMESH_HTTP2_CONNECT_TIMEOUT_MS = Number(process.env.SPIDERMESH_HTTP2_CONNECT_TIMEOUT_MS || 2000)
/** Thời gian chờ tối đa giữa hai lần thử nối lại; vòng thử không bao giờ dừng khi node còn trong Topology. */
export const SPIDERMESH_HTTP2_RECONNECT_MAX_DELAY_MS = Number(process.env.SPIDERMESH_HTTP2_RECONNECT_MAX_DELAY_MS || 30_000)
