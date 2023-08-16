import { randomUUID } from "crypto"

export const DEFAULT_NAMEPSACE = process.env.SPIDER_MESH_NAMESPACE || 'default'
export const NODE_ID = process.env.SPIDER_MESH_NODE_ID || randomUUID()
export const DEBUG = process.env.SPIDERMESH_DEBUG
export const UDP_PORT = Number(process.env.UDP_PORT || 10000)
export const SEEDING_IP_RANGES = process.env.SEEDING_IP_RANGE
export const SEEDING_IPS = process.env.SEEDING_IP