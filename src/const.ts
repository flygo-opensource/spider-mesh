import { randomUUID } from "crypto"

export const DEFAULT_NAMEPSACE = process.env.SPIDER_MESH_NAMESPACE = 'default'
export const NODE_ID = process.env.SPIDER_MESH_NODE_ID || randomUUID()
export const DEBUG =  process.env.SPIDERMESH_DEBUG