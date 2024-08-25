import { randomUUID } from "./helpers/randomUUID.js"

 
export const NAMEPSACE = process.env.SPIDERMESH_NAMESPACE || 'default'
export const NODE_ID = process.env.SPIDERMESH_NODE_ID || randomUUID()
export const UDP_BROADCAST_ADDRESS = process.env.SPIDERMESH_UDP_BROADCAST_ADDRESS || ''
export const UDP_BROADCAST_PORT = Number(process.env.SPIDERMESH_UDP_BROADCAST_PORT || 10000)
export const UDP_SECRET_KEY = process.env.SPIDERMESH_UDP_BROADCAST_PORT || 'default-spider-mesh-secret-key'
export const BROADCAST_INTERVAL =Number( process.env.SPIDERMESH_BROADCAST_INTERVAL || 0)