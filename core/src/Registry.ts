/**
 * Tên tương thích cho code cũ.
 * Runtime chỉ có một class thật là `Topology`; `Registry` chỉ là alias export.
 *
 * @deprecated Dùng `Topology`.
 */
export { Topology as Registry } from './Topology.js'
export type {
    TopologyNodePatch as RegistryPeerPatch,
    TopologyPickRpcTargetOptions as RegistryPickRpcTargetOptions,
} from './Topology.js'
