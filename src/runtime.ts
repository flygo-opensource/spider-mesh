import type { RpcEvent, SpiderMeshNode } from "./types.js"

function unique(values: string[] = []) {
    return [...new Set(values.filter(Boolean))]
}

function mergeNode(current: SpiderMeshNode | undefined, next: SpiderMeshNode) {
    return {
        ...current,
        ...next,
        ips: unique([...(current?.ips || []), ...(next.ips || [])]),
        topics: unique([...(current?.topics || []), ...(next.topics || [])]),
        services: {
            ...(current?.services || {}),
            ...(next.services || {})
        },
        nodes: {
            ...(current?.nodes || {}),
            ...(next.nodes || {})
        },
        transporters: {
            ...(current?.transporters || {}),
            ...(next.transporters || {})
        }
    } satisfies SpiderMeshNode
}

class TransportRuntime {
    #localNode: SpiderMeshNode | null = null
    #nodes = new Map<string, SpiderMeshNode>()
    #transporters = new Map<string, RpcEvent['metadata']>()

    get localNode() {
        return this.#localNode
    }

    get nodes() {
        return this.#nodes
    }

    setTransporterMetadata(name: string, metadata: RpcEvent['metadata']) {
        if (!metadata) return
        this.#transporters.set(name, metadata)
        if (this.#localNode) {
            this.updateLocalNode(this.#localNode)
        }
    }

    updateLocalNode(node: SpiderMeshNode) {
        const merged = this.withTransporters(mergeNode(this.#localNode || undefined, node))
        this.#localNode = merged
        this.#nodes.set(merged.node_id, merged)
        return merged
    }

    updateNode(node: SpiderMeshNode) {
        const merged = mergeNode(this.#nodes.get(node.node_id), node)
        this.#nodes.set(merged.node_id, merged)
        if (this.#localNode?.node_id === merged.node_id) {
            this.#localNode = this.withTransporters(merged)
            this.#nodes.set(merged.node_id, this.#localNode)
            return this.#localNode
        }
        return merged
    }

    getNode(nodeId: string) {
        return this.#nodes.get(nodeId)
    }

    withTransporters(node: SpiderMeshNode) {
        return {
            ...node,
            transporters: {
                ...node.transporters,
                ...Object.fromEntries(this.#transporters)
            }
        } satisfies SpiderMeshNode
    }
}

export const transportRuntime = new TransportRuntime()