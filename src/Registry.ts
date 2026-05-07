import { BehaviorSubject, map } from 'rxjs'
import type { SpiderMeshNode } from './types.js'

export type RegistryPickRpcTargetOptions = {
    node_id?: string
}

export class Registry {

    public readonly nodes$ = new BehaviorSubject<Map<string, SpiderMeshNode>>(new Map())
    #transporters = new Map<string, string>()
    #rrIndexes = new Map<string, number>()

    getPeer(nodeId: string) {
        return this.nodes$.value.get(nodeId)
    }

    upsertPeer(node: SpiderMeshNode) {
        node.transporters.rpc && Object.keys(node.services).forEach(service => {
            this.#transporters.set(service, node.transporters.rpc)
        })
        const nodes = new Map(this.nodes$.value)
        const current = nodes.get(node.node_id)
        nodes.set(node.node_id, {
            ...current,
            ...node,
            topics: node.topics || current?.topics || [],
            services: {
                ...(current?.services || {}),
                ...(node.services || {})
            },
            nodes: {
                ...(current?.nodes || {}),
                ...(node.nodes || {})
            },
            transporters: {
                ...(current?.transporters || {}),
                ...(node.transporters || {})
            }
        })
        this.nodes$.next(nodes)
        return nodes.get(node.node_id)!
    }

    removePeer(nodeId: string) {
        const nodes = new Map(this.nodes$.value)
        const deleted = nodes.delete(nodeId)
        if (deleted) {
            this.nodes$.next(nodes)
        }
        return deleted
    }

    listPeers(service?: string) {
        return [...this.nodes$.value.values()].filter(node => {
            if (service && node.services[service] == undefined) return false
            return true
        })
    }

    watch(service?: string) {
        return this.nodes$.pipe(
            map(() => this.listPeers(service))
        )
    }

    pickRpcNode(service: string, options: RegistryPickRpcTargetOptions = {}) {
        if (options.node_id) {
            const node = this.getPeer(options.node_id)
            if (!node) return null
            if (node.services[service] == undefined) return null
            return node.node_id
        }

        const targets = this.listPeers(service)
        if (targets.length === 0) return null

        const index = ((this.#rrIndexes.get(service) || 0) + 1) % targets.length
        this.#rrIndexes.set(service, index)
        return targets[index]?.node_id
    }

    getRpcTransporterName(service: string) {
        return this.#transporters.get(service)
    }

    listTopicNodes(topic: string) {
        return [...this.nodes$.value.values()].filter(node => node.topics.includes(topic))
    }

}