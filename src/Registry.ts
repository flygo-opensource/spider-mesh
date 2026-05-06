import { BehaviorSubject, map } from 'rxjs'
import type { SpiderMeshNode } from './types.js'

export type RegistryPickRpcTargetOptions = {
    node_id?: string
}

export class Registry {

    public readonly nodes$ = new BehaviorSubject<Map<string, SpiderMeshNode>>(new Map())

    #rrIndexes = new Map<string, number>()

    getPeer(nodeId: string) {
        return this.nodes$.value.get(nodeId)
    }

    upsertPeer(node: SpiderMeshNode) {
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

    listPeers(options: { service?: string } = {}) {
        return [...this.nodes$.value.values()].filter(node => {
            if (options.service && node.services[options.service] == undefined) return false
            return true
        })
    }

    watch(service?: string) {
        return this.nodes$.pipe(
            map(() => this.listPeers({ service }))
        )
    }

    pickRpcNode(service: string, options: RegistryPickRpcTargetOptions = {}) {
        if (options.node_id) {
            const node = this.getPeer(options.node_id)
            if (!node) return null
            if (node.services[service] == undefined) return null
            return node.node_id
        }

        const targets = this.listPeers({ service })
        if (targets.length === 0) return null

        const index = this.#rrIndexes.get(service) || 0
        this.#rrIndexes.set(service, (index + 1) % targets.length)
        return targets[index % targets.length]?.node_id || null
    }

    getRpcTransporterName(service: string, options: RegistryPickRpcTargetOptions = {}) {
        const nodeId = options.node_id || this.pickRpcNode(service, options)
        if (!nodeId) return null
        const node = this.getPeer(nodeId)
        const transporter = node?.transporters?.rpc
        return typeof transporter === 'string' ? transporter : null
    }

    listTopicNodes(topic: string) {
        return [...this.nodes$.value.values()].filter(node => node.topics.includes(topic))
    }

}