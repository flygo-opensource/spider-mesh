import { expect, test } from 'bun:test'
import { RemoteServiceLinker, SpiderMesh, Topology, type SpiderMeshNode } from '@spider-mesh/core'
import { Http2Rpc } from '../src/index.js'
import { runBunScript } from './helpers/runBunScript.js'

test('nodes without SPIDERMESH_NODE_HOSTNAME are reached at their UDP sender address', async () => {
    const result = await runBunScript(['run', 'examples/missing-hostname-test.ts'], 30000)

    expect(result.code).toBe(0)
    expect(result.stdout).toContain('hello world')
})

test('a node with an empty host fails the call with MICROSERVICE_OFFLINE instead of ERR_INVALID_URL', async () => {
    // Discovery không có địa chỉ người gửi: node tới Topology với host rỗng.
    const topology = new Topology()
    const transporter = new Http2Rpc()
    const mesh = new SpiderMesh({ topology, transporters: [transporter] })
    const ghost: SpiderMeshNode = {
        node_id: 'ghost-no-host',
        namespace: mesh.namespace,
        host: '',
        version: 1,
        topics: [],
        services: { GhostService: {} },
        nodes: {},
        transporters: { http2: { port: 1 } },
    }
    topology.upsertRemote(ghost)

    const service = RemoteServiceLinker.link<{ ping(): Promise<string> }>(mesh, { service: 'GhostService' })
    const error = await Promise.resolve(service.ping()).catch(error => error)

    expect(error?.code).toBe('MICROSERVICE_OFFLINE')
    expect(error?.message).toContain('SPIDERMESH_NODE_HOSTNAME')
    transporter.stop()
    await topology.close?.()
})
