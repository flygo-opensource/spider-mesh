// Entry chạy trong môi trường giống trình duyệt (không có `process`): import core, tạo SpiderMesh.
import { SpiderMesh } from '../../src/index.js'

const mesh = new SpiderMesh()
;(globalThis as { __browserMesh?: unknown }).__browserMesh = {
    node_id: mesh.node_id,
    namespace: mesh.localNode.namespace,
}
