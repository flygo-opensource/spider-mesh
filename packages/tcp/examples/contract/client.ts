import { createMesh } from '../helpers/createMesh.js'
import { runContract } from './runContract.js'

const { mesh } = createMesh()
const results = await runContract(mesh, 'http2')
console.log(JSON.stringify({ contract: results }))
process.exit(0)
