import { createMesh } from '../helpers/createMesh.js'
import { ContractService } from './ContractService.js'

createMesh()
new ContractService()
console.log('contract provider ready')
setInterval(() => undefined, 1000)
