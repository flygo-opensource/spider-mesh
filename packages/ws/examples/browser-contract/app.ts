import { SpiderMesh } from '@spider-mesh/core'
import { WebsocketTransporter } from '../../src/browser.js'
import { runContract } from '../contract/runContract.js'

const output = document.getElementById('result')!
const relayPort = new URLSearchParams(location.search).get('relay')

try {
    const transporter = new WebsocketTransporter()
    transporter.connect(`ws://127.0.0.1:${relayPort}`)
    const results = await runContract(new SpiderMesh({ transporters: [transporter] }), 'websocket')
    const failures = results.filter(result => !result.pass)
    output.textContent = JSON.stringify({ total: results.length, passed: results.length - failures.length, failures }, null, 2)
    document.title = failures.length === 0 ? 'PASS' : 'FAIL'
} catch (error) {
    output.textContent = `ERROR: ${(error as Error)?.stack ?? error}`
    document.title = 'ERROR'
}
