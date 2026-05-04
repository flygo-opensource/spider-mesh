import { WebsocketRelayServer } from '../src/relay-server.js'
const host = process.env.WS_HOST || '127.0.0.1'
const port = Number(process.env.WS_PORT || 8787)
const server = new WebsocketRelayServer({ port, host })

console.log(`WebSocket relay server listening on ws://${host}:${server.port}`)