/**
 * Getting started (ws) — Step 1: the relay.
 *
 * Run this FIRST, in its own terminal. The relay routes frames between nodes and
 * forwards discovery; it hosts no application services. Every node connects to it.
 *
 *   bun run examples/getting-started/relay.ts
 */
import { WebsocketRelayServer } from '../../src/relay-server.js'

const port = Number(process.env.WS_PORT || 8787)
const relay = new WebsocketRelayServer({ host: '127.0.0.1', port })

console.log(`relay listening on ws://127.0.0.1:${relay.port}`)

// keep the process alive
setInterval(() => undefined, 1000)
