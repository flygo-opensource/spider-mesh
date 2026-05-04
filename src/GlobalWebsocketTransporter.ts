import { BaseWebsocketTransporter, type WebSocketLike, type WebsocketTransporterOptions } from './BaseWebsocketTransporter.js'

export { type WebsocketConnectionStatus, type WebsocketTransporterOptions } from './BaseWebsocketTransporter.js'

export class WebsocketTransporter extends BaseWebsocketTransporter {
    constructor(options: WebsocketTransporterOptions = {}) {
        super(options)
    }

    protected createSocket(url: string): WebSocketLike {
        if (typeof globalThis.WebSocket !== 'function') {
            throw new Error('globalThis.WebSocket is not available in this runtime')
        }

        const socket = new globalThis.WebSocket(url) as unknown as WebSocketLike

        if ('binaryType' in socket) {
            socket.binaryType = 'arraybuffer'
        }

        return socket
    }
}