import WebSocket from 'ws'
import { BaseWebsocketTransporter, type WebSocketLike, type WebsocketTransporterOptions } from './BaseWebsocketTransporter.js'

export { type WebsocketConnectionStatus, type WebsocketTransporterOptions } from './BaseWebsocketTransporter.js'

/** WebSocket transporter dành cho Node.js, sử dụng package `ws`. */
export class WebsocketTransporter extends BaseWebsocketTransporter {
    constructor(options: WebsocketTransporterOptions = {}) {
        super(options)
    }

    protected createSocket(url: string): WebSocketLike {
        const socket = new WebSocket(url) as unknown as WebSocketLike

        if ('binaryType' in socket) {
            socket.binaryType = 'arraybuffer'
        }

        return socket
    }
}
