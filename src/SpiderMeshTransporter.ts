

export type SpiderMeshTransporter = {

    node_id: string
    namespace: string

    start: () => void
    on_node_offline: (cb: (node_id: string) => any) => void
    on_node_online: (cb: (node_id: string) => any) => void
    listen: <T = any>(topic: string, cb: (node_id: string, data: T) => any) => {
        unsubscribe: Function
    }

    publish<T = any>(
        event: string,
        node_id: string | null,
        data: T,
        queue?: boolean
    ): Promise<void>
}


export type SpiderMeshTransporterFactory = { new(...args: any[]): SpiderMeshTransporter }