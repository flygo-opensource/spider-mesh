


export class MicroserviceException<T extends { code: string } = { code: string, [key: string]: any }> extends Error {
    constructor(metadata: T) {
        super(metadata.code || 'UNKNOWN_ERROR')
        for (const key in metadata) {
            (this as any)[key] = metadata[key]
        }
    }
}
