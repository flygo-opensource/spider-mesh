export type Encodable = string | number | boolean | undefined | null | Buffer | Encodable[] | {
    [key: string]: Encodable | undefined
}

if (typeof Buffer == 'undefined') {
    Buffer = require('buffer').Buffer
}


export const Encoder = {
    encode: <T extends Encodable>(e: T) => {
        if (e == undefined) {
            const buffer = Buffer.alloc(4)
            buffer.writeInt32LE(-1)
            return buffer
        }
        const content = e instanceof Error ? {
            code: (e as any).code || e.message || 'UNKNOWN',
            stack: e.stack,
            name: e.name
        } : e

        function replacer(this: any, key: string, value: any) {
            const originalObject = this[key]
            if (originalObject instanceof Map) return {
                __$$__dataType: 'Map',
                __$$__value: Array.from(originalObject.entries()), // or with spread: __$$__value: [...originalObject]
            }

            if (originalObject instanceof Set) return {
                __$$__dataType: 'Set',
                __$$__value: Array.from(originalObject.values()), // or with spread: __$$__value: [...originalObject]
            }

            return value
        }
        return Buffer.from(JSON.stringify(content, replacer))
    },

    decode: <T>(
        raw: Buffer
    ) => {
        if (raw.length == 4 && raw.readInt32LE() == -1) return undefined
        function receiver(this: any, key: string, value: any) {
            if (typeof value === 'object' && value !== null) {
                if (value.__$$__dataType === 'Map') {
                    return new Map(value.__$$__value);
                }

                if (value.__$$__dataType === 'Set') {
                    return new Set(value.__$$__value);
                }
            }

            return value;
        }
        return JSON.parse(raw.toString('utf8'), receiver) as T
    }
} 