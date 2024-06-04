import { Observable } from "rxjs"

export type PureData = string | number | boolean | null | Buffer | PureData[] | {
    [key: string]: PureData
}

 


export type Encodeable = PureData | Array<PureData | Encodeable> | {
    [key: string]: PureData | Encodeable
} | Observable<PureData>

export const Encoder = {

    encode: <T extends Encodeable>(content: T) => {

        const buffers: Buffer[] = []
        

        if (content == null || content == undefined) {
            const buffer = Buffer.alloc(4)
            buffer.writeInt32LE(-1)
            return buffer
        }

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
 

            if (originalObject instanceof Buffer) {
                buffers.push(originalObject)
                return {
                    __$$__dataType: 'Buffer',
                    __index: buffers.length - 1
                }
            }

            return value
        }
        const metadata = JSON.stringify(content, replacer)

        const buffers_count = Buffer.alloc(4)
        buffers_count.writeUInt32LE(buffers.length, 0)

        const buffers_list = buffers.map(b => {
            const length = Buffer.alloc(4)
            length.writeUInt32LE(b.length)
            return [length, b]
        }).flat(2)
        const data = Buffer.from(metadata, 'utf-8')
        const buffer = Buffer.concat([buffers_count, ...buffers_list, data])
        return buffer
    },

    decode: <T>(
        raw: Buffer
    ) => {
        const buffers: Buffer[] = []
        const length = raw.readUInt32LE(0)
        if (length > raw.length || length < 0) return null
        let index = 4
        for (let i = 0; i < length; i++) {
            const blength = raw.readUint32LE(index)
            index += 4
            const buff = raw.slice(index, index + blength)
            buffers.push(buff)
            index += blength
        }
        const metadata = raw.toString('utf-8', index)
        function receiver(this: any, key: string, value: any) {
            if (typeof value === 'object' && value !== null) {
                if (value.__$$__dataType === 'Map') {
                    return new Map(value.__$$__value);
                }

                if (value.__$$__dataType === 'Set') {
                    return new Set(value.__$$__value);
                }
 

                if (value.__$$__dataType == 'Buffer') {
                    return buffers[value.__index]
                } 
            }

            return value;
        }
        return JSON.parse(metadata, receiver) as T
    }
} 