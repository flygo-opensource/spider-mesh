
export const ServiceInstanceList = new Array<{ name: string, instance: any }>()

export const SpiderMeshMicroservice = () => (factory: { new(...args: any[]): any }) => {

    const C = class extends factory {
        constructor(...args: any[]) {
            super(...args)
            ServiceInstanceList.push({
                name: factory.name,
                instance: this
            })
        }
    }
    Object.defineProperty(C, 'name', factory.name)
    return C
}

