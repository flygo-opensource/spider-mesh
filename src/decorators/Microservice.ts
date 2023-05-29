
const MicroserviceList = [] as any[]

export const Microservice: () => ClassDecorator = () => {
    return c => {
        MicroserviceList.push(c)
        return c
    }
}


export const listMicroserviceFactories = () => [...MicroserviceList]