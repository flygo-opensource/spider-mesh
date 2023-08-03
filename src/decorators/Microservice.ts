import { ReplaySubject } from "rxjs"

export const serviceInstanceList = new ReplaySubject<{ instance: any, namespaces: string[] }>()


export const Microservice = (namespaces: string[] = ['default']) => {
    return (
        (target: { new(...args: any[]): {} }) => {
            class C extends target {
                constructor(...args: any[]) {
                    super(...args)
                    serviceInstanceList.next({ instance: this, namespaces })
                }
            }
            Object.defineProperty(C, 'name', { value: target.name })
            return C as any
        }


    ) as any
}