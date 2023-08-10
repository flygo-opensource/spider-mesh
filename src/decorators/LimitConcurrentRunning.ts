import { Subject, mergeMap } from "rxjs"


export function LimitConcurrentRunning<T>(limit: number) {
    return (target: any, method: string, descriptor: TypedPropertyDescriptor<T>) => {

        const bus = new Subject<{
            $: any,
            args: any[],
            s: Function
            r: Function
        }>()

        const fn = descriptor.value as Function

        bus.pipe(
            mergeMap(async ({ $, args, r, s }) => {
                try {
                    s(await fn.call($, ...args))
                } catch (e) {
                    r(e)
                }
            }, limit)
        ).subscribe()

        return {
            value: async function (this: any, ...args: any[]) {
                return new Promise((s, r) => {
                    bus.next({ $: this, args, s, r })
                })
            } as T
        }
    }
} 