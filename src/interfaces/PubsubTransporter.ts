import { Observable } from "rxjs"

export type PubsubTransporter = {
    name: `pubsub-${string}` 
    publish: <T>(topic: string, data: T) => Promise<void>
    listen: <T>(topic: string) => Observable<T>
}