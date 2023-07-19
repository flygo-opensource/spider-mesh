import { firstValueFrom, merge, fromEvent, map } from "rxjs";
import { EventEmitter } from "stream";

export function waitFirstEvent(target: EventEmitter, ...events: string[]) {
    return firstValueFrom(merge(
        ...events.map(e => fromEvent(target, e).pipe(map((...args) => [e,...args])))
    ))
}