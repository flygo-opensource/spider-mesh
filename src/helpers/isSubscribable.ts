import type { Observable } from 'rxjs'

export const isSubscribable = (value: unknown): value is Observable<unknown> => {
    return !!value
        && typeof value === 'object'
        && typeof (value as { subscribe?: unknown }).subscribe === 'function'
}
