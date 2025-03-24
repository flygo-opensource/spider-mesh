import { pack } from 'msgpack'
import { withLatestFrom, tap, delayWhen, share } from 'rxjs/operators'
import { interval, timer } from 'rxjs'
import { buffer } from 'stream/consumers'
import { firstValueFrom } from 'rxjs/internal/firstValueFrom'


const a$ = interval(1000)

const b$ = firstValueFrom(timer(3000))

 


a$.pipe( 
    delayWhen(() => b$),
    withLatestFrom(b$),
    tap(a => {
        console.log(a)
    })
).subscribe()