


export class MicroserviceException<T = undefined> extends Error {
    constructor(
        public readonly code: string,
        public readonly metadata?: T
    ) {
        super(code) 
    }
}
