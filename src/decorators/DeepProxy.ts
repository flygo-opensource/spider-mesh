


export class DeepProxy {

    #options: { [key: string]: any } = {}

    constructor(
        private option_parser: (method: string) => boolean,
        private handler: (method: string, options) => any
    ) { }


    nest() {
        return new Proxy(this, {
            get: (_, method: string) => {

                if (method == 'then') return null

                if (this.option_parser(method)) return (value = true) => {
                    this.#options[method] = value
                    return this.nest()
                }
                return this.handler(method, this.#options)
            }
        })
    }
}
