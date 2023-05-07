


export class DeepProxy {

    #options: { [key: string]: any } = {}

    constructor(
        private options_list: string[],
        private handler: (method: string, options) => any
    ) { }


    nest() {
        return new Proxy(this, {
            get: (_, method: string) => {

                if (method == 'then') return null

                if (method.startsWith('$set_')) {
                    const m = method.split('$set_')[1]
                    if (this.options_list.includes(m)) {
                        return value => {
                            this.#options[m] = value
                            return this.nest()
                        }
                    }
                }
                return this.handler(method, this.#options)
            }
        })
    }
}
