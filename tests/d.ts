import { Encoder } from "../src/Encoder.js"

console.log(process)


const data = 0
const encoded = Encoder.encode(data)
const decoded = Encoder.decode(encoded)

console.log({encoded, decoded})