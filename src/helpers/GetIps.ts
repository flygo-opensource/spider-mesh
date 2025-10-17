import { networkInterfaces } from "node:os"



export const PublicIP = await fetch('https://api.ipify.org?format=text').then(res => res.text()).catch(() => null)

export const AllIpAddresses = [
    ...PublicIP ? [PublicIP] : [],
    ...(
        Object
            .values(networkInterfaces())
            .flat(2)
            .filter(a => !a?.internal && !!a?.address)
            .map(a => a?.address!)
    )
]