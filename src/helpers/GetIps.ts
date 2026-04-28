const isServerRuntime = typeof process !== 'undefined' && !!(process.versions?.node || process.versions?.bun)

export const PublicIP = isServerRuntime
    ? await fetch('https://api.ipify.org?format=text').then(res => res.text()).catch(() => null)
    : null

const localIpAddresses = isServerRuntime
    ? await import('node:os').then(({ networkInterfaces }) => {
        return Object
            .values(networkInterfaces())
            .flat(2)
            .filter(address => !address?.internal && !!address?.address)
            .map(address => address?.address!)
    }).catch(() => [] as string[])
    : []

export const AllIpAddresses = [
    ...(PublicIP ? [PublicIP] : []),
    ...localIpAddresses,
]