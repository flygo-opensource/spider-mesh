import crypto from 'crypto';

export const randomUUID = () => {
    if (typeof process !== 'undefined' && process.versions != null && process.versions.node != null) {
        return crypto.randomUUID()
    }

    if (typeof navigator !== 'undefined' && navigator.product === 'ReactNative') {
        const rnuuid = require('react-native-uuid').default
        const v4 = rnuuid.v4
        return v4()
       
    }


    return window.crypto.randomUUID()
}