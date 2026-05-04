import crypto from 'crypto';
import rnuuid from 'react-native-uuid';

type ReactNativeUuid = typeof import('react-native-uuid').default

export const randomUUID = () => {
    if (typeof process !== 'undefined' && process.versions != null && process.versions.node != null) {
        return crypto.randomUUID()
    }

    if (typeof navigator !== 'undefined' && navigator.product === 'ReactNative') {
        const v4 = (rnuuid as unknown as ReactNativeUuid).v4
        return v4()
       
    }


    return window.crypto.randomUUID()
}