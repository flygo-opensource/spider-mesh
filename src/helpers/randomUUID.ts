import crypto from 'crypto'

let randomUUID: () => string

if (typeof process !== 'undefined' && process.versions != null && process.versions.node != null) {
    // Node.js environment
    randomUUID = crypto.randomUUID
} else if (typeof navigator !== 'undefined' && navigator.product === 'ReactNative') {
    // React Native environment
    randomUUID = require('react-native-uuid').v4
} else {
    // Browser environment
    randomUUID = window.crypto.randomUUID
}

export { randomUUID }