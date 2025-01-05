import "reflect-metadata";


export const key = Symbol.for('BeforeMicroserviceOnline');

export const BeforeMicroserviceOnline = () => <T>(
    target: Object,
    method: string | symbol 
) => {
    // Reflect.defineMetadata(key, [
    //     ...Reflect.getMetadata(key, target) || [],
    //     method
    // ], target)
};
export const listBeforeMicroserviceOnlineMethods = (target: any) => []//Reflect.getMetadata(key, target) as string[] || []