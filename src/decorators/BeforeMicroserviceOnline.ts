import "reflect-metadata";
import { SpiderMesh } from "../SpiderMesh.js";
export const key = Symbol.for('BeforeMicroserviceOnline');
export const BeforeMicroserviceOnline = () => <T>(
    target: Object,
    method: string | symbol,
    descriptor: TypedPropertyDescriptor<T | ((sm?: SpiderMesh) => Promise<void>)>
) => {
    Reflect.defineMetadata(key, [
        ...Reflect.getMetadata(key, target) || [],
        method
    ], target)
};
export const listBeforeMicroserviceOnlineMethods = (target: any) => Reflect.getMetadata(key, target) as string[]
//# sourceMappingURL=BeforeMicroserviceOnline.js.map