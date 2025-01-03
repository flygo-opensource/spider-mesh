import { transformSync } from 'bun';

export async function resolve(specifier, context, defaultResolve) {
    return defaultResolve(specifier, context);
}

export async function load(url, context, defaultLoad) {
    if (url.endsWith('.ts') || url.endsWith('.tsx')) {
        const source = await Bun.file(url).text();
        const transformed = transformSync(source, { loader: 'ts' });
        return {
            format: 'module',
            source: transformed.code,
        };
    }

    return defaultLoad(url, context);
}
