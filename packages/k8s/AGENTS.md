# @spider-mesh/k8s — Agent / Contributor Guide

How to build, test, and work inside this package. This file is the operating
guide for anyone (human or agent) **editing** `@spider-mesh/k8s`.

- **Using the package?** → [README.md](README.md)
- **Understanding the internals?** → [ARCHITECTURE.md](ARCHITECTURE.md)

## Build & Test

Build `core` and `tcp` first: the e2e test imports their build output.

```bash
bun run build        # tsc -b
bun run test:e2e     # bun test tests/  (unit + cross-process e2e)
```

Narrower commands:

```bash
bun test tests/kubernetes-discovery.test.ts   # fake API server + fake pods, no tcp needed
bun test tests/mesh.e2e.test.ts               # two real SpiderMesh processes with Http2Rpc
```

## Trying it on a real cluster

`tests/cluster/` holds a demo (`demo.ts`: 3 providers, 1 client calling every second) and its
`manifest.yaml`. The manifest mounts the code from `/opt/spider-mesh` on the node with `hostPath`, so
it only fits a single-node test cluster:

```bash
# from packages/: -L dereferences the absolute symlinks Bun creates for file: dependencies
COPYFILE_DISABLE=1 tar --no-xattrs -czLf /tmp/spider-mesh-test.tgz \
  tcp/package.json tcp/build tcp/node_modules k8s/package.json k8s/tsconfig.json k8s/src k8s/tests k8s/node_modules
# on the node
sudo mkdir -p /opt/spider-mesh && sudo tar xzf /tmp/spider-mesh-test.tgz -C /opt/spider-mesh
kubectl apply -f k8s/tests/cluster/manifest.yaml
kubectl -n spider-mesh-test logs -f deploy/client
```

Scenarios worth repeating after a change: delete a provider pod, scale 3 → 1 → 3, `rollout restart`,
and delete the RoleBinding then restart the client (must warn once and switch to `mode: "dns"`).

## Conventions

- **ESM-only**, `.js` relative specifiers in TypeScript source.
- **Node/Bun only.** Use `node:http`/`node:https`, not `fetch`: Node's `fetch` cannot take a custom CA.
- **Import only types from `@spider-mesh/core`.** Nothing at runtime, so the package works with whichever
  core instance the app loads.
- **Never remove a node on a transient error.** Only a target leaving membership removes a node; errors
  emit `null` (keep members, `synced = false`).
- **Warnings go through `onWarning`**, prefixed `[spider-mesh/k8s]` by the default handler, and must say
  what to do about them.
- **`@spider-mesh/tcp` is not a devDependency.** Bun resolves the `file:` dependencies inside it
  relative to the wrong directory, so tests import `../../tcp/build` and core from
  `../../tcp/node_modules` (a single core instance).
- `examples/` and `tests/` are excluded from the package build.

## Where things live

| Concern | File |
| --- | --- |
| Discovery, `/node` server, mode selection | `src/KubernetesDiscovery.ts` |
| EndpointSlice watch, DNS polling, in-cluster config | `src/membership.ts` |
| Pulling one pod's `/node` | `src/NodeStream.ts` |
| Env config constants | `src/const.ts` |
| Fake API server and fake pods | `tests/helpers/fakes.ts` |
| Real-cluster demo | `tests/cluster/` |
