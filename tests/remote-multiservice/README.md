# Remote multi-service discovery test

This is a real cross-host Bun test for the case where Service A depends on both Service B and
Service C, while B and C each have providers on two other servers.

## Topology

```text
192.168.1.10                    192.168.1.20                  192.168.1.30
Service A                      b-sv2: ServiceB              b-truenas: ServiceB
(observer + RPC client)        c-sv2: ServiceC              c-truenas: ServiceC
       │                              ▲   ▲                         ▲   ▲
       └── Ohayo UDP discovery ───────┴───┴─────────────────────────┴───┘
       └── direct HTTP/2 RPC ───────────────────────────────────────────►
```

- Service A runs on a third machine.
- Each remote host runs two independent processes on the same UDP discovery port:
  one `ServiceB` provider and one `ServiceC` provider.
- A must discover two providers for B and two for C from one-shot announcements, retain all four
  through maintained HTTP/2 sessions, call each node directly, and reach both through round-robin.

This covers the concern that multiple UDP sockets sharing a host/port might cause only one B/C
process to receive an announcement. Ohayo's own new-peer one-shot reply and redundant copies must
converge the full Topology; TCP then owns endpoint reachability.

The fixed `SPIDERMESH_NODE_ID` values in this harness are deterministic test labels. These scripts
construct raw node snapshots directly and do not change the production rule that `SpiderMesh`
always generates a random node ID.

## Prepare each host

Place these package archives next to `package.json`:

- `spider-mesh-core-3.0.0.tgz`
- `spider-mesh-tcp-3.0.0.tgz`
- `ohayo-udp-3.0.0.tgz`

Then install using Bun only:

```bash
mkdir -p /tmp/spider-mesh-ohayo-3host/remote-multiservice
cd /tmp/spider-mesh-ohayo-3host/remote-multiservice
bun install
```

Use one namespace, key, UDP port, and explicit peer list on every host. Explicit peers make the
test independent of whether multicast is allowed:

```bash
export SPIDERMESH_NAMESPACE=ohayo-3host
export OHAYO_DISCOVERY_KEY=ohayo-3host-key
export OHAYO_DISCOVERY_PORT=28446
export OHAYO_UDP_WHITELIST_ADDRESS=192.168.1.10,192.168.1.20,192.168.1.30
export SPIDERMESH_HTTP2_RECONNECT_ATTEMPTS=3
export SPIDERMESH_HTTP2_RECONNECT_DELAY_MS=100
export SPIDERMESH_HTTP2_CONNECT_TIMEOUT_MS=1000
```

## Start providers

On `192.168.1.20`, run two independent processes:

```bash
SPIDERMESH_NODE_HOSTNAME=192.168.1.20 SPIDERMESH_NODE_ID=b-sv2 \
SERVICE_NAME=ServiceB PROVIDER_LABEL=sv2-b bun run provider.mjs

SPIDERMESH_NODE_HOSTNAME=192.168.1.20 SPIDERMESH_NODE_ID=c-sv2 \
SERVICE_NAME=ServiceC PROVIDER_LABEL=sv2-c bun run provider.mjs
```

On `192.168.1.30`:

```bash
SPIDERMESH_NODE_HOSTNAME=192.168.1.30 SPIDERMESH_NODE_ID=b-truenas \
SERVICE_NAME=ServiceB PROVIDER_LABEL=truenas-b bun run provider.mjs

SPIDERMESH_NODE_HOSTNAME=192.168.1.30 SPIDERMESH_NODE_ID=c-truenas \
SERVICE_NAME=ServiceC PROVIDER_LABEL=truenas-c bun run provider.mjs
```

## Run Service A

On `192.168.1.10`:

```bash
SPIDERMESH_NODE_HOSTNAME=192.168.1.10 \
SPIDERMESH_NODE_ID=service-a-local \
EXPECTED_SERVICE_B=b-sv2,b-truenas \
EXPECTED_SERVICE_C=c-sv2,c-truenas \
bun run service-a.mjs
```

The pass event must contain all four direct RPC nodes and both providers in each round-robin set:

```json
{
  "event": "multiservice-test-passed",
  "stable_seconds": 12,
  "ServiceB": ["b-sv2", "b-truenas"],
  "ServiceC": ["c-sv2", "c-truenas"],
  "direct_rpc_nodes": ["b-sv2", "b-truenas", "c-sv2", "c-truenas"]
}
```

## Last verified

Verified on 2026-07-14 with Bun on all three hosts. Service A discovered all four providers using
Ohayo's own one-shot reply (no manual reply in the harness),
retained them for 12 seconds, called every node directly, and round-robin reached both copies of B
and both copies of C.
