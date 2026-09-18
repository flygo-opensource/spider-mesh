# Bug: UDP Discovery Registry Drops All Nodes Over Time

> Historical investigation for TCP 2.0.156. Current code uses generic `@ohayo/udp`,
> optional Core Topology, generic Discovery ingestion và HTTP/2 reachability tách biệt;
> `@spider-mesh/tcp` no longer
> contains `UdpDiscovery`.

Observed on 2026-07-04 in a production BVLS deployment using:

- `@spider-mesh/core@2.0.156`
- `@spider-mesh/tcp@2.0.156`
- one long-running monitor process consuming `RemoteService.nodes`
- ten remote worker processes exposing the same service
- shared setup cũ per process: one `Registry`, `Http2Rpc(registry)`, and `UdpDiscovery(registry)`

## Symptom

The monitor process initially discovers all workers and can call RPC methods:

```text
Total 10 workers
Checked 10 workers in 8 ms
```

After running for a while, the local discovery state in the monitor degrades gradually:

```text
Checked 10 workers
Checked 9 workers
Total 9 workers
Checked 5 workers
Total 5 workers
Checked 4 workers
Total 4 workers
Checked 2 workers
Total 2 workers
Checked 1 workers
Total 1 workers
Checked 0 workers
Total 0 workers
```

Once `RemoteService.nodes` reaches zero, the monitor stops polling workers, so downstream state
stops updating even though the workers themselves are still online.

Restarting only the monitor immediately restores discovery:

```text
[monitor] up
Total 10 workers
Checked 10 workers in 30 ms
```

## Evidence That Workers Are Not Actually Down

During the same time window:

- PM2 shows the worker processes still online.
- Other services can still route work to worker IPs.
- The issue is local to the affected process's in-memory discovery/registry state.
- Restarting the affected process, without restarting workers, restores the full worker list.

This points to `UdpDiscovery`/`Registry` losing or pruning peers locally, rather than real service
unavailability across the mesh.

## Related Errors

The affected process also logs intermittent RPC errors:

```text
{
  code: "MICROSERVICE_OFFLINE",
  message: "RPC response stream ended unexpectedly",
}
```

These may be a consequence of stale/disappearing registry entries, or a contributing cause if failed
RPC streams trigger node removal indirectly.

## Expected Behavior

For a stable set of long-running providers:

- `RemoteService.nodes` should not decay from 10 to 0 unless the providers actually stop advertising
  and become unreachable.
- Temporary missed UDP heartbeats should not permanently remove all peers from a consumer process if
  future advertisements are still arriving.
- A consumer process should be able to recover discovery without requiring a process restart.

## Suspected Area

Likely areas to inspect:

- `src/UdpDiscovery.ts`
- peer TTL / pruning logic
- watch/list node propagation from discovery into the shared `Registry`
- whether an RPC stream failure can remove peers from the registry
- whether UDP socket membership or receive loop can silently stop while the process remains alive

## Desired Regression Test

Add an e2e test that runs one observer/client and several provider processes long enough to verify:

1. The observer sees all providers.
2. Providers remain alive and continue advertising.
3. The observer's `RemoteService.nodes` does not decay to zero.
4. If discovery packets are temporarily missed, later packets repopulate the registry without
   restarting the observer.

The production workaround was to restart the monitor when it sees zero workers for several checks,
but the package should ideally recover discovery in-process.
