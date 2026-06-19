import { BehaviorSubject, catchError, combineLatest, distinctUntilChanged, EMPTY, filter, finalize, firstValueFrom, from, lastValueFrom, map, mergeMap, NEVER, Observable, of, retry, share, Subject, Subscription, switchMap, take, tap, throwError, timeout, timer } from "rxjs"
import { listBeforeMicroserviceOnlineMethods } from "./decorators/BeforeMicroserviceOnline.js";
import { LOCAL_SERVICES$ } from "./decorators/Microservice.js";
import { SPIDERMESH_NAMESPACE, SPIDERMESH_NODE_HOSTNAME, SPIDERMESH_NODE_ID, SPIDERMESH_BUILD_VERSION, SPIDERMESH_BUILD_GIT_TAG, SPIDERMESH_BUILD_GIT_BRANCH, SPIDERMESH_BUILD_GIT_COMMIT, SPIDERMESH_BUILD_TIME, SPIDERMESH_BUILD_ENVIRONMENT, SPIDERMESH_BUILD_TAGS } from "../const.js";
import { SpiderMeshNode, BuildInfo, RpcTransporter, PubsubTransporter, DiscoveryTransporter, RpcOptions, SpiderMeshError, RpcEvent, RpcResponsePacket, MeshTransporter, RpcCancelPacket, NodeRef, ServiceDirectory } from './types.js'

export type HelloEvent = SpiderMeshNode & { back?: boolean }
export type ServiceChecker = (nodes: NodeRef[]) => Promise<boolean> | boolean
export type NodesMap = {
    nodes: Map<string, SpiderMeshNode>,
    last_updated_node_id: string
}

type PendingRpcStream = {
    stream: Subject<any>
    finished: boolean
}

type RpcTransportTarget = {
    node_id?: string
    transporter: RpcTransporter
}

const isSubscribable = (value: unknown): value is Observable<unknown> => {
    return !!value && typeof value === 'object' && typeof (value as { subscribe?: unknown }).subscribe === 'function'
}

const isNamedTransporter = (value: unknown): value is { constructor?: { name?: string } } => {
    return !!value && typeof value === 'object'
}

export class SpiderMesh {

    public readonly node_id = SPIDERMESH_NODE_ID ?? `${Date.now().toString(36).toUpperCase()}|${Math.random().toString(36).slice(10).toUpperCase()}`
    public readonly namespace = SPIDERMESH_NAMESPACE
    #transporters = {
        rpcs: new BehaviorSubject(new Map<string, RpcTransporter>()),
        pubsubs: new BehaviorSubject(new Map<string, PubsubTransporter>()),
        discoveries: new BehaviorSubject(new Map<string, DiscoveryTransporter>())
    }
    #localServices = new Map<string, any>()
    #me$ = new BehaviorSubject<SpiderMeshNode>({
        host: SPIDERMESH_NODE_HOSTNAME,
        namespace: SPIDERMESH_NAMESPACE,
        node_id: this.node_id,
        topics: [],
        services: {},
        transporters: {},
        nodes: {},
        version: 0,
        build: SpiderMesh.#readBuildInfo(),
    })

    static #readBuildInfo(): BuildInfo | undefined {
        const runtime = (() => {
            if (typeof Bun !== 'undefined') return `bun@${(Bun as any).version}`
            if (typeof process !== 'undefined') return `node@${process.version}`
            return undefined
        })()

        const info: BuildInfo = {
            version: SPIDERMESH_BUILD_VERSION,
            git_tag: SPIDERMESH_BUILD_GIT_TAG,
            git_branch: SPIDERMESH_BUILD_GIT_BRANCH,
            git_commit: SPIDERMESH_BUILD_GIT_COMMIT,
            build_time: SPIDERMESH_BUILD_TIME,
            environment: SPIDERMESH_BUILD_ENVIRONMENT,
            runtime,
            tags: SPIDERMESH_BUILD_TAGS,
        }

        // Only attach if at least one field is set
        return Object.values(info).some(v => v != null) ? info : undefined
    }

    #rpc = {
        pending: new Map<string, PendingRpcStream>(),
        running: new Map<string, Subscription>()
    }

    constructor() {
        LOCAL_SERVICES$.pipe(
            mergeMap(async service => {
                const list = listBeforeMicroserviceOnlineMethods(service.instance)
                for (const method of list) {
                    await service.instance[method]()
                }

                this.#localServices.set(service.name, service.instance)
                this.#refresh({
                    services: {
                        [service.name]: service.metadata
                    }
                })
            }, 1),
            catchError(() => EMPTY)
        ).subscribe()
    }


    registerTransporter(meshTransporter: MeshTransporter, name?: string) {
        const resolvedName = name
            || (isNamedTransporter(meshTransporter) ? meshTransporter.constructor?.name : undefined)
            || 'AnonymousTransporter'
        const subscription = new Subscription()

        if (this.#isRpcTransporter(meshTransporter)) {
            subscription.add(this.#linkRpcTransporter(resolvedName, meshTransporter).subscribe())
        }
        if (this.#isPubsubTransporter(meshTransporter)) {
            subscription.add(this.#linkPubsubTransporter(resolvedName, meshTransporter).subscribe())
        }
        if (this.#isDiscoveryTransporter(meshTransporter)) {
            subscription.add(this.#linkDiscoveryTransporter(resolvedName, meshTransporter).subscribe())
        }

        return subscription
    }

    /**
     * Unique transporter instances that expose a `ServiceDirectory` (availability source).
     * A single instance may appear in several capability maps (e.g. a WS transporter that
     * serves rpc + pubsub + discovery), so we de-duplicate by instance identity.
     */
    #serviceDirectories(): ServiceDirectory[] {
        const seen = new Set<unknown>()
        const directories: ServiceDirectory[] = []
        for (const map of [this.#transporters.discoveries.value, this.#transporters.rpcs.value, this.#transporters.pubsubs.value]) {
            for (const transporter of map.values()) {
                if (seen.has(transporter)) continue
                seen.add(transporter)
                const candidate = transporter as Partial<ServiceDirectory>
                if (typeof candidate.watchService === 'function' && typeof candidate.listNodes === 'function') {
                    directories.push(transporter as unknown as ServiceDirectory)
                }
            }
        }
        return directories
    }

    listRpcNodes(service: string): NodeRef[] {
        const seen = new Set<string>()
        const nodes: NodeRef[] = []
        for (const directory of this.#serviceDirectories()) {
            for (const node of directory.listNodes(service)) {
                if (seen.has(node.node_id)) continue
                seen.add(node.node_id)
                nodes.push(node)
            }
        }
        return nodes
    }

    watchService(service: string): Observable<NodeRef[]> {
        return combineLatest([
            this.#transporters.rpcs,
            this.#transporters.pubsubs,
            this.#transporters.discoveries,
        ]).pipe(
            map(() => this.#serviceDirectories()),
            switchMap(directories => {
                if (!directories.length) return of([] as NodeRef[])
                return combineLatest(
                    directories.map(directory => directory.watchService(service))
                ).pipe(
                    map(lists => {
                        const seen = new Set<string>()
                        const merged: NodeRef[] = []
                        for (const node of lists.flat()) {
                            if (seen.has(node.node_id)) continue
                            seen.add(node.node_id)
                            merged.push(node)
                        }
                        return merged
                    }),
                )
            }),
            distinctUntilChanged((prev, curr) => {
                if (prev.length !== curr.length) return false
                const prevIds = new Set(prev.map(n => n.node_id))
                return curr.every(n => prevIds.has(n.node_id))
            })
        )
    }

    #selectRpcTransport(filters: Partial<Pick<RpcOptions<any>, 'node_id' | 'service' | 'transporter'>> = {}): RpcTransporter | undefined {
        if (!filters.service) return undefined
        // Explicit binding always wins.
        if (filters.transporter) {
            const name = typeof filters.transporter === 'string' ? filters.transporter : filters.transporter.name
            if (!name) return
            return this.#transporters.rpcs.value.get(name)
        }

        const rpcs = this.#transporters.rpcs.value
        // Nothing to choose from — probe canRoute only when at least one RPC transporter exists.
        if (rpcs.size === 0) return undefined

        // Reachability-first: the first-registered transporter may not actually be able to
        // reach the provider (e.g. a LAN Http2Rpc has no route to a relay-only peer that the
        // WS transporter sees). Prefer a transporter that declares it can route to this
        // service/node, so routing follows the same source of truth as wait()/watch()/nodes.
        for (const transporter of rpcs.values()) {
            if (transporter.canRoute(filters.service, filters.node_id)) {
                return transporter
            }
        }

        // Fallback: the only/first RPC transporter (typical single-transport / relay setup).
        // Kept so a sole transporter still routes even when its canRoute reported no route
        // yet — the send path then surfaces the real error instead of "no route at all".
        return rpcs.values().next().value
    }

    #normalizeRpcError(error: any): SpiderMeshError | { code?: string, message: string } {
        if (error && typeof error === 'object') {
            return {
                code: typeof error.code === 'string' ? error.code : undefined,
                message: typeof error.message === 'string' ? error.message : 'Unknown RPC error'
            }
        }

        return { message: typeof error === 'string' ? error : 'Unknown RPC error' }
    }

    #completePendingRpc(request_id: string) {
        this.#rpc.pending.delete(request_id)
    }

    #completeRunningRpc(request_id: string) {
        this.#rpc.running.delete(request_id)
    }

    #index = 1
    callRemoteService<R, T>(options: RpcOptions<T>) {
        return of(1).pipe(
            switchMap(() => firstValueFrom(this.watchService(options.service), { defaultValue: [] as NodeRef[] })),
            take(1),
            mergeMap(() => {
                const transporter = this.#selectRpcTransport(options)
                if (!transporter) throw { code: 'MICROSERVICE_OFFLINE', message: `No transporter available for service ${options.service}` }
                return new Observable<R>(subscriber => {
                    const request_id = `${this.node_id}:${Date.now().toString(36)}:${(this.#index++).toString(36)}`
                    const pending = {
                        stream: new Subject<R>(),
                        finished: false
                    }
                    this.#rpc.pending.set(request_id, pending)

                    const subscription = pending.stream.subscribe({
                        next: value => subscriber.next(value),
                        error: error => {
                            pending.finished = true
                            subscriber.error(error)
                        },
                        complete: () => {
                            pending.finished = true
                            subscriber.complete()
                        }
                    })

                    let cancelSend: (() => void) | undefined
                    transporter.send({
                        kind: 'request',
                        request_id,
                        sender_node_id: this.node_id,
                        destination_node_id: options.node_id,
                        service: options.service,
                        method: options.method,
                        args: options.args
                    }).then(({ cancel }) => {
                        cancelSend = cancel
                    }).catch((error: any) => {
                        pending.finished = true
                        pending.stream.error(this.#normalizeRpcError(error))
                        this.#completePendingRpc(request_id)
                    })

                    return () => {
                        subscription.unsubscribe()
                        if (!pending.finished) {
                            cancelSend?.()
                        }
                        this.#completePendingRpc(request_id)
                    }
                })
            }),
            options.timeout ? timeout({
                each: options.timeout,
                with: () => throwError(() => ({ code: 'MICROSERVICE_RPC_TIMEOUT', message: 'RPC timeout' }))
            }) : tap(),
            retry({
                delay: (e: { code: string }, count: number) => {
                    if (e.code === 'MICROSERVICE_OFFLINE') {
                        if (options.retry && count < options.retry) return timer(1000)
                    }
                    throw e
                }
            }),
            catchError(e => {
                if (options.fallback != undefined) return of(options.fallback as any as T)
                throw e
            })
        )
    }

    #linkRpcTransporter(name: string, transporter: RpcTransporter) {
        this.#transporters.rpcs.next(new Map(this.#transporters.rpcs.value).set(name, transporter))
        this.#ensureLocalTransporterPresence(name)

        const initialEndpoints = (transporter as RpcTransporter & { metadata?: RpcEvent['endpoints'] }).metadata
        if (initialEndpoints) {
            this.#refresh({
                transporters: {
                    [name]: initialEndpoints
                }
            })
        }

        return transporter.pipe(
            map(({ rpc, offline, endpoints }) => {
                if (rpc) {
                    const packet = rpc

                    if (packet.kind == 'request') {
                        const reply = async (response: Omit<RpcResponsePacket, 'kind' | 'request_id' | 'destination_node_id'>) => {
                            await transporter.send({
                                kind: 'response',
                                request_id: packet.request_id,
                                destination_node_id: packet.sender_node_id,
                                ...response
                            })
                        }

                        const handleResponse = (response: any) => {
                            if (isSubscribable(response)) {
                                let queue = Promise.resolve()
                                const running = response.subscribe({
                                    next: data => {
                                        queue = queue.then(() => reply({ data }))
                                    },
                                    error: error => {
                                        queue = queue.then(() => reply({
                                            error: this.#normalizeRpcError(error),
                                            completed: true
                                        })).finally(() => this.#completeRunningRpc(packet.request_id))
                                    },
                                    complete: () => {
                                        queue = queue.then(() => reply({ completed: true })).finally(() => this.#completeRunningRpc(packet.request_id))
                                    }
                                })

                                this.#rpc.running.set(packet.request_id, running)
                                return
                            }

                            void reply({ data: response, completed: true })
                        }

                        const service = this.#localServices.get(packet.service)
                        if (!service || typeof service[packet.method] !== 'function') {
                            void reply({
                                error: {
                                    code: 'MICROSERVICE_NOT_FOUND',
                                    message: `Service ${packet.service}.${packet.method} not found`
                                },
                                completed: true
                            })
                        } else {
                            try {
                                const response = service[packet.method].apply(service, packet.args)

                                Promise.resolve(response)
                                    .then(handleResponse)
                                    .catch(error => reply({
                                        error: this.#normalizeRpcError(error),
                                        completed: true
                                    }))
                            } catch (error) {
                                void reply({
                                    error: this.#normalizeRpcError(error),
                                    completed: true
                                })
                            }
                        }
                    }

                    if (packet?.kind === 'response') {
                        const pending = this.#rpc.pending.get(packet.request_id)
                        if (pending) {
                            if ('data' in packet) {
                                pending.stream.next(packet.data)
                            }

                            if ('error' in packet && packet.error != undefined) {
                                pending.finished = true
                                pending.stream.error(packet.error)
                                this.#completePendingRpc(packet.request_id)
                            } else if (packet.completed) {
                                pending.finished = true
                                pending.stream.complete()
                                this.#completePendingRpc(packet.request_id)
                            }
                        }
                    }

                    if (packet?.kind === 'cancel') {
                        const stream = this.#rpc.running.get(packet.request_id)
                        if (stream) {
                            stream.unsubscribe()
                            this.#completeRunningRpc(packet.request_id)
                        }
                    }
                }

                if (endpoints) {
                    this.#refresh({
                        transporters: {
                            [name]: endpoints
                        }
                    })
                }
            }),
            finalize(() => undefined)
        )
    }

    #linkPubsubTransporter(name: string, transporter: PubsubTransporter) {
        this.#transporters.pubsubs.next(new Map(this.#transporters.pubsubs.value).set(name, transporter))
        this.#ensureLocalTransporterPresence(name)

        return transporter.pipe(
            tap(({ endpoints }) => {
                this.#refresh({
                    transporters: {
                        [name]: endpoints
                    }
                })
            }),
            finalize(() => undefined)
        )
    }

    #linkDiscoveryTransporter(name: string, transporter: DiscoveryTransporter) {
        this.#transporters.discoveries.next(new Map(this.#transporters.discoveries.value).set(name, transporter))
        this.#ensureLocalTransporterPresence(name)

        const announce = this.#me$.subscribe(metadata => {
            void transporter.broadcast({
                hi: true,
                node: metadata,
                sender_id: this.node_id,
            }).catch(() => undefined)
        })

        // Core no longer ingests discovered peers — each discovery-capable transporter
        // owns its peer table and exposes it via ServiceDirectory. We only keep the
        // outbound self-announce alive for the lifetime of this subscription.
        return transporter.pipe(
            finalize(() => {
                announce.unsubscribe()
            })
        )
    }

    #isRpcTransporter(transporter: MeshTransporter): transporter is RpcTransporter {
        return 'send' in transporter
    }

    #isPubsubTransporter(transporter: MeshTransporter): transporter is PubsubTransporter {
        return 'publish' in transporter
    }

    #isDiscoveryTransporter(transporter: MeshTransporter): transporter is DiscoveryTransporter {
        return 'broadcast' in transporter
    }

    #refresh(patch: Partial<Omit<SpiderMeshNode, 'version'>>) {
        const current = this.#me$.value
        this.#me$.next({
            ...current,
            ...patch,
            version: current.version + 1,
            topics: patch.topics || current.topics,
            services: {
                ...current.services,
                ...(patch.services || {})
            },
            nodes: {
                ...current.nodes,
                ...(patch.nodes || {})
            },
            transporters: {
                ...current.transporters,
                ...(patch.transporters || {})
            }
        })
    }

    #ensureLocalTransporterPresence(name: string) {
        if (Object.hasOwn(this.#me$.value.transporters, name)) return

        this.#refresh({
            transporters: {
                [name]: true
            }
        })
    }

    #registerTopic(topic: string) {
        if (this.#me$.value.topics.includes(topic)) return

        this.#refresh({
            topics: [...this.#me$.value.topics, topic]
        })
    }

    #unregisterTopic(topic: string) {
        if (!this.#me$.value.topics.includes(topic)) return

        this.#refresh({
            topics: this.#me$.value.topics.filter(item => item !== topic)
        })
    }

    linkEvent<T>(factory: { new(...args: any[]): T }) {
        const topic = factory.name
        const listen$ = new Observable<T>(subscriber => {
            this.#registerTopic(topic)

            const subscription = from(this.#transporters.pubsubs.value.values()).pipe(
                mergeMap(t => t.listen<T>(topic))
            ).subscribe(subscriber)

            return () => {
                subscription.unsubscribe()
                this.#unregisterTopic(topic)
            }
        }).pipe(share())

        return {
            publish: (data: T) => lastValueFrom(
                from(this.#transporters.pubsubs.value.values()).pipe(
                    mergeMap(t => t.publish(topic, data))
                ), { defaultValue: undefined as any as void }
            ),
            listen: () => listen$
        }
    }
}