/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.internal.alto;

import com.hazelcast.cluster.Address;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.server.ServerConnectionManager;
import com.hazelcast.internal.server.tcp.TcpServerConnection;
import com.hazelcast.internal.tpc.TpcEngine;
import com.hazelcast.internal.util.HashUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.internal.tpc.AsyncSocket;
import com.hazelcast.internal.tpc.Eventloop;
import com.hazelcast.internal.tpc.ReadHandler;
import com.hazelcast.internal.tpc.epoll.EpollAsyncServerSocket;
import com.hazelcast.internal.tpc.epoll.EpollEventloop;
import com.hazelcast.internal.tpc.epoll.EpollReadHandler;
import com.hazelcast.internal.tpc.iobuffer.ConcurrentIOBufferAllocator;
import com.hazelcast.internal.tpc.iobuffer.IOBuffer;
import com.hazelcast.internal.tpc.iobuffer.IOBufferAllocator;
import com.hazelcast.internal.tpc.iobuffer.NonConcurrentIOBufferAllocator;
import com.hazelcast.internal.tpc.iobuffer.UnpooledIOBufferAllocator;
import com.hazelcast.internal.tpc.nio.NioAsyncServerSocket;
import com.hazelcast.internal.tpc.nio.NioAsyncSocket;
import com.hazelcast.internal.tpc.nio.NioEventloop;
import com.hazelcast.internal.tpc.nio.NioAsyncReadHandler;
import com.hazelcast.table.impl.TableManager;
import com.hazelcast.internal.tpc.epoll.EpollAsyncSocket;
import com.hazelcast.internal.tpc.iouring.IOUringAsyncServerSocket;
import com.hazelcast.internal.tpc.iouring.IOUringAsyncSocket;
import com.hazelcast.internal.tpc.iouring.IOUringEventloop;
import com.hazelcast.internal.tpc.iouring.IOUringAsyncReadHandler;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static com.hazelcast.internal.util.HashUtil.hashToIndex;
import static com.hazelcast.internal.alto.FrameCodec.OFFSET_REQ_CALL_ID;
import static java.util.concurrent.TimeUnit.SECONDS;


/**
 * The AltoRuntime is runtime that provides the infrastructure to build next generation data-structures.
 * For more information see:
 * https://www.micahlerner.com/2022/06/04/data-parallel-actors-a-programming-model-for-scalable-query-serving-systems.html
= * <p>
 * Mapping from partition to CPU is easy; just a simple mod.
 * <p>
 * RSS: How can we align:
 * - the CPU receiving data from some TCP/IP-connection.
 * - and pinning the same CPU to the RX-queue that processes that TCP/IP-connection
 * So how can we make sure that all TCP/IP-connections for that CPU are processed by the same CPU processing the IRQ.
 * <p>
 * And how can we make sure that for example we want to isolate a few CPUs for the RSS part, but then
 * forward to the CPU that owns the TCP/IP-connection
 * <p>
 * So it appears that Seastar is using the toeplitz hash
 * https://github.com/scylladb/seastar/issues/654
 * <p>
 * So we have a list of channels to some machine.
 * <p>
 * And we determine for each of the channel the toeplitz hash based on src/dst port/ip;
 * <p>
 * So this would determine which channels are mapped to some CPU.
 * <p>
 * So how do we go from partition to a channel?
 */
public class AltoRuntime {

    public final NodeEngineImpl nodeEngine;
    public final InternalSerializationService ss;
    public final ILogger logger;
    private final Address thisAddress;
    private final int socketCount;
    private final SocketConfig socketConfig;
    private final boolean poolRequests;
    private final boolean poolRemoteResponses;
    private final boolean writeThrough;
    private final int requestTimeoutMs;
    private final boolean regularSchedule;
    private final ResponseHandler responseHandler;
    private volatile ServerConnectionManager connectionManager;
    public volatile boolean shuttingdown = false;
    public final Managers managers;
    private final RequestRegistry requestRegistry;
    private TpcEngine tpcEngine;
    private final int concurrentRequestLimit;
    private final Map<Eventloop, Supplier<? extends ReadHandler>> readHandlerSuppliers = new HashMap<>();
    private PartitionActorRef[] partitionActorRefs;

    public AltoRuntime(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(AltoRuntime.class);
        this.ss = (InternalSerializationService) nodeEngine.getSerializationService();
        int responseThreadCount = Integer.parseInt(System.getProperty("reactor.responsethread.count", "1"));
        boolean responseThreadSpin = Boolean.parseBoolean(System.getProperty("reactor.responsethread.spin", "false"));
        this.writeThrough = Boolean.parseBoolean(java.lang.System.getProperty("reactor.write-through", "false"));
        this.regularSchedule = Boolean.parseBoolean(java.lang.System.getProperty("reactor.regular-schedule", "true"));
        this.poolRequests = Boolean.parseBoolean(java.lang.System.getProperty("reactor.pool-requests", "true"));
        boolean poolLocalResponses = Boolean.parseBoolean(System.getProperty("reactor.pool-local-responses", "true"));
        this.poolRemoteResponses = Boolean.parseBoolean(java.lang.System.getProperty("reactor.pool-remote-responses", "false"));
        this.concurrentRequestLimit = Integer.parseInt(java.lang.System.getProperty("reactor.concurrent-request-limit", "-1"));
        this.requestTimeoutMs = Integer.parseInt(java.lang.System.getProperty("reactor.request.timeoutMs", "23000"));
        this.socketCount = Integer.parseInt(java.lang.System.getProperty("reactor.channels", "" + Runtime.getRuntime().availableProcessors()));

        this.partitionActorRefs = new PartitionActorRef[271];

        this.requestRegistry = new RequestRegistry(concurrentRequestLimit, partitionActorRefs.length);
        this.responseHandler = new ResponseHandler(responseThreadCount,
                responseThreadSpin,
                requestRegistry);
        this.thisAddress = nodeEngine.getThisAddress();
        this.tpcEngine = newEngine();

        this.socketConfig = new SocketConfig();
        this.managers = new Managers();
        //hack
        managers.tableManager = new TableManager(partitionActorRefs.length);
    }

    public TpcEngine getTpcEngine() {
        return tpcEngine;
    }

    public int getRequestTimeoutMs() {
        return requestTimeoutMs;
    }

    @NotNull
    private TpcEngine newEngine() {
        TpcEngine.Configuration configuration = new TpcEngine.Configuration();
        configuration.setThreadFactory(AltoEventloopThread::new);
        configuration.setEventloopConfigUpdater(eventloopConfiguration -> {
            // remote responses will be created and released by the TPC thread.
            // So a non-concurrent allocator is good enough.
            IOBufferAllocator remoteResponseAllocator = new NonConcurrentIOBufferAllocator(128, true);
            // local responses will be created by the TPC thread, but will be released by a user thread.
            // So a concurrent allocator is needed.
            IOBufferAllocator localResponseAllocator = new ConcurrentIOBufferAllocator(128, true);

            OpScheduler opScheduler = new OpScheduler(32768,
                    Integer.MAX_VALUE,
                    managers,
                    localResponseAllocator,
                    remoteResponseAllocator,
                    responseHandler);

            eventloopConfiguration.setScheduler(opScheduler);
        });

        TpcEngine engine = new TpcEngine(configuration);

        if (socketCount % engine.eventloopCount() != 0) {
            throw new IllegalStateException("socket count is not multiple of eventloop count");
        }

        return engine;
    }

    public void start() {
        logger.info("AltoRuntime starting");
        tpcEngine.start();

        Eventloop.Type eventloopType = tpcEngine.eventloopType();
        switch (eventloopType) {
            case NIO:
                startNio();
                break;
            case EPOLL:
                startEpoll();
                break;
            case IOURING:
                startIOUring();
                break;
            default:
                throw new IllegalStateException("Unknown eventloopType:" + eventloopType);
        }

        responseHandler.start();

        for (int partitionId = 0; partitionId < partitionActorRefs.length; partitionId++) {
            partitionActorRefs[partitionId] = new PartitionActorRef(
                    partitionId,
                    nodeEngine.getPartitionService(),
                    tpcEngine,
                    this,
                    thisAddress,
                    requestRegistry.getByPartitionId(partitionId));
        }

        logger.info("AltoRuntime started");
    }

    private void startNio() {
        for (int k = 0; k < tpcEngine.eventloopCount(); k++) {
            NioEventloop eventloop = (NioEventloop) tpcEngine.eventloop(k);

            Supplier<NioAsyncReadHandler> readHandlerSupplier = () -> {
                RequestNioReadHandler readHandler = new RequestNioReadHandler();
                readHandler.opScheduler = (OpScheduler) eventloop.scheduler();
                readHandler.responseHandler = responseHandler;
                readHandler.requestIOBufferAllocator = poolRequests
                        ? new NonConcurrentIOBufferAllocator(128, true)
                        : new UnpooledIOBufferAllocator();
                readHandler.remoteResponseIOBufferAllocator = poolRemoteResponses
                        ? new ConcurrentIOBufferAllocator(128, true)
                        : new UnpooledIOBufferAllocator();
                return readHandler;
            };
            readHandlerSuppliers.put(eventloop, readHandlerSupplier);

            try {
                int port = toPort(thisAddress, k);
                NioAsyncServerSocket serverSocket = NioAsyncServerSocket.open(eventloop);
                serverSocket.receiveBufferSize(socketConfig.receiveBufferSize);
                serverSocket.reuseAddress(true);
                serverSocket.bind(new InetSocketAddress(thisAddress.getInetAddress(), port));
                serverSocket.accept(socket -> {
                    socket.readHandler(readHandlerSuppliers.get(eventloop).get());
                    socket.setWriteThrough(writeThrough);
                    socket.setRegularSchedule(regularSchedule);
                    socket.sendBufferSize(socketConfig.sendBufferSize);
                    socket.receiveBufferSize(socketConfig.receiveBufferSize);
                    socket.tcpNoDelay(socketConfig.tcpNoDelay);
                    socket.keepAlive(true);
                    socket.activate(eventloop);
                });
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    public PartitionActorRef[] partitionActorRefs() {
        return partitionActorRefs;
    }

    private void startIOUring() {
        for (int k = 0; k < tpcEngine.eventloopCount(); k++) {
            IOUringEventloop eventloop = (IOUringEventloop) tpcEngine.eventloop(k);
            try {
                Supplier<IOUringAsyncReadHandler> readHandlerSupplier = () -> {
                    RequestIOUringReadHandler readHandler = new RequestIOUringReadHandler();
                    readHandler.opScheduler = (OpScheduler) eventloop.scheduler();
                    readHandler.responseHandler = responseHandler;
                    readHandler.requestIOBufferAllocator = poolRequests
                            ? new NonConcurrentIOBufferAllocator(128, true)
                            : new UnpooledIOBufferAllocator();
                    readHandler.remoteResponseIOBufferAllocator = poolRemoteResponses
                            ? new ConcurrentIOBufferAllocator(128, true)
                            : new UnpooledIOBufferAllocator();
                    return readHandler;
                };
                readHandlerSuppliers.put(eventloop, readHandlerSupplier);

                int port = toPort(thisAddress, k);

                IOUringAsyncServerSocket serverSocket = IOUringAsyncServerSocket.open(eventloop);
                serverSocket.receiveBufferSize(socketConfig.receiveBufferSize);
                serverSocket.reuseAddress(true);
                serverSocket.bind(new InetSocketAddress(thisAddress.getInetAddress(), port));
                serverSocket.listen(10);
                serverSocket.accept(socket -> {
                    socket.readHandler(readHandlerSuppliers.get(eventloop).get());
                    socket.sendBufferSize(socketConfig.sendBufferSize);
                    socket.receiveBufferSize(socketConfig.receiveBufferSize);
                    socket.tcpNoDelay(socketConfig.tcpNoDelay);
                    socket.keepAlive(true);
                    socket.activate(eventloop);
                });
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private void startEpoll() {
        for (int k = 0; k < tpcEngine.eventloopCount(); k++) {
            EpollEventloop eventloop = (EpollEventloop) tpcEngine.eventloop(k);
            try {
                Supplier<EpollReadHandler> readHandlerSupplier = () -> {
                    RequestEpollReadHandler readHandler = new RequestEpollReadHandler();
                    readHandler.opScheduler = (OpScheduler) eventloop.scheduler();
                    readHandler.responseHandler = responseHandler;
                    readHandler.requestIOBufferAllocator = poolRequests
                            ? new NonConcurrentIOBufferAllocator(128, true)
                            : new UnpooledIOBufferAllocator();
                    readHandler.remoteResponseIOBufferAllocator = poolRemoteResponses
                            ? new ConcurrentIOBufferAllocator(128, true)
                            : new UnpooledIOBufferAllocator();
                    return readHandler;
                };
                readHandlerSuppliers.put(eventloop, readHandlerSupplier);

                int port = toPort(thisAddress, k);

                EpollAsyncServerSocket serverSocket = EpollAsyncServerSocket.open(eventloop);
                serverSocket.receiveBufferSize(socketConfig.receiveBufferSize);
                serverSocket.reuseAddress(true);
                serverSocket.bind(new InetSocketAddress(thisAddress.getInetAddress(), port));
                serverSocket.listen(10);
                serverSocket.accept(socket -> {
                    socket.readHandler(readHandlerSuppliers.get(eventloop).get());
                    socket.sendBufferSize(socketConfig.sendBufferSize);
                    socket.receiveBufferSize(socketConfig.receiveBufferSize);
                    socket.tcpNoDelay(socketConfig.tcpNoDelay);
                    socket.keepAlive(true);
                    socket.activate(eventloop);
                });
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    public int toPort(Address address, int socketId) {
        return (address.getPort() - 5701) * 100 + 11000 + socketId % tpcEngine.eventloopCount();
    }

    private void ensureActive() {
        if (shuttingdown) {
            throw new RuntimeException("Can't make invocation, frontend shutting down");
        }
    }

    public void shutdown() {
        logger.info("AltoRuntime shutdown");

        shuttingdown = true;

        tpcEngine.shutdown();

        requestRegistry.shutdown();

        responseHandler.shutdown();

        try {
            tpcEngine.awaitTermination(5, SECONDS);
        } catch (InterruptedException e) {
            logger.warning("TpcEngine failed to terminate.");
            Thread.currentThread().interrupt();
        }

        logger.info("AltoRuntime terminated");
    }

    public RequestFuture invoke(IOBuffer request, AsyncSocket socket) {
        ensureActive();

        RequestFuture future = new RequestFuture(request);
        // we need to acquire the frame because storage will release it once written
        // and we need to keep the frame around for the response.
        request.acquire();
        Requests requests = requestRegistry.getRequestsOrCreate(socket.remoteAddress());
        long callId = requests.nextCallId();
        request.putLong(OFFSET_REQ_CALL_ID, callId);
        //System.out.println("request.refCount:"+request.refCount());
        //  requests.map.put(callId, future);
        socket.writeAndFlush(request);
        return future;
    }

    TcpServerConnection getConnection(Address address) {
        if (connectionManager == null) {
            connectionManager = nodeEngine.getNode().getServer().getConnectionManager(EndpointQualifier.MEMBER);
        }

        TcpServerConnection connection = (TcpServerConnection) connectionManager.get(address);
        if (connection == null) {
            connectionManager.getOrConnect(address);
            for (int k = 0; k < 60; k++) {
                try {
                    System.out.println("Waiting for connection: " + address);
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                }
                connection = (TcpServerConnection) connectionManager.get(address);
                if (connection != null) {
                    break;
                }
            }

            if (connection == null) {
                throw new RuntimeException("Could not connect to : " + address);
            }
        }

        if (connection.sockets == null) {
            synchronized (connection) {
                if (connection.sockets == null) {
                    AsyncSocket[] sockets = new AsyncSocket[socketCount];

                    for (int socketIndex = 0; socketIndex < sockets.length; socketIndex++) {
                        SocketAddress eventloopAddress = new InetSocketAddress(address.getHost(), toPort(address, socketIndex));
                        sockets[socketIndex] = connect(eventloopAddress, socketIndex);
                    }

                    connection.sockets = sockets;
                }
            }
        }

        return connection;
    }

    public AsyncSocket connect(SocketAddress address, int channelIndex) {
        int eventloopIndex = HashUtil.hashToIndex(channelIndex, tpcEngine.eventloopCount());
        Eventloop eventloop = tpcEngine.eventloop(eventloopIndex);

        AsyncSocket socket;
        switch (tpcEngine.eventloopType()) {
            case NIO:
                NioAsyncSocket nioSocket = NioAsyncSocket.open();
                nioSocket.setWriteThrough(writeThrough);
                nioSocket.setRegularSchedule(regularSchedule);
                socket = nioSocket;
                break;
            case IOURING:
                socket = IOUringAsyncSocket.open();
                break;
            case EPOLL:
                socket = EpollAsyncSocket.open();
                break;
            default:
                throw new RuntimeException();
        }

        socket.readHandler(readHandlerSuppliers.get(eventloop).get());
        socket.sendBufferSize(socketConfig.sendBufferSize);
        socket.receiveBufferSize(socketConfig.receiveBufferSize);
        socket.tcpNoDelay(socketConfig.tcpNoDelay);
        socket.activate(eventloop);

        CompletableFuture future = socket.connect(address);
        future.join();
        System.out.println("AsyncSocket " + address + " connected");
        return socket;
    }
}
