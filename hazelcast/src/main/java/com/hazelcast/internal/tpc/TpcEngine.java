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

package com.hazelcast.internal.tpc;

import com.hazelcast.internal.util.ThreadAffinity;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.internal.util.executor.HazelcastManagedThread;
import com.hazelcast.internal.tpc.epoll.EpollEventloop.EpollConfiguration;
import com.hazelcast.internal.tpc.iouring.IOUringAsyncSocket;
import com.hazelcast.internal.tpc.iouring.IOUringEventloop.IOUringConfiguration;
import com.hazelcast.internal.tpc.nio.NioAsyncSocket;
import com.hazelcast.internal.tpc.nio.NioEventloop;
import com.hazelcast.internal.tpc.epoll.EpollEventloop;
import com.hazelcast.internal.tpc.iouring.IOUringEventloop;
import com.hazelcast.internal.tpc.nio.NioEventloop.NioConfiguration;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkPositive;
import static com.hazelcast.internal.tpc.TpcEngine.State.NEW;
import static com.hazelcast.internal.tpc.TpcEngine.State.RUNNING;
import static com.hazelcast.internal.tpc.TpcEngine.State.SHUTDOWN;
import static java.lang.System.currentTimeMillis;
import static java.lang.System.getProperty;

/**
 * The TpcEngine is effectively an array of eventloops
 * <p>
 * The TpcEngine is not aware of any specific applications. E.g. it could execute operations, but it
 * can equally well run client requests or completely different applications.
 */
public final class TpcEngine {

    private final ILogger logger = Logger.getLogger(getClass());
    private final boolean monitorSilent;
    private final Eventloop.Type eventloopType;
    private final int eventloopCount;
    private final Eventloop[] eventloops;
    private final MonitorThread monitorThread;
    private final AtomicReference<State> state = new AtomicReference<>(NEW);
    final CountDownLatch terminationLatch;

    /**
     * Creates an TpcEngine with the default {@link Configuration}.
     */
    public TpcEngine() {
        this(new Configuration());
    }

    /**
     * Creates an TpcEngine with the given Configuration.
     *
     * @param cfg the Configuration.
     * @throws NullPointerException when configuration is null.
     */
    public TpcEngine(Configuration cfg) {
        this.eventloopCount = cfg.eventloopCount;
        this.eventloopType = cfg.eventloopType;
        this.monitorSilent = cfg.monitorSilent;
        this.eventloops = new Eventloop[eventloopCount];
        this.monitorThread = new MonitorThread(eventloops, monitorSilent);
        this.terminationLatch = new CountDownLatch(eventloopCount);

        for (int idx = 0; idx < eventloopCount; idx++) {
            switch (eventloopType) {
                case NIO:
                    NioConfiguration nioCfg = new NioConfiguration();
                    nioCfg.setThreadAffinity(cfg.threadAffinity);
                    nioCfg.setThreadName("eventloop-" + idx);
                    nioCfg.setThreadFactory(cfg.threadFactory);
                    cfg.eventloopConfigUpdater.accept(nioCfg);
                    eventloops[idx] = new NioEventloop(nioCfg);
                    break;
                case EPOLL:
                    EpollConfiguration epollCfg = new EpollConfiguration();
                    epollCfg.setThreadAffinity(cfg.threadAffinity);
                    epollCfg.setThreadName("eventloop-" + idx);
                    epollCfg.setThreadFactory(cfg.threadFactory);
                    cfg.eventloopConfigUpdater.accept(epollCfg);
                    eventloops[idx] = new EpollEventloop(epollCfg);
                    break;
                case IOURING:
                    IOUringConfiguration ioUringCfg = new IOUringConfiguration();
                    ioUringCfg.setThreadName("eventloop-" + idx);
                    ioUringCfg.setThreadAffinity(cfg.threadAffinity);
                    ioUringCfg.setThreadFactory(cfg.threadFactory);
                    cfg.eventloopConfigUpdater.accept(ioUringCfg);
                    eventloops[idx] = new IOUringEventloop(ioUringCfg);
                    break;
                default:
                    throw new IllegalStateException("Unknown eventloopType:" + eventloopType);
            }
            eventloops[idx].engine = this;
        }
    }

    /**
     * Returns the TpcEngine State.
     * <p>
     * This method is thread-safe.
     *
     * @return the engine state.
     */
    public State state() {
        return state.get();
    }

    /**
     * Returns the type of Eventloop used by this TpcEngine.
     *
     * @return the type of Eventloop.
     */
    public Eventloop.Type eventloopType() {
        return eventloopType;
    }

    /**
     * Returns the eventloops.
     *
     * @return the {@link Eventloop}s.
     */
    public Eventloop[] eventloops() {
        return eventloops;
    }

    /**
     * Returns the number of Eventloop instances in this TpcEngine.
     * <p>
     * This method is thread-safe.
     *
     * @return the number of eventloop instances.
     */
    public int eventloopCount() {
        return eventloopCount;
    }

    /**
     * Gets the {@link Eventloop} at the given index.
     *
     * @param idx the index of the Eventloop.
     * @return The Eventloop at the given index.
     */
    public Eventloop eventloop(int idx) {
        return eventloops[idx];
    }

    /**
     * Starts the TpcEngine by starting all the {@link Eventloop} instances.
     *
     * @throws IllegalStateException if
     */
    public void start() {
        logger.info("Starting " + eventloopCount + " eventloops");

        for (; ; ) {
            State oldState = state.get();
            if (oldState != NEW) {
                throw new IllegalStateException("Can't start TpcEngine, it isn't in NEW state.");
            }

            if (!state.compareAndSet(oldState, RUNNING)) {
                continue;
            }

            for (Eventloop eventloop : eventloops) {
                eventloop.start();
            }

            monitorThread.start();
            return;
        }
    }

    /**
     * Shuts down the TpcEngine. If the TpcEngine is already shutdown or terminated, the call is ignored.
     * <p>
     * This method is thread-safe.
     */
    public void shutdown() {
        for (; ; ) {
            State oldState = state.get();
            switch (oldState) {
                case NEW:
                    if (!state.compareAndSet(oldState, SHUTDOWN)) {
                        continue;
                    }
                    break;
                case RUNNING:
                    if (!state.compareAndSet(oldState, SHUTDOWN)) {
                        continue;
                    }
                    break;
                case SHUTDOWN:
                    return;
                case TERMINATED:
                    return;
                default:
                    throw new IllegalStateException();
            }

            monitorThread.shutdown();

            for (Eventloop eventloop : eventloops) {
                eventloop.shutdown();
            }
        }
    }

    /**
     * Awaits for the termination of the TpcEngine.
     * <p>
     * This method is thread-safe.
     *
     * @param timeout the timeout
     * @param unit    the TimeUnit
     * @return true if the TpcEngine is terminated.
     * @throws InterruptedException if the calling thread got interrupted while waiting.
     */
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return terminationLatch.await(timeout, unit);
    }

    void notifyEventloopTerminated() {
        synchronized (terminationLatch) {
            if (terminationLatch.getCount() == 1) {
                state.set(State.TERMINATED);
            }
            terminationLatch.countDown();
        }
    }

    /**
     * Contains the configuration of the {@link TpcEngine}.
     */
    public static class Configuration {
        private int eventloopCount = Integer.parseInt(getProperty("reactor.count", "" + Runtime.getRuntime().availableProcessors()));
        private Eventloop.Type eventloopType = Eventloop.Type.fromString(getProperty("reactor.type", "nio"));
        private ThreadAffinity threadAffinity = ThreadAffinity.newSystemThreadAffinity("reactor.cpu-affinity");
        private boolean monitorSilent = Boolean.parseBoolean(getProperty("reactor.monitor.silent", "false"));
        private ThreadFactory threadFactory = HazelcastManagedThread::new;
        private Consumer<Eventloop.Configuration> eventloopConfigUpdater = configuration -> {
        };

        public void setThreadAffinity(ThreadAffinity threadAffinity) {
            this.threadAffinity = threadAffinity;
        }

        public void setThreadFactory(ThreadFactory threadFactory) {
            this.threadFactory = checkNotNull(threadFactory, "threadFactory can't be null");
        }

        public void setEventloopType(Eventloop.Type eventloopType) {
            this.eventloopType = checkNotNull(eventloopType, "eventloopType can't be null");
        }

        public void setEventloopConfigUpdater(Consumer<Eventloop.Configuration> eventloopConfigUpdater) {
            this.eventloopConfigUpdater = eventloopConfigUpdater;
        }

        public void setEventloopCount(int eventloopCount) {
            this.eventloopCount = checkPositive("reactorCount", eventloopCount);
        }

        public void setMonitorSilent(boolean monitorSilent) {
            this.monitorSilent = monitorSilent;
        }
    }

    public enum State {
        NEW,
        RUNNING,
        SHUTDOWN,
        TERMINATED
    }

    static private final class MonitorThread extends Thread {

        private final Eventloop[] eventloops;
        private final boolean silent;
        private volatile boolean shutdown = false;
        private long prevMillis = currentTimeMillis();
        // There is a memory leak on the counters. When channels die, counters are not removed.
        private final Map<SwCounter, LongHolder> prevMap = new HashMap<>();

        private MonitorThread(Eventloop[] eventloops, boolean silent) {
            super("MonitorThread");
            this.eventloops = eventloops;
            this.silent = silent;
        }

        static class LongHolder {
            public long value;
        }

        @Override
        public void run() {
            try {
                long nextWakeup = currentTimeMillis() + 5000;
                while (!shutdown) {
                    try {
                        long now = currentTimeMillis();
                        long remaining = nextWakeup - now;
                        if (remaining > 0) {
                            Thread.sleep(remaining);
                        }
                    } catch (InterruptedException e) {
                        return;
                    }

                    nextWakeup += 5000;

                    long currentMillis = currentTimeMillis();
                    long elapsed = currentMillis - prevMillis;
                    this.prevMillis = currentMillis;

                    for (Eventloop eventloop : eventloops) {
                        //                    for (AsyncSocket socket : eventloop.resources()) {
                        //                        monitor(socket, elapsed);
                        //                    }
                    }

                    if (!silent) {
                        for (Eventloop eventloop : eventloops) {
                            monitor(eventloop, elapsed);
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private void monitor(Eventloop eventloop, long elapsed) {
            //log(reactor + " request-count:" + reactor.requests.get());
            //log(reactor + " channel-count:" + reactor.channels().size());

            //        long requests = reactor.requests.get();
            //        LongHolder prevRequests = getPrev(reactor.requests);
            //        long requestsDelta = requests - prevRequests.value;
            //log(reactor + " " + thp(requestsDelta, elapsed) + " requests/second");
            //prevRequests.value = requests;
            //
            //        log("head-block:" + reactor.reactorQueue.head_block
            //                + " tail:" + reactor.reactorQueue.tail.get()
            //                + " dirtyHead:" + reactor.reactorQueue.dirtyHead.get()
            //                + " cachedTail:" + reactor.reactorQueue.cachedTail);
            ////
        }

        private void log(String s) {
            System.out.println("[monitor] " + s);
        }

        private void monitor(AsyncSocket socket, long elapsed) {
            long packetsRead = socket.ioBuffersRead.get();
            LongHolder prevPacketsRead = getPrev(socket.ioBuffersRead);
            long packetsReadDelta = packetsRead - prevPacketsRead.value;

            if (!silent) {
                log(socket + " " + thp(packetsReadDelta, elapsed) + " packets/second");

                long bytesRead = socket.bytesRead.get();
                LongHolder prevBytesRead = getPrev(socket.bytesRead);
                long bytesReadDelta = bytesRead - prevBytesRead.value;
                log(socket + " " + thp(bytesReadDelta, elapsed) + " bytes-read/second");
                prevBytesRead.value = bytesRead;

                long bytesWritten = socket.bytesWritten.get();
                LongHolder prevBytesWritten = getPrev(socket.bytesWritten);
                long bytesWrittenDelta = bytesWritten - prevBytesWritten.value;
                log(socket + " " + thp(bytesWrittenDelta, elapsed) + " bytes-written/second");
                prevBytesWritten.value = bytesWritten;

                long handleOutboundCalls = socket.handleWriteCnt.get();
                LongHolder prevHandleOutboundCalls = getPrev(socket.handleWriteCnt);
                long handleOutboundCallsDelta = handleOutboundCalls - prevHandleOutboundCalls.value;
                log(socket + " " + thp(handleOutboundCallsDelta, elapsed) + " handleOutbound-calls/second");
                prevHandleOutboundCalls.value = handleOutboundCalls;

                long readEvents = socket.readEvents.get();
                LongHolder prevReadEvents = getPrev(socket.readEvents);
                long readEventsDelta = readEvents - prevReadEvents.value;
                log(socket + " " + thp(readEventsDelta, elapsed) + " read-events/second");
                prevReadEvents.value = readEvents;

                log(socket + " " + (packetsReadDelta * 1.0f / (handleOutboundCallsDelta + 1)) + " packets/handleOutbound-call");
                log(socket + " " + (packetsReadDelta * 1.0f / (readEventsDelta + 1)) + " packets/read-events");
                log(socket + " " + (bytesReadDelta * 1.0f / (readEventsDelta + 1)) + " bytes-read/read-events");
            }
            prevPacketsRead.value = packetsRead;

            if (packetsReadDelta == 0 || true) {
                if (socket instanceof NioAsyncSocket) {
                    NioAsyncSocket c = (NioAsyncSocket) socket;
                    boolean hasData = !c.unflushedBufs.isEmpty() || !c.ioVector.isEmpty();
                    //if (nioChannel.flushThread.get() == null && hasData) {
                    log(socket + " is stuck: unflushed-iobuffers:" + c.unflushedBufs.size()
                            + " ioVector.empty:" + c.ioVector.isEmpty()
                            + " flushed:" + c.flushThread.get()
                            + " eventloop.contains:" + c.eventloop().concurrentRunQueue.contains(c));
                    //}
                } else if (socket instanceof IOUringAsyncSocket) {
                    IOUringAsyncSocket c = (IOUringAsyncSocket) socket;
                    boolean hasData = !c.unflushedBufs.isEmpty() || !c.ioVector.isEmpty();
                    //if (c.flushThread.get() == null && hasData) {
                    log(socket + " is stuck: unflushed-isbuffers:" + c.unflushedBufs.size()
                            + " ioVector.empty:" + c.ioVector.isEmpty()
                            + " flushed:" + c.flushThread.get()
                            + " eventloop.contains:" + c.eventloop().concurrentRunQueue.contains(c));
                    //}
                }
            }
        }

        private LongHolder getPrev(SwCounter counter) {
            LongHolder prev = prevMap.get(counter);
            if (prev == null) {
                prev = new LongHolder();
                prevMap.put(counter, prev);
            }
            return prev;
        }

        private static float thp(long delta, long elapsed) {
            return (delta * 1000f) / elapsed;
        }

        private void shutdown() {
            shutdown = true;
            interrupt();
        }
    }
}
