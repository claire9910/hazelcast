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

package com.hazelcast.internal.tpc.iouring;

import com.hazelcast.internal.tpc.AsyncFile;
import com.hazelcast.internal.tpc.Eventloop;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.unix.FileDescriptor;
import io.netty.incubator.channel.uring.IOUringCompletionQueue;
import io.netty.incubator.channel.uring.IOUringCompletionQueueCallback;
import io.netty.incubator.channel.uring.IOUringSubmissionQueue;
import io.netty.incubator.channel.uring.Native;
import io.netty.incubator.channel.uring.RingBuffer;
import io.netty.util.collection.IntObjectHashMap;
import io.netty.util.collection.IntObjectMap;
import io.netty.util.internal.PlatformDependent;

import static com.hazelcast.internal.util.Preconditions.checkNotNegative;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkPositive;
import static com.hazelcast.internal.alto.util.Util.epochNanos;
import static io.netty.incubator.channel.uring.Native.DEFAULT_IOSEQ_ASYNC_THRESHOLD;
import static io.netty.incubator.channel.uring.Native.DEFAULT_RING_SIZE;
import static io.netty.incubator.channel.uring.Native.IORING_OP_TIMEOUT;


/**
 * To build io uring:
 * <p>
 * sudo yum install autoconf
 * sudo yum install automake
 * sudo yum install libtool
 * <p>
 * Good read:
 * https://unixism.net/2020/04/io-uring-by-example-part-3-a-web-server-with-io-uring/
 * <p>
 * Another example (blocking socket)
 * https://github.com/ddeka0/AsyncIO/blob/master/src/asyncServer.cpp
 * <p>
 * no syscalls:
 * https://wjwh.eu/posts/2021-10-01-no-syscall-server-iouring.html
 * <p>
 * Error codes:
 * https://www.thegeekstuff.com/2010/10/linux-error-codes/
 * <p>
 * <p>
 * https://github.com/torvalds/linux/blob/master/include/uapi/linux/io_uring.h
 * IORING_OP_NOP               0
 * IORING_OP_READV             1
 * IORING_OP_WRITEV            2
 * IORING_OP_FSYNC             3
 * IORING_OP_READ_FIXED        4
 * IORING_OP_WRITE_FIXED       5
 * IORING_OP_POLL_ADD          6
 * IORING_OP_POLL_REMOVE       7
 * IORING_OP_SYNC_FILE_RANGE   8
 * IORING_OP_SENDMSG           9
 * IORING_OP_RECVMSG           10
 * IORING_OP_TIMEOUT,          11
 * IORING_OP_TIMEOUT_REMOVE,   12
 * IORING_OP_ACCEPT,           13
 * IORING_OP_ASYNC_CANCEL,     14
 * IORING_OP_LINK_TIMEOUT,     15
 * IORING_OP_CONNECT,          16
 * IORING_OP_FALLOCATE,        17
 * IORING_OP_OPENAT,
 * IORING_OP_CLOSE,
 * IORING_OP_FILES_UPDATE,
 * IORING_OP_STATX,
 * IORING_OP_READ,
 * IORING_OP_WRITE,
 * IORING_OP_FADVISE,
 * IORING_OP_MADVISE,
 * IORING_OP_SEND,
 * IORING_OP_RECV,
 * IORING_OP_OPENAT2,
 * IORING_OP_EPOLL_CTL,
 * IORING_OP_SPLICE,
 * IORING_OP_PROVIDE_BUFFERS,
 * IORING_OP_REMOVE_BUFFERS,
 * IORING_OP_TEE,
 * IORING_OP_SHUTDOWN,
 * IORING_OP_RENAMEAT,
 * IORING_OP_UNLINKAT,
 * IORING_OP_MKDIRAT,
 * IORING_OP_SYMLINKAT,
 * IORING_OP_LINKAT,
 * IORING_OP_MSG_RING,
 */
public class IOUringEventloop extends Eventloop {

    public static int IORING_SETUP_IOPOLL = 1;
    public static int IORING_SETUP_SQPOLL = 2;

    private final RingBuffer ringBuffer;
    private final FileDescriptor eventfd = Native.newBlockingEventFd();
    private final IOUringConfiguration config;
    IOUringSubmissionQueue sq;
    private final IOUringCompletionQueue cq;
    private final long eventfdReadBuf = PlatformDependent.allocateMemory(8);
    final IntObjectMap<CompletionListener> completionListeners = new IntObjectHashMap<>(4096);
    final UnpooledByteBufAllocator allocator = new UnpooledByteBufAllocator(true);
    protected final PooledByteBufAllocator iovArrayBufferAllocator = new PooledByteBufAllocator();
    protected final IORequestScheduler ioRequestScheduler;
    private final EventloopHandler eventLoopHandler = new EventloopHandler();

    public IOUringEventloop() {
        this(new IOUringConfiguration());
    }

    public IOUringEventloop(IOUringConfiguration config) {
        super(config, Type.IOURING);
        this.config = config;
        this.ringBuffer = Native.createRingBuffer(
                config.ringbufferSize,
                config.ioseqAsyncThreshold,
                config.flags);
        this.sq = ringBuffer.ioUringSubmissionQueue();
        this.cq = ringBuffer.ioUringCompletionQueue();
        this.completionListeners.put(eventfd.intValue(), (fd, op, res, flags, data) -> sq_addEventRead());
        this.ioRequestScheduler = config.ioRequestScheduler;
    }

    @Override
    protected Unsafe createUnsafe() {
        return new IOUringUnsafe();
    }

    @Override
    protected void beforeEventloop() {
        ioRequestScheduler.init(this);
    }

    @Override
    public void wakeup() {
        if (spin || Thread.currentThread() == eventloopThread) {
            return;
        }

        if (wakeupNeeded.get() && wakeupNeeded.compareAndSet(true, false)) {
            Native.eventFdWrite(eventfd.intValue(), 1L);
        }
    }

    @Override
    protected void eventLoop() {
        if ((config.flags & IORING_SETUP_IOPOLL) != 0) {
            throw new UnsupportedOperationException();

//            // see https://kernel.dk/io_uring.pdf 8.2
//            sq_addEventRead();
//
//            do {
//                sq.submit(Native.IORING_ENTER_GETEVENTS);
//                cq.process(eventLoopHandler);
//                runConcurrentTasks();
//                scheduler.tick();
//                runLocalTasks();
//            } while (state == RUNNING);
        } else {
            sq_addEventRead();

            boolean moreWork = false;
            do {
                if (cq.hasCompletions()) {
                    cq.process(eventLoopHandler);
                } else if (spin || moreWork) {
                    sq.submit();
                } else {
                    wakeupNeeded.set(true);
                    if (concurrentRunQueue.isEmpty()) {
                        if (earliestDeadlineEpochNanos != -1) {
                            long timeoutNanos = earliestDeadlineEpochNanos - epochNanos();
                            if (timeoutNanos > 0) {
                                sq.addTimeout(timeoutNanos, (short) 0);
                                sq.submitAndWait();
                            } else {
                                sq.submit();
                            }
                        } else {
                            sq.submitAndWait();
                        }
                    } else {
                        sq.submit();
                    }
                    wakeupNeeded.set(false);
                }

                runConcurrentTasks();
                moreWork = scheduler.tick();
                runLocalTasks();
            } while (state == State.RUNNING);
        }
    }

    private void sq_addEventRead() {
        sq.addEventFdRead(eventfd.intValue(), eventfdReadBuf, 0, 8, (short) 0);
    }

    private class EventloopHandler implements IOUringCompletionQueueCallback {
        @Override
        public void handle(int fd, int res, int flags, byte op, short data) {
            CompletionListener l = completionListeners.get(fd);
            if (l == null) {
                if (op != IORING_OP_TIMEOUT) {
                    System.out.println("no listener found for fd:" + fd + " op:" + op);
                }
            } else {
                l.handle(fd, res, flags, op, data);
            }
        }
    }

    protected class IOUringUnsafe extends Unsafe {
        @Override
        public AsyncFile newAsyncFile(String path) {
            return new IOUringAsyncFile(path, IOUringEventloop.this);
        }
    }

    /**
     * Contains the configuration for the {@link IOUringEventloop}.
     */
    public static class IOUringConfiguration extends Configuration {
        private int flags;
        private int ringbufferSize = DEFAULT_RING_SIZE;
        private int ioseqAsyncThreshold = DEFAULT_IOSEQ_ASYNC_THRESHOLD;
        private IORequestScheduler ioRequestScheduler = new IORequestScheduler(512);

        public void setFlags(int flags) {
            this.flags = checkNotNegative(flags, "flags can't be negative");
        }

        public void setRingbufferSize(int ringbufferSize) {
            this.ringbufferSize = checkPositive("ringbufferSize", ringbufferSize);
        }

        public void setIoseqAsyncThreshold(int ioseqAsyncThreshold) {
            this.ioseqAsyncThreshold = checkPositive("ioseqAsyncThreshold", ioseqAsyncThreshold);
        }

        public void setIoRequestScheduler(IORequestScheduler ioRequestScheduler) {
            this.ioRequestScheduler = checkNotNull(ioRequestScheduler);
        }
    }
}

