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

package com.hazelcast.internal.tpc.epoll;

import com.hazelcast.internal.tpc.AsyncFile;
import com.hazelcast.internal.tpc.Eventloop;
import io.netty.channel.epoll.EpollEventArray;
import io.netty.channel.epoll.Native;
import io.netty.channel.unix.FileDescriptor;
import io.netty.util.collection.IntObjectHashMap;
import io.netty.util.collection.IntObjectMap;

import java.io.IOException;

import static com.hazelcast.internal.tpc.Eventloop.State.RUNNING;
import static io.netty.channel.epoll.Native.EPOLLIN;
import static io.netty.channel.epoll.Native.epollCtlAdd;

public final class EpollEventloop extends Eventloop {
    private final IntObjectMap channels = new IntObjectHashMap<>(4096);
    final FileDescriptor epollFd;
    private final FileDescriptor eventFd;
    private final FileDescriptor timerFd;
    private final EpollEventArray events;

    public EpollEventloop() {
        this(new EpollConfiguration());
    }

    public EpollEventloop(EpollConfiguration config) {
        super(config, Type.EPOLL);
        this.events = new EpollEventArray(4096);
        this.epollFd = Native.newEpollCreate();
        this.eventFd = Native.newEventFd();
        try {
            // It is important to use EPOLLET here as we only want to get the notification once per
            // wakeup and don't call eventfd_read(...).
            epollCtlAdd(epollFd.intValue(), eventFd.intValue(), EPOLLIN | Native.EPOLLET);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to add eventFd filedescriptor to epoll", e);
        }
        this.timerFd = Native.newTimerFd();
        try {
            // It is important to use EPOLLET here as we only want to get the notification once per
            // wakeup and don't call read(...).
            epollCtlAdd(epollFd.intValue(), timerFd.intValue(), EPOLLIN | Native.EPOLLET);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to add timerFd filedescriptor to epoll", e);
        }
    }

    @Override
    protected Unsafe createUnsafe() {
        return new EpollUnsafe();
    }

    @Override
    public void wakeup() {
        if (spin || Thread.currentThread() == eventloopThread) {
            return;
        }

        if (wakeupNeeded.get() && wakeupNeeded.compareAndSet(true, false)) {
            Native.eventFdWrite(eventFd.intValue(), 1L);
        }
    }

    @Override
    protected void eventLoop() throws Exception {
        int k = 0;
        while (state == RUNNING) {
            runConcurrentTasks();
            k++;

            System.out.println(eventloopThread.getName() + " eventLoop run " + k);

            boolean moreWork = scheduler.tick();

            runLocalTasks();

            int ready;
            if (spin || moreWork) {
                System.out.println("epollBusyWait");
                ready = epollBusyWait();
            } else {
                wakeupNeeded.set(true);
                if (concurrentRunQueue.isEmpty()) {
                    System.out.println("epollWait");
                    ready = epollWait();
                } else {
                    System.out.println("epollBusyWait");
                    ready = epollBusyWait();
                }
                wakeupNeeded.set(false);
            }

            if (ready > 0) {
                processReady(ready);
            }
        }
    }

    private int epollWait() throws IOException {
        return Native.epollWait(epollFd, events, false);
    }

    private int epollBusyWait() throws IOException {
        return Native.epollBusyWait(epollFd, events);
    }

    private void processReady(int ready) {
        System.out.println("handleReadyEvents: " + ready + " ready");

        for (int i = 0; i < ready; i++) {
            final int fd = events.fd(i);
            if (fd == eventFd.intValue()) {
                System.out.println("eventFd");
                //pendingWakeup = false;
            } else if (fd == timerFd.intValue()) {
                System.out.println("timerFd");
                //timerFired = true;
            } else {
                System.out.println("Something else");
                final long ev = events.events(i);

                Object channel = channels.get(fd);
                if (channel != null) {
                    System.out.println("channel found");
                    if ((ev & (Native.EPOLLERR | Native.EPOLLOUT)) != 0) {
                        System.out.println("epollout");
                        //((EpollChannel)channel).handleWrite();
                    }

                    if ((ev & (Native.EPOLLERR | EPOLLIN)) != 0) {
                        System.out.println("epoll in");
                        if (channel instanceof EpollAsyncServerSocket) {
                            System.out.println("EpollServerChannel.handleAccept");
                            ((EpollAsyncServerSocket) channel).handleAccept();
                        } else {
                            System.out.println("EpollChannel.handleRead");
                            //  ((EpollChannel)channel).handleRead();
                        }
                    }
                } else {
                    System.out.println("no channel found");
                    // no channel found
                    // We received an event for an fd which we not use anymore. Remove it from the epoll_event set.
                    try {
                        Native.epollCtlDel(epollFd.intValue(), fd);
                    } catch (IOException ignore) {
                    }
                }
            }
        }
    }

    private class EpollUnsafe extends Unsafe {
        @Override
        public AsyncFile newAsyncFile(String path) {
            throw new RuntimeException("Not implemented");
        }
    }

    /**
     * Contains the configuration for the {@link EpollEventloop}.
     */
    public static class EpollConfiguration extends Configuration {
    }
}
