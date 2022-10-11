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

package com.hazelcast.internal.alto.apps;


import com.hazelcast.internal.util.ThreadAffinity;
import com.hazelcast.internal.tpc.iobuffer.IOBuffer;
import com.hazelcast.internal.tpc.iobuffer.IOBufferAllocator;
import com.hazelcast.internal.tpc.iobuffer.NonConcurrentIOBufferAllocator;
import com.hazelcast.internal.tpc.nio.NioAsyncServerSocket;
import com.hazelcast.internal.tpc.nio.NioAsyncSocket;
import com.hazelcast.internal.tpc.nio.NioEventloop;
import com.hazelcast.internal.tpc.nio.NioAsyncReadHandler;
import com.hazelcast.internal.alto.FrameCodec;
import org.jetbrains.annotations.NotNull;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.internal.nio.Bits.BYTES_INT;
import static com.hazelcast.internal.nio.Bits.BYTES_LONG;

/**
 * A very trivial benchmark to measure the throughput we can get with a RPC system.
 * <p>
 * JMH would be better; but for now this will give some insights.
 */
public class RpcBenchmark {

    public static int serverCpu = -1;
    public static int clientCpu = -1;
    public static boolean spin = true;
    public static int requestTotal = 100 * 1000 * 1000;
    public static int concurrency = 2000;

    public static void main(String[] args) throws InterruptedException {
        SocketAddress serverAddress = new InetSocketAddress("127.0.0.1", 5000);

        NioAsyncServerSocket socket = newServer(serverAddress);

        CountDownLatch latch = new CountDownLatch(concurrency);

        NioAsyncSocket clientSocket = newClient(serverAddress, latch);

        System.out.println("Starting");

        long startMs = System.currentTimeMillis();

        for (int k = 0; k < concurrency; k++) {
            IOBuffer buf = new IOBuffer(128);
            buf.writeInt(-1);
            buf.writeLong(requestTotal / concurrency);
            FrameCodec.constructComplete(buf);
            clientSocket.write(buf);
        }
        clientSocket.flush();

        latch.await();

        long duration = System.currentTimeMillis() - startMs;
        float throughput = requestTotal * 1000f / duration;
        System.out.println("Throughput:" + throughput);
        System.exit(0);
    }

    @NotNull
    private static NioAsyncSocket newClient(SocketAddress serverAddress, CountDownLatch latch) {
        NioEventloop.NioConfiguration config = new NioEventloop.NioConfiguration();
        if (clientCpu >= 0) {
            config.setThreadAffinity(new ThreadAffinity("" + clientCpu));
        }
        config.setSpin(spin);
        NioEventloop clientEventLoop = new NioEventloop(config);
        clientEventLoop.start();

        NioAsyncSocket clientSocket = NioAsyncSocket.open();
        clientSocket.tcpNoDelay(true);
        clientSocket.readHandler(new NioAsyncReadHandler() {
            private final IOBufferAllocator responseAllocator = new NonConcurrentIOBufferAllocator(8, true);

            @Override
            public void onRead(ByteBuffer buffer) {
                for (; ; ) {
                    if (buffer.remaining() < BYTES_INT + BYTES_LONG) {
                        return;
                    }

                    int size = buffer.getInt();
                    long l = buffer.getLong();
                    if (l == 0) {
                        latch.countDown();
                    } else {
                        IOBuffer buf = responseAllocator.allocate(8);
                        buf.writeInt(-1);
                        buf.writeLong(l);
                        FrameCodec.constructComplete(buf);
                        socket.unsafeWriteAndFlush(buf);
                    }
                }
            }
        });
        clientSocket.activate(clientEventLoop);
        clientSocket.connect(serverAddress).join();
        return clientSocket;
    }

    private static NioAsyncServerSocket newServer(SocketAddress serverAddress) {
        NioEventloop.NioConfiguration config = new NioEventloop.NioConfiguration();
        config.setSpin(spin);
        if (serverCpu >= 0) {
            config.setThreadAffinity(new ThreadAffinity("" + serverCpu));
        }
        NioEventloop serverEventloop = new NioEventloop(config);
        serverEventloop.start();

        NioAsyncServerSocket serverSocket = NioAsyncServerSocket.open(serverEventloop);
        serverSocket.bind(serverAddress);
        serverSocket.listen(10);
        serverSocket.accept(socket -> {
            socket.tcpNoDelay(true);
            socket.readHandler(new NioAsyncReadHandler() {
                private final IOBufferAllocator responseAllocator = new NonConcurrentIOBufferAllocator(8, true);

                @Override
                public void onRead(ByteBuffer buffer) {
                    for (; ; ) {
                        if (buffer.remaining() < BYTES_INT + BYTES_LONG) {
                            return;
                        }
                        int size = buffer.getInt();
                        long l = buffer.getLong();

                        IOBuffer buf = responseAllocator.allocate(8);
                        buf.writeInt(-1);
                        buf.writeLong(l - 1);
                        FrameCodec.constructComplete(buf);
                        socket.unsafeWriteAndFlush(buf);
                    }
                }
            });
            socket.activate(serverEventloop);
        });

        return serverSocket;
    }
}
