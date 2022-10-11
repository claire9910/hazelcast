package com.hazelcast.alto.engine.nio;

import com.hazelcast.internal.tpc.nio.NioAsyncReadHandler;
import com.hazelcast.internal.tpc.nio.NioAsyncServerSocket;
import com.hazelcast.internal.tpc.nio.NioEventloop;
import com.hazelcast.internal.tpc.nio.NioSyncReadHandler;
import com.hazelcast.internal.tpc.nio.NioSyncSocket;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.internal.tpc.iobuffer.IOBuffer;
import com.hazelcast.internal.tpc.iobuffer.IOBufferAllocator;
import com.hazelcast.internal.tpc.iobuffer.NonConcurrentIOBufferAllocator;
import com.hazelcast.internal.alto.FrameCodec;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;

import static com.hazelcast.internal.nio.Bits.BYTES_INT;
import static com.hazelcast.internal.nio.IOUtil.closeResource;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@Category({QuickTest.class})
public class NioSyncSocket_IntegrationTest {

    private NioEventloop eventloop;
    private NioAsyncServerSocket serverSocket;
    private NioSyncSocket clientSocket;

    @Before
    public void setup() {
        eventloop = new NioEventloop();
        eventloop.start();
    }

    public void after() throws InterruptedException {
        if (clientSocket != null) {
            closeResource(clientSocket);
        }

        if (serverSocket != null) {
            closeResource(serverSocket);
        }

        if (eventloop != null) {
            eventloop.shutdown();
            assertTrue(eventloop.awaitTermination(5, SECONDS));
        }
    }

    @Test
    public void test() {
        SocketAddress serverAddress = new InetSocketAddress("127.0.0.1", 6000);

        createServer(serverAddress);

        createClient(serverAddress);

        for (int k = 0; k < 1000; k++) {
            System.out.println("at: "+k);
            IOBuffer request = new IOBuffer(128, true);
            request.writeInt(-1);
            FrameCodec.constructComplete(request);
            clientSocket.writeAndFlush(request);

            IOBuffer response = clientSocket.read();
            assertNotNull(response);
        }
    }

    private void createClient(SocketAddress serverAddress) {
        clientSocket = NioSyncSocket.open();
        clientSocket.tcpNoDelay(true);
        clientSocket.readHandler(new NioSyncReadHandler() {
            private final IOBufferAllocator responseAllocator = new NonConcurrentIOBufferAllocator(8, true);

            @Override
            public IOBuffer decode(ByteBuffer buffer) {
                if (buffer.remaining() < BYTES_INT) {
                    return null;
                }

                int size = buffer.getInt();
                IOBuffer buf = responseAllocator.allocate(8);
                buf.writeInt(-1);
                FrameCodec.constructComplete(buf);
                return buf;
            }
        });
        clientSocket.connect(serverAddress);
    }

    private void createServer(SocketAddress serverAddress) {
        serverSocket = NioAsyncServerSocket.open(eventloop);
        serverSocket.reuseAddress(true);
        serverSocket.bind(serverAddress);
        serverSocket.listen(10);
        serverSocket.accept(socket -> {
            socket.tcpNoDelay(true);
            socket.readHandler(new NioAsyncReadHandler() {
                private final IOBufferAllocator responseAllocator = new NonConcurrentIOBufferAllocator(8, true);

                @Override
                public void onRead(ByteBuffer buffer) {
                    for (; ; ) {
                        if (buffer.remaining() < BYTES_INT) {
                            return;
                        }
                        int size = buffer.getInt();

                        IOBuffer buf = responseAllocator.allocate(8);
                        buf.writeInt(-1);
                        buf.flip();
                        socket.unsafeWriteAndFlush(buf);
                    }
                }
            });
            socket.activate(eventloop);
        });
    }
}
