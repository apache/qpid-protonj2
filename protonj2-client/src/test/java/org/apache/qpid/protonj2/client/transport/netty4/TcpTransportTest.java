/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.qpid.protonj2.client.transport.netty4;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferAllocator;
import org.apache.qpid.protonj2.buffer.netty.Netty4ProtonBufferAllocator;
import org.apache.qpid.protonj2.client.SslOptions;
import org.apache.qpid.protonj2.client.TransportOptions;
import org.apache.qpid.protonj2.client.test.ImperativeClientTestCase;
import org.apache.qpid.protonj2.client.test.Wait;
import org.apache.qpid.protonj2.client.transport.IOContext;
import org.apache.qpid.protonj2.client.transport.Transport;
import org.apache.qpid.protonj2.client.transport.TransportListener;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.kqueue.KQueue;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.incubator.channel.uring.IOUring;
import io.netty.incubator.channel.uring.IOUringEventLoopGroup;
import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakDetector.Level;

/**
 * Test basic functionality of the Netty based TCP transport.
 */
@Timeout(30)
public class TcpTransportTest extends ImperativeClientTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(TcpTransportTest.class);

    protected static final int SEND_BYTE_COUNT = 1024;
    protected static final String HOSTNAME = "localhost";

    protected volatile boolean transportInitialized;
    protected volatile boolean transportConnected;
    protected volatile boolean transportErrored;
    protected final List<Throwable> exceptions = Collections.synchronizedList(new ArrayList<>());
    protected final List<ProtonBuffer> data = new ArrayList<>();
    protected final AtomicInteger bytesRead = new AtomicInteger();

    protected final TransportListener testListener = new NettyTransportListener();

    protected ProtonBufferAllocator allocator;
    protected Netty4IOContext context;

    @Override
    @BeforeEach
    public void setUp(TestInfo testInfo) throws Exception {
        super.setUp(testInfo);

        allocator = new Netty4ProtonBufferAllocator(UnpooledByteBufAllocator.DEFAULT);
    }

    @Override
    @AfterEach
    public void tearDown(TestInfo testInfo) throws Exception {
        super.tearDown(testInfo);

        if (context != null) {
            context.shutdown();
            context = null;
        }

        data.removeIf((buffer) -> {
            buffer.close();
            return true;
        });
    }

    @Test
    public void testCannotCreateWithIllegalArgs() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> new TcpTransport(null, createTransportOptions(), createSSLOptions()));
        assertThrows(IllegalArgumentException.class, () -> new TcpTransport(new Bootstrap(), null, createSSLOptions()));
        assertThrows(IllegalArgumentException.class, () -> new TcpTransport(new Bootstrap(), createTransportOptions(), null));
    }

    @Test
    public void testCloseOnNeverConnectedTransport() throws Exception {
        Transport transport = createTransport(createTransportOptions(), createSSLOptions());
        assertFalse(transport.isConnected());

        transport.close();

        assertFalse(transportInitialized);
        assertFalse(transportConnected);
        assertFalse(transportErrored);
        assertTrue(exceptions.isEmpty());
        assertTrue(data.isEmpty());
    }

    @Test
    public void testCannotCallConnectOnClosedTransport() throws Exception {
        Transport transport = createTransport(createTransportOptions(), createSSLOptions());

        transport.close();

        assertThrows(IllegalStateException.class, () -> transport.connect("localhost", 5672, testListener));
    }

    @Test
    public void testCreateWithBadHostOrPortThrowsIAE() throws Exception {
        Transport transport = createTransport(createTransportOptions().defaultTcpPort(-1), createSSLOptions().defaultSslPort(-1));

        try {
            transport.connect(HOSTNAME, -1, testListener);
            fail("Should have thrown IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
        }

        try {
            transport.connect(null, 5672, testListener);
            fail("Should have thrown IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
        }

        try {
            transport.connect("", 5672, testListener);
            fail("Should have thrown IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
        }
    }

    @Test
    public void testCreateWithNullOptionsThrowsIAE() throws Exception {
        try {
            new Netty4IOContext(null, null, getTestName());
            fail("Should have thrown NullPointerException");
        } catch (NullPointerException npe) {
        }

        try {
            new Netty4IOContext(createTransportOptions(), null, getTestName());
            fail("Should have thrown NullPointerException");
        } catch (NullPointerException npe) {
        }

        try {
            new Netty4IOContext(null, createSSLOptions(), getTestName());
            fail("Should have thrown NullPointerException");
        } catch (NullPointerException npe) {
        }
    }

    @Test
    public void testConnectWithoutRunningServer() throws Exception {
        try (NettyEchoServer server = createEchoServer()) {
            server.start();

            final int port = server.getServerPort();

            server.close();

            Transport transport = createTransport(createTransportOptions(), createSSLOptions());

            assertNull(transport.getTransportListener());

            try {
                transport.connect(HOSTNAME, port, testListener).awaitConnect();
                fail("Should have failed to connect to the server: " + HOSTNAME + ":" + port);
            } catch (Exception e) {
                LOG.info("Failed to connect to: {}:{} as expected.", HOSTNAME, port);
            }

            assertEquals(testListener, transport.getTransportListener());
            assertFalse(transport.isConnected());

            transport.close();
        }

        assertTrue(transportInitialized);
        assertFalse(transportConnected);
        assertTrue(transportErrored);
        assertFalse(exceptions.isEmpty());
        assertTrue(data.isEmpty());
    }

    @Test
    public void testConnectWithoutListenerFails() throws Exception {
        try (NettyEchoServer server = createEchoServer()) {
            server.start();

            final int port = server.getServerPort();

            Transport transport = createTransport(createTransportOptions(), createSSLOptions());

            try {
                transport.connect(HOSTNAME, port, null);
                fail("Should have failed to connect to the server: " + HOSTNAME + ":" + port);
            } catch (IllegalArgumentException e) {
                LOG.info("Failed to connect to: {}:{} as expected.", HOSTNAME, port);
            }

            assertFalse(transport.isConnected());

            transport.close();
        }
    }

    @Test
    public void testConnectToServer() throws Exception {
        try (NettyEchoServer server = createEchoServer()) {
            server.start();

            final int port = server.getServerPort();

            Transport transport = createTransport(createTransportOptions(), createSSLOptions());
            try {
                transport.connect(HOSTNAME, port, testListener).awaitConnect();
                LOG.info("Connected to server:{}:{} as expected.", HOSTNAME, port);
            } catch (Exception e) {
                fail("Should not have failed to connect to the server at " + HOSTNAME + ":" + port + " but got exception: " + e);
            }

            assertTrue(transport.isConnected());
            assertEquals(HOSTNAME, transport.getHost(), "Server host is incorrect");
            assertEquals(port, transport.getPort(), "Server port is incorrect");

            final URI remoteURI = transport.getRemoteURI();

            if (transport.isSecure()) {
                if (transport.getTransportOptions().useWebSockets()) {
                    assertEquals(remoteURI.getScheme(), "wss");
                } else {
                    assertEquals(remoteURI.getScheme(), "ssl");
                }
            } else {
                if (transport.getTransportOptions().useWebSockets()) {
                    assertEquals(remoteURI.getScheme(), "ws");
                } else {
                    assertEquals(remoteURI.getScheme(), "tcp");
                }
            }

            transport.close();

            // Additional close should not fail or cause other problems.
            transport.close();
        }

        assertTrue(transportInitialized);
        assertTrue(transportConnected);
        assertFalse(transportErrored);  // Normal shutdown does not trigger the event.
        assertTrue(exceptions.isEmpty());
        assertTrue(data.isEmpty());
    }

    @Test
    public void testMultipleConnectionsToServer() throws Exception {
        final int CONNECTION_COUNT = 10;

        try (NettyEchoServer server = createEchoServer()) {
            server.start();

            final int port = server.getServerPort();

            List<Transport> transports = new ArrayList<>();

            IOContext context = createContext(createTransportOptions(), createSSLOptions());

            for (int i = 0; i < CONNECTION_COUNT; ++i) {
                Transport transport = context.newTransport();
                try {
                    transport.connect(HOSTNAME, port, testListener).awaitConnect();
                    assertTrue(transport.isConnected());
                    LOG.info("Connected to server:{}:{} as expected.", HOSTNAME, port);
                    transports.add(transport);
                } catch (Exception e) {
                    fail("Should not have failed to connect to the server at " + HOSTNAME + ":" + port + " but got exception: " + e);
                }
            }

            for (Transport transport : transports) {
                transport.close();
            }
        }

        assertTrue(transportInitialized);
        assertFalse(transportErrored);  // Normal shutdown does not trigger the event.
        assertTrue(exceptions.isEmpty());
        assertTrue(data.isEmpty());
    }

    @Test
    public void testMultipleConnectionsSendReceive() throws Exception {
        final int CONNECTION_COUNT = 10;
        final int FRAME_SIZE = 8;

        ProtonBuffer sendBuffer = allocator.allocate(FRAME_SIZE);
        for (int i = 0; i < 8; ++i) {
            sendBuffer.writeByte((byte) 'A');
        }

        try (NettyEchoServer server = createEchoServer()) {
            server.start();

            final int port = server.getServerPort();

            List<Transport> transports = new ArrayList<>();

            IOContext context = createContext(createTransportOptions(), createSSLOptions());

            for (int i = 0; i < CONNECTION_COUNT; ++i) {
                Transport transport = context.newTransport();
                try {
                    transport.connect(HOSTNAME, port, testListener).awaitConnect();
                    transport.writeAndFlush(sendBuffer.copy());
                    transports.add(transport);
                } catch (Exception e) {
                    fail("Should not have failed to connect to the server at " + HOSTNAME + ":" + port + " but got exception: " + e);
                }
            }

            assertTrue(Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisfied() throws Exception {
                    LOG.debug("Checking completion: read {} expecting {}", bytesRead.get(), (FRAME_SIZE * CONNECTION_COUNT));
                    return bytesRead.get() == (FRAME_SIZE * CONNECTION_COUNT);
                }
            }, 10000, 50));

            for (Transport transport : transports) {
                transport.close();
            }
        }

        assertTrue(exceptions.isEmpty());
    }

    @Test
    public void testDetectServerClose() throws Exception {
        Transport transport = null;

        try (NettyEchoServer server = createEchoServer()) {
            server.start();

            int port = server.getServerPort();

            transport = createTransport(createTransportOptions(), createSSLOptions());
            try {
                transport.connect(HOSTNAME, port, testListener).awaitConnect();
                LOG.info("Connected to server:{}:{} as expected.", HOSTNAME, port);
            } catch (Exception e) {
                fail("Should not have failed to connect to the server at " + HOSTNAME + ":" + port + " but got exception: " + e);
            }

            assertTrue(transport.isConnected());
            assertSame(testListener, transport.getTransportListener());
            assertEquals(HOSTNAME, transport.getHost());

            assertTrue(Wait.waitFor(() -> server.getTotalConnections() == 1));
        }

        final Transport connectedTransport = transport;
        assertTrue(Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisfied() throws Exception {
                return !connectedTransport.isConnected();
            }
        }, 10000, 50));

        assertTrue(data.isEmpty());

        try {
            transport.close();
        } catch (Exception ex) {
            fail("Close of a disconnect transport should not generate errors");
        }
    }

    @Test
    public void testZeroSizedSentNoErrorsWriteAndFlush() throws Exception {
        testZeroSizedSentNoErrors(true);
    }

    @Test
    public void testZeroSizedSentNoErrorsWriteThenFlush() throws Exception {
        testZeroSizedSentNoErrors(false);
    }

    private void testZeroSizedSentNoErrors(boolean writeAndFlush) throws Exception {
        try (NettyEchoServer server = createEchoServer()) {
            server.start();

            int port = server.getServerPort();

            Transport transport = createTransport(createTransportOptions(), createSSLOptions());
            try {
                transport.connect(HOSTNAME, port, testListener).awaitConnect();
                LOG.info("Connected to server:{}:{} as expected.", HOSTNAME, port);
            } catch (Exception e) {
                fail("Should not have failed to connect to the server at " + HOSTNAME + ":" + port + " but got exception: " + e);
            }

            assertTrue(transport.isConnected());

            final ProtonBuffer sendBuffer = allocator.allocate(0);

            if (writeAndFlush) {
                transport.writeAndFlush(sendBuffer);
            } else {
                transport.write(sendBuffer);
                transport.flush();
            }

            transport.close();
        }

        assertTrue(!transportErrored);  // Normal shutdown does not trigger the event.
        assertTrue(exceptions.isEmpty());
        assertTrue(data.isEmpty());
    }

    @Test
    public void testUseAllocatorToCreateFixedSizeOutputBuffer() throws Exception {
        try (NettyEchoServer server = createEchoServer()) {
            server.start();

            int port = server.getServerPort();

            Transport transport = createTransport(createTransportOptions(), createSSLOptions());
            try {
                transport.connect(HOSTNAME, port, testListener).awaitConnect();
                LOG.info("Connected to server:{}:{} as expected.", HOSTNAME, port);
            } catch (Exception e) {
                fail("Should not have failed to connect to the server at " + HOSTNAME + ":" + port + " but got exception: " + e);
            }

            assertTrue(transport.isConnected());

            ProtonBuffer buffer = transport.getBufferAllocator().allocate(64).implicitGrowthLimit(512);

            assertEquals(64, buffer.capacity());
            assertEquals(512, buffer.implicitGrowthLimit());

            transport.writeAndFlush(buffer);

            transport.close();
        }

        assertTrue(!transportErrored);  // Normal shutdown does not trigger the event.
        assertTrue(exceptions.isEmpty());
        assertTrue(data.isEmpty());
    }

    @Test
    public void testUseAllocatorToCreateFixedSizeHeapBuffer() throws Exception {
        try (NettyEchoServer server = createEchoServer()) {
            server.start();

            int port = server.getServerPort();

            Transport transport = createTransport(createTransportOptions(), createSSLOptions());
            try {
                transport.connect(HOSTNAME, port, testListener).awaitConnect();
                LOG.info("Connected to server:{}:{} as expected.", HOSTNAME, port);
            } catch (Exception e) {
                fail("Should not have failed to connect to the server at " + HOSTNAME + ":" + port + " but got exception: " + e);
            }

            assertTrue(transport.isConnected());

            ProtonBuffer buffer = transport.getBufferAllocator().allocate(64).implicitGrowthLimit(512);

            assertEquals(64, buffer.capacity());
            assertEquals(512, buffer.implicitGrowthLimit());

            transport.writeAndFlush(buffer);

            transport.close();
        }

        assertTrue(!transportErrored);  // Normal shutdown does not trigger the event.
        assertTrue(exceptions.isEmpty());
        assertTrue(data.isEmpty());
    }

    @Test
    public void testDataSentWithWriteAndThenFlushedIsReceived() throws Exception {
        try (NettyEchoServer server = createEchoServer()) {
            server.start();

            int port = server.getServerPort();

            Transport transport = createTransport(createTransportOptions(), createSSLOptions());
            try {
                transport.connect(HOSTNAME, port, testListener).awaitConnect();
                LOG.info("Connected to server:{}:{} as expected.", HOSTNAME, port);
            } catch (Exception e) {
                fail("Should not have failed to connect to the server at " + HOSTNAME + ":" + port + " but got exception: " + e);
            }

            assertTrue(transport.isConnected());

            ProtonBuffer sendBuffer = transport.getBufferAllocator().outputBuffer(SEND_BYTE_COUNT);
            for (int i = 0; i < SEND_BYTE_COUNT; ++i) {
                sendBuffer.writeByte((byte) 'A');
            }

            transport.write(sendBuffer, () -> LOG.debug("Netty reports write complete"));
            LOG.trace("Flush of Transport happens now");
            transport.flush();

            assertTrue(Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisfied() throws Exception {
                    return !data.isEmpty();
                }
            }, 10000, 50));

            assertEquals(SEND_BYTE_COUNT, data.get(0).getReadableBytes());

            transport.close();
        }

        assertTrue(!transportErrored);  // Normal shutdown does not trigger the event.
        assertTrue(exceptions.isEmpty());
    }

    @Test
    public void testDataSentWithinCompositeBufferIsReceived() throws Exception {
        try (NettyEchoServer server = createEchoServer()) {
            server.start();

            int port = server.getServerPort();

            Transport transport = createTransport(createTransportOptions(), createSSLOptions());
            try {
                transport.connect(HOSTNAME, port, testListener).awaitConnect();
                LOG.info("Connected to server:{}:{} as expected.", HOSTNAME, port);
            } catch (Exception e) {
                fail("Should not have failed to connect to the server at " + HOSTNAME + ":" + port + " but got exception: " + e);
            }

            assertTrue(transport.isConnected());

            ProtonBuffer sendBuffer1 = transport.getBufferAllocator().outputBuffer(SEND_BYTE_COUNT);
            for (int i = 0; i < SEND_BYTE_COUNT; ++i) {
                sendBuffer1.writeByte((byte) 'A');
            }
            ProtonBuffer sendBuffer2 = transport.getBufferAllocator().outputBuffer(SEND_BYTE_COUNT + 10);
            for (int i = 0; i < SEND_BYTE_COUNT + 10; ++i) {
                sendBuffer2.writeByte((byte) 'A');
            }

            ProtonBuffer sendBuffer = transport.getBufferAllocator().composite(new ProtonBuffer[] { sendBuffer1, sendBuffer2});

            transport.write(sendBuffer, () -> LOG.debug("Netty reports write complete"));
            LOG.trace("Flush of Transport happens now");
            transport.flush();

            sendBuffer.close();

            assertTrue(Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisfied() throws Exception {
                    return bytesRead.get() == SEND_BYTE_COUNT + SEND_BYTE_COUNT + 10;
                }
            }, 10000, 50));

            transport.close();
        }

        assertTrue(!transportErrored);  // Normal shutdown does not trigger the event.
        assertTrue(exceptions.isEmpty());
    }

    @Test
    public void testDataSentWithWriteAndFlushIsReceived() throws Exception {
        try (NettyEchoServer server = createEchoServer()) {
            server.start();

            int port = server.getServerPort();

            Transport transport = createTransport(createTransportOptions(), createSSLOptions());
            try {
                transport.connect(HOSTNAME, port, testListener).awaitConnect();
                LOG.info("Connected to server:{}:{} as expected.", HOSTNAME, port);
            } catch (Exception e) {
                fail("Should not have failed to connect to the server at " + HOSTNAME + ":" + port + " but got exception: " + e);
            }

            assertTrue(transport.isConnected());

            ProtonBuffer sendBuffer = transport.getBufferAllocator().outputBuffer(SEND_BYTE_COUNT);
            for (int i = 0; i < SEND_BYTE_COUNT; ++i) {
                sendBuffer.writeByte((byte) 'A');
            }

            transport.writeAndFlush(sendBuffer, () -> LOG.debug("Netty reports write complete"));

            assertTrue(Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisfied() throws Exception {
                    return !data.isEmpty();
                }
            }, 10000, 50));

            assertEquals(SEND_BYTE_COUNT, data.get(0).getReadableBytes());

            transport.close();
        }

        assertTrue(!transportErrored);  // Normal shutdown does not trigger the event.
        assertTrue(exceptions.isEmpty());
    }

    @Test
    public void testMultipleDataPacketsSentAreReceived() throws Exception {
        doMultipleDataPacketsSentAndReceive(SEND_BYTE_COUNT, 1);
    }

    @Test
    public void testMultipleDataPacketsSentAreReceivedRepeatedly() throws Exception {
        doMultipleDataPacketsSentAndReceive(SEND_BYTE_COUNT, 10);
    }

    public void doMultipleDataPacketsSentAndReceive(final int byteCount, final int iterations) throws Exception {
        try (NettyEchoServer server = createEchoServer()) {
            server.start();

            int port = server.getServerPort();

            Transport transport = createTransport(createTransportOptions(), createSSLOptions());
            try {
                transport.connect(HOSTNAME, port, testListener).awaitConnect();
                LOG.info("Connected to server:{}:{} as expected.", HOSTNAME, port);
            } catch (Exception e) {
                fail("Should not have failed to connect to the server at " + HOSTNAME + ":" + port + " but got exception: " + e);
            }

            assertTrue(transport.isConnected());

            final ProtonBuffer sendBuffer = allocator.allocate(byteCount);

            for (int i = 0; i < byteCount; ++i) {
                sendBuffer.writeByte((byte) 'A');
            }

            for (int i = 0; i < iterations; ++i) {
                transport.writeAndFlush(sendBuffer.copy(true));
            }

            assertTrue(Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisfied() throws Exception {
                    return bytesRead.get() == (byteCount * iterations);
                }
            }, 10000, 50));

            transport.close();
        }

        assertTrue(!transportErrored);  // Normal shutdown does not trigger the event.
        assertTrue(exceptions.isEmpty());
    }

    @Test
    public void testSendToClosedTransportFails() throws Exception {
        Transport transport = null;

        try (NettyEchoServer server = createEchoServer()) {
            server.start();

            int port = server.getServerPort();

            transport = createTransport(createTransportOptions(), createSSLOptions());
            try {
                transport.connect(HOSTNAME, port, testListener).awaitConnect();
                LOG.info("Connected to server:{}:{} as expected.", HOSTNAME, port);
            } catch (Exception e) {
                fail("Should not have failed to connect to the server at " + HOSTNAME + ":" + port + " but got exception: " + e);
            }

            assertTrue(transport.isConnected());

            transport.close();

            final ProtonBuffer sendBuffer = allocator.allocate(10);

            try {
                transport.writeAndFlush(sendBuffer);
                fail("Should throw on send of closed transport");
            } catch (IOException ex) {
            }
        }
    }

    @Test
    public void testConnectRunsInitializationMethod() throws Exception {
        try (NettyEchoServer server = createEchoServer()) {
            server.start();

            int port = server.getServerPort();
            final CountDownLatch initialized = new CountDownLatch(1);

            Transport transport = createTransport(createTransportOptions(), createSSLOptions());
            try {
                transport.connect(HOSTNAME, port, new NettyTransportListener() {

                    @Override
                    public void transportInitialized(Transport transport) {
                        initialized.countDown();
                        assertFalse(transport.isConnected());
                    }
                });
                LOG.info("Connected to server:{}:{} as expected.", HOSTNAME, port);
            } catch (Exception e) {
                fail("Should not have failed to connect to the server at " + HOSTNAME + ":" + port + " but got exception: " + e);
            }

            initialized.await();
            transport.awaitConnect();

            assertTrue(transport.isConnected());
            assertEquals(HOSTNAME, transport.getHost(), "Server host is incorrect");
            assertEquals(port, transport.getPort(), "Server port is incorrect");
            assertEquals(0, initialized.getCount());

            transport.close();
        }

        assertTrue(!transportErrored);  // Normal shutdown does not trigger the event.
        assertTrue(exceptions.isEmpty());
        assertTrue(data.isEmpty());
    }

    @Test
    public void testFailureInInitializationRoutineFailsConnect() throws Exception {
        try (NettyEchoServer server = createEchoServer()) {
            server.start();

            int port = server.getServerPort();

            Transport transport = createTransport(createTransportOptions(), createSSLOptions());
            try {
                transport.connect(HOSTNAME, port, new NettyTransportListener() {

                    @Override
                    public void transportInitialized(Transport transport) {
                        throw new RuntimeException();
                    }
                }).awaitConnect();
                fail("Should not have connected to the server at " + HOSTNAME + ":" + port);
            } catch (Exception e) {
                LOG.info("Failed to connect to: {}:{} as expected.", HOSTNAME, port);
            }

            assertFalse(transport.isConnected(), "Should not be connected");
            assertEquals(HOSTNAME, transport.getHost(), "Server host is incorrect");
            assertEquals(port, transport.getPort(), "Server port is incorrect");

            transport.close();
        }

        assertTrue(transportErrored);
        assertFalse(exceptions.isEmpty());
        assertTrue(data.isEmpty());
    }

    @Disabled("Used for checking for transport level leaks, my be unstable on CI.")
    @Test
    public void testSendToClosedTransportFailsButDoesNotLeak() throws Exception {
        Transport transport = null;

        ResourceLeakDetector.setLevel(Level.PARANOID);

        try (NettyEchoServer server = createEchoServer()) {
            server.start();

            int port = server.getServerPort();

            for (int i = 0; i < 256; ++i) {
                transport = createTransport(createTransportOptions(), createSSLOptions());
                try {
                    transport.connect(HOSTNAME, port, testListener).awaitConnect();
                    LOG.info("Connected to server:{}:{} as expected.", HOSTNAME, port);
                } catch (Exception e) {
                    fail("Should not have failed to connect to the server at " + HOSTNAME + ":" + port + " but got exception: " + e);
                }

                assertTrue(transport.isConnected());

                ProtonBuffer sendBuffer = transport.getBufferAllocator().outputBuffer(10 * 1024 * 1024);
                sendBuffer.writeBytes(new byte[] {0, 1, 2, 3, 4});

                transport.close();

                try {
                    transport.writeAndFlush(sendBuffer);
                    fail("Should throw on send of closed transport");
                } catch (IOException ex) {
                }
            }

            System.gc();
        }
    }

    @Test
    public void testCreateFailsIfUnknownPreferredNativeIOLayerSelected() throws Exception {
        TransportOptions options = createTransportOptions();
        options.allowNativeIO(true);
        options.nativeIOPreference("NATIVE-IO");

        assertThrows(IllegalArgumentException.class, () -> createTransport(options, createSSLOptions()));
    }

    @Test
    public void testConnectToServerWithEpollEnabled() throws Exception {
        doTestEpollSupport(true);
    }

    @Test
    public void testConnectToServerWithEpollDisabled() throws Exception {
        doTestEpollSupport(false);
    }

    private void doTestEpollSupport(boolean useEpoll) throws Exception {
        assumeTrue(Epoll.isAvailable());

        try (NettyEchoServer server = createEchoServer()) {
            server.start();

            int port = server.getServerPort();

            TransportOptions options = createTransportOptions();
            options.allowNativeIO(useEpoll);
            options.nativeIOPreference("EPOLL");
            Transport transport = createTransport(options, createSSLOptions());
            try {
                transport.connect(HOSTNAME, port, testListener).awaitConnect();
                LOG.info("Connected to server:{}:{} as expected.", HOSTNAME, port);
            } catch (Exception e) {
                fail("Should not have failed to connect to the server at " + HOSTNAME + ":" + port + " but got exception: " + e);
            }

            assertTrue(transport.isConnected());
            assertEquals(HOSTNAME, transport.getHost(), "Server host is incorrect");
            assertEquals(port, transport.getPort(), "Server port is incorrect");
            assertEpoll("Transport should be using Epoll", useEpoll, transport);

            transport.close();

            // Additional close should not fail or cause other problems.
            transport.close();
        }

        assertFalse(transportErrored);
        assertTrue(exceptions.isEmpty());
        assertTrue(data.isEmpty());
    }

    @Test
    public void testConnectToServerWithIOUringEnabled() throws Exception {
        doTestIORingSupport(true);
    }

    @Test
    public void testConnectToServerWithIOUringDisabled() throws Exception {
        doTestIORingSupport(false);
    }

    private void doTestIORingSupport(boolean useIOUring) throws Exception {
        assumeTrue(IOUring.isAvailable());

        try (NettyEchoServer server = createEchoServer()) {
            server.start();

            int port = server.getServerPort();

            TransportOptions options = createTransportOptions();
            options.allowNativeIO(useIOUring);
            options.nativeIOPreference("IO_URING");
            Transport transport = createTransport(options, createSSLOptions());
            try {
                transport.connect(HOSTNAME, port, testListener).awaitConnect();
                LOG.info("Connected to server:{}:{} as expected.", HOSTNAME, port);
            } catch (Exception e) {
                fail("Should not have failed to connect to the server at " + HOSTNAME + ":" + port + " but got exception: " + e);
            }

            assertTrue(transport.isConnected());
            assertEquals(HOSTNAME, transport.getHost(), "Server host is incorrect");
            assertEquals(port, transport.getPort(), "Server port is incorrect");
            assertIOUring("Transport should be using URing", useIOUring, transport);

            transport.close();

            // Additional close should not fail or cause other problems.
            transport.close();
        }

        assertFalse(transportErrored);
        assertTrue(exceptions.isEmpty());
        assertTrue(data.isEmpty());
    }

    private void assertEpoll(String message, boolean expected, Transport transport) throws Exception {
        Field bootstrap = null;
        Class<?> transportType = transport.getClass();

        while (transportType != null && bootstrap == null) {
            try {
                bootstrap = transportType.getDeclaredField("bootstrap");
            } catch (NoSuchFieldException error) {
                transportType = transportType.getSuperclass();
                if (Object.class.equals(transportType)) {
                    transportType = null;
                }
            }
        }

        assertNotNull(bootstrap, "Transport implementation unknown");

        bootstrap.setAccessible(true);

        Bootstrap transportBootstrap = (Bootstrap) bootstrap.get(transport);

        if (expected) {
            assertTrue(transportBootstrap.config().group() instanceof EpollEventLoopGroup, message);
        } else {
            assertFalse(transportBootstrap.config().group() instanceof EpollEventLoopGroup, message);
        }
    }

    private void assertIOUring(String message, boolean expected, Transport transport) throws Exception {
        Field bootstrap = null;
        Class<?> transportType = transport.getClass();

        while (transportType != null && bootstrap == null) {
            try {
                bootstrap = transportType.getDeclaredField("bootstrap");
            } catch (NoSuchFieldException error) {
                transportType = transportType.getSuperclass();
                if (Object.class.equals(transportType)) {
                    transportType = null;
                }
            }
        }

        assertNotNull(bootstrap, "Transport implementation unknown");

        bootstrap.setAccessible(true);

        Bootstrap transportBootstrap = (Bootstrap) bootstrap.get(transport);

        if (expected) {
            assertTrue(transportBootstrap.config().group() instanceof IOUringEventLoopGroup, message);
        } else {
            assertFalse(transportBootstrap.config().group() instanceof IOUringEventLoopGroup, message);
        }
    }

    @Test
    public void testConnectToServerWithKQueueEnabled() throws Exception {
        doTestKQueueSupport(true);
    }

    @Test
    public void testConnectToServerWithKQueueDisabled() throws Exception {
        doTestKQueueSupport(false);
    }

    private void doTestKQueueSupport(boolean useKQueue) throws Exception {
        assumeTrue(KQueue.isAvailable());

        try (NettyEchoServer server = createEchoServer()) {
            server.start();

            int port = server.getServerPort();

            TransportOptions options = createTransportOptions();
            options.allowNativeIO(true);
            Transport transport = createTransport(options, createSSLOptions());
            try {
                transport.connect(HOSTNAME, port, testListener).awaitConnect();
                LOG.info("Connected to server:{}:{} as expected.", HOSTNAME, port);
            } catch (Exception e) {
                fail("Should not have failed to connect to the server at " + HOSTNAME + ":" + port + " but got exception: " + e);
            }

            assertTrue(transport.isConnected());
            assertEquals(HOSTNAME, transport.getHost(), "Server host is incorrect");
            assertEquals(port, transport.getPort(), "Server port is incorrect");
            assertKQueue("Transport should be using Kqueue", useKQueue, transport);

            transport.close();

            // Additional close should not fail or cause other problems.
            transport.close();
        }

        assertTrue(!transportErrored);  // Normal shutdown does not trigger the event.
        assertTrue(exceptions.isEmpty());
        assertTrue(data.isEmpty());
    }

    @Test
    public void testFallbackToNioWhenNativeIOConfiguredNotSupportedEpoll() throws Exception {
        assumeFalse(Epoll.isAvailable());

        doTestFallbackToNioWhenNativeLayerNotSupported(EpollSupport.NAME);
    }

    @Test
    public void testFallbackToNioWhenNativeIOConfiguredNotSupportedKQueue() throws Exception {
        assumeFalse(KQueue.isAvailable());

        doTestFallbackToNioWhenNativeLayerNotSupported(KQueueSupport.NAME);
    }

    private void doTestFallbackToNioWhenNativeLayerNotSupported(String nativeIOLayer) throws Exception {
        try (NettyEchoServer server = createEchoServer()) {
            server.start();

            int port = server.getServerPort();

            TransportOptions options = createTransportOptions();
            options.allowNativeIO(true);
            options.nativeIOPreference(nativeIOLayer);

            Transport transport = createTransport(options, createSSLOptions());
            try {
                transport.connect(HOSTNAME, port, testListener).awaitConnect();
                LOG.info("Connected to server:{}:{} as expected.", HOSTNAME, port);
            } catch (Exception e) {
                fail("Should not have failed to connect to the server at " + HOSTNAME + ":" + port + " but got exception: " + e);
            }

            assertTrue(transport.isConnected());
            assertEquals(HOSTNAME, transport.getHost(), "Server host is incorrect");
            assertEquals(port, transport.getPort(), "Server port is incorrect");

            transport.close();

            // Additional close should not fail or cause other problems.
            transport.close();
        }

        assertFalse(transportErrored);
        assertTrue(exceptions.isEmpty());
        assertTrue(data.isEmpty());
    }

    private void assertKQueue(String message, boolean expected, Transport transport) throws Exception {
        Field group = null;
        Class<?> transportType = transport.getClass();

        while (transportType != null && group == null) {
            try {
                group = transportType.getDeclaredField("group");
            } catch (NoSuchFieldException error) {
                transportType = transportType.getSuperclass();
                if (Object.class.equals(transportType)) {
                    transportType = null;
                }
            }
        }

        assertNotNull(group, "Transport implementation unknown");

        group.setAccessible(true);
        if (expected) {
            assertTrue(group.get(transport) instanceof KQueueEventLoopGroup, message);
        } else {
            assertFalse(group.get(transport) instanceof KQueueEventLoopGroup, message);
        }
    }

    protected IOContext createContext(TransportOptions options, SslOptions sslOptions) {
        if (context != null) {
            throw new IllegalStateException("Test already has a defined Netty IO Context");
        }

        return this.context = new Netty4IOContext(options, sslOptions, getTestName());
    }

    protected Transport createTransport(TransportOptions options, SslOptions sslOptions) {
        if (context != null) {
            throw new IllegalStateException("Test already has a defined Netty IO Context");
        } else {
            this.context = new Netty4IOContext(options, sslOptions, getTestName());
        }

        return context.newTransport();
    }

    protected TransportOptions createTransportOptions() {
        return new TransportOptions();
    }

    protected SslOptions createSSLOptions() {
        return new SslOptions().sslEnabled(false);
    }

    protected TransportOptions createServerTransportOptions() {
        return new TransportOptions();
    }

    protected SslOptions createServerSSLOptions() {
        return new SslOptions().sslEnabled(false);
    }

    protected void logTransportErrors() {
        if (!exceptions.isEmpty()) {
            for(Throwable ex : exceptions) {
                LOG.info("Transport sent exception: {}", ex, ex);
            }
        }
    }

    protected final NettyEchoServer createEchoServer() {
        return createEchoServer(false);
    }

    protected final NettyEchoServer createEchoServer(SslOptions options) {
        return createEchoServer(options, false);
    }

    protected final NettyEchoServer createEchoServer(boolean needClientAuth) {
        return createEchoServer(createServerSSLOptions(), needClientAuth);
    }

    protected final NettyEchoServer createEchoServer(SslOptions options, boolean needClientAuth) {
        return createEchoServer(createServerTransportOptions(), options, needClientAuth);
    }

    protected final NettyEchoServer createEchoServer(TransportOptions options) {
        return new NettyEchoServer(options, createServerSSLOptions(), false);
    }

    protected final NettyEchoServer createEchoServer(TransportOptions options, SslOptions sslOptions, boolean needClientAuth) {
        return new NettyEchoServer(options, sslOptions, needClientAuth);
    }

    public class NettyTransportListener implements TransportListener {

        @Override
        public void transportRead(ProtonBuffer incoming) {
            LOG.debug("Client has new incoming data of size: {}", incoming.getReadableBytes());
            bytesRead.addAndGet(incoming.getReadableBytes());
            data.add(incoming.transfer());
        }

        @Override
        public void transportError(Throwable cause) {
            LOG.info("Transport error caught: {}", cause.getMessage(), cause);
            transportErrored = true;
            exceptions.add(cause);
        }

        @Override
        public void transportConnected(Transport transport) {
            transportConnected = true;
        }

        @Override
        public void transportInitialized(Transport transport) {
            transportInitialized = true;
        }
    }
}
