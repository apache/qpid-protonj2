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
package org.apache.qpid.protonj2.client.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonNettyByteBuffer;
import org.apache.qpid.protonj2.client.SslOptions;
import org.apache.qpid.protonj2.client.TransportOptions;
import org.apache.qpid.protonj2.client.impl.ClientThreadFactory;
import org.apache.qpid.protonj2.client.test.ImperativeClientTestCase;
import org.apache.qpid.protonj2.client.test.Wait;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.kqueue.KQueue;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
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

    protected boolean transportClosed;
    protected final List<Throwable> exceptions = new ArrayList<>();
    protected final List<ProtonBuffer> data = new ArrayList<>();
    protected final AtomicInteger bytesRead = new AtomicInteger();

    protected final TransportListener testListener = new NettyTransportListener(false);

    @Test
    public void testCloseOnNeverConnectedTransport() throws Exception {
        Transport transport = createTransport(HOSTNAME, 5672, testListener, createTransportOptions(), createServerSSLOptions());
        assertFalse(transport.isConnected());

        transport.close();

        assertTrue(!transportClosed);
        assertTrue(exceptions.isEmpty());
        assertTrue(data.isEmpty());
    }

    @Test
    public void testCreateWithBadHostOrPortThrowsIAE() throws Exception {
        try {
            createTransport(HOSTNAME, -1, testListener, createTransportOptions(), createSSLOptions());
            fail("Should have thrown IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
        }

        try {
            createTransport(null, 5672, testListener, createTransportOptions(), createSSLOptions());
            fail("Should have thrown IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
        }

        try {
            createTransport("", 5672, testListener, createTransportOptions(), createSSLOptions());
            fail("Should have thrown IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
        }
    }

    @Test
    public void testCreateWithNullOptionsThrowsIAE() throws Exception {
        try {
            createTransport(HOSTNAME, 5672, testListener, null, null);
            fail("Should have thrown IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
        }

        try {
            createTransport(HOSTNAME, 5672, testListener, createTransportOptions(), null);
            fail("Should have thrown IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
        }

        try {
            createTransport(HOSTNAME, 5672, testListener, null, createSSLOptions());
            fail("Should have thrown IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
        }
    }

    @Test
    public void testConnectWithCustomThreadFactoryConfigured() throws Exception {
        try (NettyEchoServer server = createEchoServer()) {
            server.start();

            final int port = server.getServerPort();

            ClientThreadFactory factory = new ClientThreadFactory("NettyTransportTest", true);

            TcpTransport transport = createTransport(HOSTNAME, port, testListener, createTransportOptions(), createSSLOptions());
            transport.setThreadFactory(factory);

            try {
                transport.connect(null);
            } catch (Exception e) {
                LOG.info("Failed to connect to: {}:{} as expected.", HOSTNAME, port);
                fail("Should have failed to connect to the server: " + HOSTNAME + ":" + port);
            }

            assertTrue(transport.isConnected());
            assertSame(factory, transport.getThreadFactory());

            try {
                transport.setThreadFactory(factory);
            } catch (IllegalStateException expected) {
                LOG.trace("Caught expected state exception");
            }

            transport.close();
        }

        assertTrue(!transportClosed);  // Normal shutdown does not trigger the event.
        assertTrue(exceptions.isEmpty());
        assertTrue(data.isEmpty());
    }

    @Test
    public void testConnectWithoutRunningServer() throws Exception {
        try (NettyEchoServer server = createEchoServer()) {
            server.start();

            final int port = server.getServerPort();

            server.close();

            Transport transport = createTransport(HOSTNAME, port, testListener, createTransportOptions(), createSSLOptions());
            try {
                transport.connect(null);
                fail("Should have failed to connect to the server: " + HOSTNAME + ":" + port);
            } catch (Exception e) {
                LOG.info("Failed to connect to: {}:{} as expected.", HOSTNAME, port);
            }

            assertFalse(transport.isConnected());

            transport.close();
        }

        assertTrue(!transportClosed);  // Normal shutdown does not trigger the event.
        assertTrue(exceptions.isEmpty());
        assertTrue(data.isEmpty());
    }

    @Test
    public void testConnectWithoutListenerFails() throws Exception {
        try (NettyEchoServer server = createEchoServer()) {
            server.start();

            final int port = server.getServerPort();

            Transport transport = createTransport(HOSTNAME, port, null, createTransportOptions(), createSSLOptions());
            try {
                transport.connect(null);
                fail("Should have failed to connect to the server: " + HOSTNAME + ":" + port);
            } catch (Exception e) {
                LOG.info("Failed to connect to: {}:{} as expected.", HOSTNAME, port);
            }

            assertFalse(transport.isConnected());

            transport.close();
        }
    }

    @Test
    public void testConnectAfterListenerSetWorks() throws Exception {
        try (NettyEchoServer server = createEchoServer()) {
            server.start();

            final int port = server.getServerPort();

            TcpTransport transport = createTransport(HOSTNAME, port, null, createTransportOptions(), createSSLOptions());
            assertNull(transport.getTransportListener());
            transport.setTransportListener(testListener);
            assertNotNull(transport.getTransportListener());

            try {
                transport.connect(null);
                LOG.info("Connected to server:{}:{} as expected.", HOSTNAME, port);
            } catch (Exception e) {
                fail("Should not have failed to connect to the server at " + HOSTNAME + ":" + port + " but got exception: " + e);
            }

            assertTrue(transport.isConnected());

            transport.close();
        }
    }

    @Test
    public void testConnectToServer() throws Exception {
        try (NettyEchoServer server = createEchoServer()) {
            server.start();

            final int port = server.getServerPort();

            Transport transport = createTransport(HOSTNAME, port, testListener, createTransportOptions(), createSSLOptions());
            try {
                transport.connect(null);
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

        assertTrue(!transportClosed);  // Normal shutdown does not trigger the event.
        assertTrue(exceptions.isEmpty());
        assertTrue(data.isEmpty());
    }

    @Test
    public void testMultipleConnectionsToServer() throws Exception {
        final int CONNECTION_COUNT = 10;

        try (NettyEchoServer server = createEchoServer()) {
            server.start();

            final int port = server.getServerPort();

            List<Transport> transports = new ArrayList<Transport>();

            for (int i = 0; i < CONNECTION_COUNT; ++i) {
                Transport transport = createTransport(HOSTNAME, port, testListener, createTransportOptions(), createSSLOptions());
                try {
                    transport.connect(null);
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

        assertTrue(!transportClosed);  // Normal shutdown does not trigger the event.
        assertTrue(exceptions.isEmpty());
        assertTrue(data.isEmpty());
    }

    @Test
    public void testMultipleConnectionsSendReceive() throws Exception {
        final int CONNECTION_COUNT = 10;
        final int FRAME_SIZE = 8;

        ProtonNettyByteBuffer sendBuffer = new ProtonNettyByteBuffer(Unpooled.buffer(FRAME_SIZE));
        for (int i = 0; i < 8; ++i) {
            sendBuffer.writeByte('A');
        }

        try (NettyEchoServer server = createEchoServer()) {
            server.start();

            final int port = server.getServerPort();

            List<Transport> transports = new ArrayList<Transport>();

            for (int i = 0; i < CONNECTION_COUNT; ++i) {
                Transport transport = createTransport(HOSTNAME, port, testListener, createTransportOptions(), createSSLOptions());
                try {
                    transport.connect(null);
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

            transport = createTransport(HOSTNAME, port, testListener, createTransportOptions(), createSSLOptions());
            try {
                transport.connect(null);
                LOG.info("Connected to server:{}:{} as expected.", HOSTNAME, port);
            } catch (Exception e) {
                fail("Should not have failed to connect to the server at " + HOSTNAME + ":" + port + " but got exception: " + e);
            }

            assertTrue(transport.isConnected());

            server.close();
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
    public void testZeroSizedSentNoErrors() throws Exception {
        try (NettyEchoServer server = createEchoServer()) {
            server.start();

            int port = server.getServerPort();

            Transport transport = createTransport(HOSTNAME, port, testListener, createTransportOptions(), createSSLOptions());
            try {
                transport.connect(null);
                LOG.info("Connected to server:{}:{} as expected.", HOSTNAME, port);
            } catch (Exception e) {
                fail("Should not have failed to connect to the server at " + HOSTNAME + ":" + port + " but got exception: " + e);
            }

            assertTrue(transport.isConnected());

            transport.writeAndFlush(new ProtonNettyByteBuffer(Unpooled.buffer(0)));

            transport.close();
        }

        assertTrue(!transportClosed);  // Normal shutdown does not trigger the event.
        assertTrue(exceptions.isEmpty());
        assertTrue(data.isEmpty());
    }

    @Test
    public void testDataSentIsReceived() throws Exception {
        try (NettyEchoServer server = createEchoServer()) {
            server.start();

            int port = server.getServerPort();

            Transport transport = createTransport(HOSTNAME, port, testListener, createTransportOptions(), createSSLOptions());
            try {
                transport.connect(null);
                LOG.info("Connected to server:{}:{} as expected.", HOSTNAME, port);
            } catch (Exception e) {
                fail("Should not have failed to connect to the server at " + HOSTNAME + ":" + port + " but got exception: " + e);
            }

            assertTrue(transport.isConnected());

            ProtonBuffer sendBuffer = transport.getBufferAllocator().outputBuffer(SEND_BYTE_COUNT);
            for (int i = 0; i < SEND_BYTE_COUNT; ++i) {
                sendBuffer.writeByte('A');
            }

            transport.writeAndFlush(sendBuffer);

            assertTrue(Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisfied() throws Exception {
                    return !data.isEmpty();
                }
            }, 10000, 50));

            assertEquals(SEND_BYTE_COUNT, data.get(0).getReadableBytes());

            transport.close();
        }

        assertTrue(!transportClosed);  // Normal shutdown does not trigger the event.
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

            Transport transport = createTransport(HOSTNAME, port, testListener, createTransportOptions(), createSSLOptions());
            try {
                transport.connect(null);
                LOG.info("Connected to server:{}:{} as expected.", HOSTNAME, port);
            } catch (Exception e) {
                fail("Should not have failed to connect to the server at " + HOSTNAME + ":" + port + " but got exception: " + e);
            }

            assertTrue(transport.isConnected());

            ProtonNettyByteBuffer sendBuffer = new ProtonNettyByteBuffer(Unpooled.buffer(byteCount));
            for (int i = 0; i < byteCount; ++i) {
                sendBuffer.writeByte('A');
            }

            for (int i = 0; i < iterations; ++i) {
                transport.writeAndFlush(sendBuffer.copy());
            }

            assertTrue(Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisfied() throws Exception {
                    return bytesRead.get() == (byteCount * iterations);
                }
            }, 10000, 50));

            transport.close();
        }

        assertTrue(!transportClosed);  // Normal shutdown does not trigger the event.
        assertTrue(exceptions.isEmpty());
    }

    @Test
    public void testSendToClosedTransportFails() throws Exception {
        Transport transport = null;

        try (NettyEchoServer server = createEchoServer()) {
            server.start();

            int port = server.getServerPort();

            transport = createTransport(HOSTNAME, port, testListener, createTransportOptions(), createSSLOptions());
            try {
                transport.connect(null);
                LOG.info("Connected to server:{}:{} as expected.", HOSTNAME, port);
            } catch (Exception e) {
                fail("Should not have failed to connect to the server at " + HOSTNAME + ":" + port + " but got exception: " + e);
            }

            assertTrue(transport.isConnected());

            transport.close();

            ProtonNettyByteBuffer sendBuffer = new ProtonNettyByteBuffer(Unpooled.buffer(10));
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
            final AtomicBoolean initialized = new AtomicBoolean();

            Transport transport = createTransport(HOSTNAME, port, testListener, createTransportOptions(), createSSLOptions());
            try {
                transport.connect(() -> initialized.set(true));
                LOG.info("Connected to server:{}:{} as expected.", HOSTNAME, port);
            } catch (Exception e) {
                fail("Should not have failed to connect to the server at " + HOSTNAME + ":" + port + " but got exception: " + e);
            }

            assertTrue(transport.isConnected());
            assertEquals(HOSTNAME, transport.getHost(), "Server host is incorrect");
            assertEquals(port, transport.getPort(), "Server port is incorrect");
            assertTrue(initialized.get());

            transport.close();
        }

        assertTrue(!transportClosed);  // Normal shutdown does not trigger the event.
        assertTrue(exceptions.isEmpty());
        assertTrue(data.isEmpty());
    }

    @Test
    public void testFailureInInitializationRoutineFailsConnect() throws Exception {
        try (NettyEchoServer server = createEchoServer()) {
            server.start();

            int port = server.getServerPort();

            Transport transport = createTransport(HOSTNAME, port, testListener, createTransportOptions(), createSSLOptions());
            try {
                transport.connect(() -> { throw new RuntimeException(); });
                fail("Should not have connected to the server at " + HOSTNAME + ":" + port);
            } catch (Exception e) {
                LOG.info("Failed to connect to: {}:{} as expected.", HOSTNAME, port);
            }

            assertFalse(transport.isConnected(), "Should not be connected");
            assertEquals(HOSTNAME, transport.getHost(), "Server host is incorrect");
            assertEquals(port, transport.getPort(), "Server port is incorrect");

            transport.close();
        }

        assertFalse(transportClosed);  // Normal shutdown does not trigger the event.
        assertTrue(exceptions.isEmpty());
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
                transport = createTransport(HOSTNAME, port, testListener, createTransportOptions(), createSSLOptions());
                try {
                    transport.connect(null);
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
            Transport transport = createTransport(HOSTNAME, port, testListener, options, createSSLOptions());
            try {
                transport.connect(null);
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

        assertTrue(!transportClosed);  // Normal shutdown does not trigger the event.
        assertTrue(exceptions.isEmpty());
        assertTrue(data.isEmpty());
    }

    private void assertEpoll(String message, boolean expected, Transport transport) throws Exception {
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
            assertTrue(group.get(transport) instanceof EpollEventLoopGroup, message);
        } else {
            assertFalse(group.get(transport) instanceof EpollEventLoopGroup, message);
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
            Transport transport = createTransport(HOSTNAME, port, testListener, options, createSSLOptions());
            try {
                transport.connect(null);
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

        assertTrue(!transportClosed);  // Normal shutdown does not trigger the event.
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

    protected TcpTransport createTransport(String host, int port, TransportListener listener, TransportOptions options, SslOptions sslOptions) {
        TcpTransport transport = new TcpTransport(host, port, options, sslOptions);
        transport.setTransportListener(listener);
        return transport;
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

    protected final NettyEchoServer createEchoServer(TransportOptions options, SslOptions sslOptions, boolean needClientAuth) {
        return new NettyEchoServer(options, sslOptions, needClientAuth);
    }

    public class NettyTransportListener implements TransportListener {
        final boolean retainDataBufs;

        NettyTransportListener(boolean retainDataBufs) {
            this.retainDataBufs = retainDataBufs;
        }

        @Override
        public void onData(ProtonBuffer incoming) {
            LOG.debug("Client has new incoming data of size: {}", incoming.getReadableBytes());
            data.add(incoming);
            bytesRead.addAndGet(incoming.getReadableBytes());

            if (retainDataBufs) {
                ((ByteBuf) incoming.unwrap()).retain();
            }
        }

        @Override
        public void onTransportClosed() {
            LOG.debug("Transport reports that it has closed.");
            transportClosed = true;
        }

        @Override
        public void onTransportError(Throwable cause) {
            LOG.info("Transport error caught: {}", cause.getMessage(), cause);
            exceptions.add(cause);
        }
    }
}
