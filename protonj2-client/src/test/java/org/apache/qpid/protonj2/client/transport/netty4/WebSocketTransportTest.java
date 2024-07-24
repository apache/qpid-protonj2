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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferAllocator;
import org.apache.qpid.protonj2.client.SslOptions;
import org.apache.qpid.protonj2.client.TransportOptions;
import org.apache.qpid.protonj2.client.test.Wait;
import org.apache.qpid.protonj2.client.transport.Transport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler.HandshakeComplete;

/**
 * Test the Netty based WebSocket Transport
 */
@Timeout(30)
public class WebSocketTransportTest extends TcpTransportTest {

    private static final Logger LOG = LoggerFactory.getLogger(WebSocketTransportTest.class);

    @Override
    protected TransportOptions createTransportOptions() {
        return new TransportOptions().useWebSockets(true);
    }

    @Override
    protected TransportOptions createServerTransportOptions() {
        return new TransportOptions().useWebSockets(true);
    }

    @Override
    @Test
    public void testCannotCreateWithIllegalArgs() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> new WebSocketTransport(null, createTransportOptions(), createSSLOptions()));
        assertThrows(IllegalArgumentException.class, () -> new WebSocketTransport(new Bootstrap(), null, createSSLOptions()));
        assertThrows(IllegalArgumentException.class, () -> new WebSocketTransport(new Bootstrap(), createTransportOptions(), null));
    }

    @Test
    public void testConnectToServerUsingCorrectPath() throws Exception {
        final String WEBSOCKET_PATH = "/testpath";

        try (NettyEchoServer server = createEchoServer()) {
            server.setWebSocketPath(WEBSOCKET_PATH);
            server.start();

            final int port = server.getServerPort();

            Transport transport = createTransport(createTransportOptions().webSocketPath(WEBSOCKET_PATH), createSSLOptions());

            try {
                transport.connect(HOSTNAME, port, testListener).awaitConnect();
                LOG.info("Connected to server:{}:{} as expected.", HOSTNAME, port);
            } catch (Exception e) {
                fail("Should have connected to the server at " + HOSTNAME + ":" + port + " but got exception: " + e);
            }

            assertTrue(transport instanceof WebSocketTransport);
            assertTrue(transport.isConnected());
            assertEquals(HOSTNAME, transport.getHost(), "Server host is incorrect");
            assertEquals(port, transport.getPort(), "Server port is incorrect");

            transport.close();

            // Additional close should not fail or cause other problems.
            transport.close();
        }

        assertTrue(!transportErrored);  // Normal shutdown does not trigger the event.
        assertTrue(exceptions.isEmpty());
        assertTrue(data.isEmpty());
    }

    @Test
    public void testConnectToServerUsingIncorrectPath() throws Exception {
        final String WEBSOCKET_PATH = "/testpath";

        try (NettyEchoServer server = createEchoServer()) {
            // No configured path means it won't match the requested one.
            server.start();

            final int port = server.getServerPort();

            server.close();

            Transport transport = createTransport(createTransportOptions().webSocketPath(WEBSOCKET_PATH), createSSLOptions());

            try {
                transport.connect(HOSTNAME, port, testListener).awaitConnect();
                fail("Should have failed to connect to the server: " + HOSTNAME + ":" + port);
            } catch (Exception e) {
                LOG.info("Failed to connect to: {}:{} as expected.", HOSTNAME, port);
            }

            assertTrue(transport instanceof WebSocketTransport);
            assertFalse(transport.isConnected());

            transport.close();
        }

        assertTrue(transportErrored);
        assertFalse(exceptions.isEmpty());
        assertTrue(data.isEmpty());
    }

    @Test
    public void testConnectionsSendReceiveLargeDataWhenFrameSizeAllowsIt() throws Exception {
        final int FRAME_SIZE = 8192;

        final ProtonBuffer sendBuffer = allocator.allocate(FRAME_SIZE);
        for (int i = 0; i < FRAME_SIZE; ++i) {
            sendBuffer.writeByte((byte) 'A');
        }

        try (NettyEchoServer server = createEchoServer()) {
            // Server should pass the data through without issue with this size
            server.setMaxFrameSize(FRAME_SIZE);
            server.start();

            final int port = server.getServerPort();

            List<Transport> transports = new ArrayList<>();

            Transport transport = createTransport(createTransportOptions().webSocketMaxFrameSize(FRAME_SIZE), createSSLOptions());

            try {
                // The transport should allow for the size of data we sent.
                transport.connect(HOSTNAME, port, testListener).awaitConnect();
                transports.add(transport);
                transport.writeAndFlush(sendBuffer.copy());
            } catch (Exception e) {
                fail("Should have connected to the server at " + HOSTNAME + ":" + port + " but got exception: " + e);
            }

            assertTrue(transport instanceof WebSocketTransport);
            assertTrue(Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisfied() throws Exception {
                    LOG.debug("Checking completion: read {} expecting {}", bytesRead.get(), FRAME_SIZE);
                    return bytesRead.get() == FRAME_SIZE || !transport.isConnected();
                }
            }, 10000, 50));

            assertTrue(transport.isConnected(), "Connection failed while receiving.");

            transport.close();
        }

        assertTrue(exceptions.isEmpty());
    }

    @Test
    public void testConnectionReceivesFragmentedDataSingleWriteAndFlush() throws Exception {
        testConnectionReceivesFragmentedData(true);
    }

    @Test
    public void testConnectionReceivesFragmentedDataWriteThenFlush() throws Exception {
        testConnectionReceivesFragmentedData(false);
    }

    private void testConnectionReceivesFragmentedData(boolean writeAndFlush) throws Exception {

        final int FRAME_SIZE = 5317;

        final ProtonBuffer sendBuffer = allocator.allocate(FRAME_SIZE);
        for (int i = 0; i < FRAME_SIZE; ++i) {
            sendBuffer.writeByte((byte) ('A' + (i % 10)));
        }

        try (NettyEchoServer server = createEchoServer()) {
            server.setMaxFrameSize(FRAME_SIZE);
            // Server should fragment the data as it goes through
            server.setFragmentWrites(true);
            server.start();

            final int port = server.getServerPort();

            List<Transport> transports = new ArrayList<>();

            TransportOptions clientOptions = createTransportOptions();
            clientOptions.traceBytes(true);
            clientOptions.webSocketMaxFrameSize(FRAME_SIZE);

            NettyTransportListener wsListener = new NettyTransportListener();

            Transport transport = createTransport(clientOptions, createSSLOptions());
            try {
                transport.connect(HOSTNAME, port, wsListener).awaitConnect();
                transports.add(transport);
                if (writeAndFlush) {
                    transport.writeAndFlush(allocator.allocate());
                    transport.writeAndFlush(sendBuffer.copy());
                } else {
                    transport.write(allocator.allocate());
                    transport.write(sendBuffer.copy());
                    transport.flush();
                }
            } catch (Exception e) {
                fail("Should have connected to the server at " + HOSTNAME + ":" + port + " but got exception: " + e);
            }

            assertTrue(transport instanceof WebSocketTransport);
            assertTrue(Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisfied() throws Exception {
                    LOG.debug("Checking completion: read {} expecting {}", bytesRead.get(), FRAME_SIZE);
                    return bytesRead.get() == FRAME_SIZE || !transport.isConnected();
                }
            }, 10000, 50));

            assertTrue(transport.isConnected(), "Connection failed while receiving.");

            transport.close();

            assertEquals(2, data.size(), "Expected 2 data packets due to separate websocket frames");

            ProtonBuffer receivedBuffer = ProtonBufferAllocator.defaultAllocator().allocate(FRAME_SIZE);
            for (ProtonBuffer buf : data) {
                buf.copyInto(buf.getReadOffset(), receivedBuffer, receivedBuffer.getWriteOffset(), buf.getReadableBytes());
                receivedBuffer.advanceWriteOffset(buf.getReadableBytes());
            }

            assertEquals(FRAME_SIZE, receivedBuffer.getReadableBytes(), "Unexpected data length");
            assertEquals(sendBuffer, receivedBuffer, "Unexpected data");
        } finally {
            for (ProtonBuffer buf : data) {
                ((ByteBuf) buf.unwrap()).release();
            }
        }

        assertTrue(exceptions.isEmpty());
    }

    @Test
    public void testConnectionsSendReceiveLargeDataFailsDueToMaxFrameSize() throws Exception {
        final int FRAME_SIZE = 1024;

        final ProtonBuffer sendBuffer = allocator.allocate(FRAME_SIZE);
        for (int i = 0; i < FRAME_SIZE; ++i) {
            sendBuffer.writeByte((byte) 'A');
        }

        try (NettyEchoServer server = createEchoServer()) {
            // Server should pass the data through, client should choke on the incoming size.
            server.setMaxFrameSize(FRAME_SIZE);
            server.start();

            final int port = server.getServerPort();

            List<Transport> transports = new ArrayList<>();

            final Transport transport = createTransport(createTransportOptions().webSocketMaxFrameSize(FRAME_SIZE / 2), createSSLOptions());

            try {
                // Transport can't receive anything bigger so it should fail the connection
                // when data arrives that is larger than this value.
                transport.connect(HOSTNAME, port, testListener).awaitConnect();
                transports.add(transport);
                transport.writeAndFlush(sendBuffer.copy());
            } catch (Exception e) {
                fail("Should have connected to the server at " + HOSTNAME + ":" + port + " but got exception: " + e);
            }

            assertTrue(transport instanceof WebSocketTransport);
            assertTrue(Wait.waitFor(() -> !transport.isConnected()), "Transport should have lost connection");
        }

        assertFalse(exceptions.isEmpty());
    }

    @Test
    public void testTransportDetectsConnectionDropWhenServerEnforcesMaxFrameSize() throws Exception {
        final int FRAME_SIZE = 1024;

        final ProtonBuffer sendBuffer = allocator.allocate(FRAME_SIZE);
        for (int i = 0; i < FRAME_SIZE; ++i) {
            sendBuffer.writeByte((byte) 'A');
        }

        try (NettyEchoServer server = createEchoServer()) {
            // Server won't accept the data as it's to large and will close the connection.
            server.setMaxFrameSize(FRAME_SIZE / 2);
            server.start();

            final int port = server.getServerPort();

            List<Transport> transports = new ArrayList<>();

            final Transport transport = createTransport(createTransportOptions().webSocketMaxFrameSize(FRAME_SIZE), createSSLOptions());

            assertTrue(transport instanceof WebSocketTransport);

            try {
                // Transport allows bigger frames in so that server is the one causing the failure.
                transport.connect(HOSTNAME, port, testListener).awaitConnect();
                transports.add(transport);
                transport.writeAndFlush(sendBuffer.copy());
            } catch (Exception e) {
                fail("Should have connected to the server at " + HOSTNAME + ":" + port + " but got exception: " + e);
            }

            assertTrue(Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisfied() throws Exception {
                    try {
                        transport.writeAndFlush(sendBuffer.copy());
                    } catch (IOException e) {
                        LOG.info("Transport send caught error:", e);
                        return true;
                    }

                    return false;
                }
            }, 10000, 10), "Transport should have lost connection");

            transport.close();
        }
    }

    @Test
    public void testTransportConnectionDoesNotDropWhenServerAndClientUseCompressionWithLargePayloads() throws Exception {
        final int FRAME_SIZE = 16384; // This value would exceed the set max frame size without compression.

        final ProtonBuffer sendBuffer = allocator.allocate(FRAME_SIZE);
        for (int i = 0; i < FRAME_SIZE; ++i) {
            sendBuffer.writeByte((byte) 'A');
        }

        try (NettyEchoServer server = createEchoServer(createServerTransportOptions().webSocketCompression(true).traceBytes(true))) {
            // Server won't accept the data as it's to large and will close the connection.
            server.setMaxFrameSize(FRAME_SIZE / 2);
            server.start();

            final int port = server.getServerPort();

            List<Transport> transports = new ArrayList<>();

            final Transport transport = createTransport(createTransportOptions().webSocketMaxFrameSize(FRAME_SIZE)
                                                                                .webSocketCompression(true), createSSLOptions());

            assertTrue(transport instanceof WebSocketTransport);

            try {
                // Transport allows bigger frames in so that server is the one causing the failure.
                transport.connect(HOSTNAME, port, testListener).awaitConnect();
                transports.add(transport);
                transport.writeAndFlush(sendBuffer.copy());
            } catch (Exception e) {
                fail("Should have connected to the server at " + HOSTNAME + ":" + port + " but got exception: " + e);
            }

            assertTrue(Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisfied() throws Exception {
                    try {
                        transport.writeAndFlush(sendBuffer.copy());
                    } catch (IOException e) {
                        LOG.info("Transport send caught error:", e);
                        return false;
                    }

                    return true;
                }
            }, 10000, 10), "Transport should not have lost connection");

            transport.close();
        }
    }

    @Test
    public void testConfiguredHttpHeadersArriveAtServer() throws Exception {
        try (NettyEchoServer server = createEchoServer()) {
            server.start();

            final int port = server.getServerPort();

            TransportOptions clientOptions = createTransportOptions();
            clientOptions.addWebSocketHeader("test-header1", "FOO");
            clientOptions.webSocketHeaders().put("test-header2", "BAR");

            final Transport transport = createTransport(clientOptions, createSSLOptions());

            assertTrue(transport instanceof WebSocketTransport);

            try {
                transport.connect(HOSTNAME, port, testListener).awaitConnect();
                LOG.info("Connected to server:{}:{} as expected.", HOSTNAME, port);
            } catch (Exception e) {
                fail("Should have connected to the server at " + HOSTNAME + ":" + port + " but got exception: " + e);
            }

            assertTrue(transport.isConnected());
            assertEquals(HOSTNAME, transport.getHost(), "Server host is incorrect");
            assertEquals(port, transport.getPort(), "Server port is incorrect");

            assertTrue(server.awaitHandshakeCompletion(2000), "HandshakeCompletion not set within given time");
            HandshakeComplete handshake = server.getHandshakeComplete();
            assertNotNull(handshake, "completion should not be null");
            HttpHeaders requestHeaders = handshake.requestHeaders();

            assertTrue(requestHeaders.contains("test-header1"));
            assertTrue(requestHeaders.contains("test-header2"));

            assertEquals("FOO", requestHeaders.get("test-header1"));
            assertEquals("BAR", requestHeaders.get("test-header2"));

            transport.close();
        }

        assertTrue(!transportErrored);  // Normal shutdown does not trigger the event.
        assertTrue(exceptions.isEmpty());
        assertTrue(data.isEmpty());
    }

    private static final String BROKER_JKS_KEYSTORE = "src/test/resources/broker-jks.keystore";
    private static final String PASSWORD = "password";

    @Test
    public void testNonSslWebSocketConnectionFailsToSslServer() throws Exception {
        SslOptions serverSslOptions = new SslOptions();
        serverSslOptions.keyStoreLocation(BROKER_JKS_KEYSTORE);
        serverSslOptions.keyStorePassword(PASSWORD);
        serverSslOptions.verifyHost(false);
        serverSslOptions.sslEnabled(true);

        try (NettyBlackHoleServer server = new NettyBlackHoleServer(createServerTransportOptions(), serverSslOptions)) {
            server.start();

            final int port = server.getServerPort();

            TransportOptions clientOptions = createTransportOptions();

            final Transport transport = createTransport(clientOptions, createSSLOptions());

            assertTrue(transport instanceof WebSocketTransport);

            try {
                transport.connect(HOSTNAME, port, testListener).awaitConnect();
                fail("should not have connected");
            } catch (Exception e) {
                LOG.trace("Failed to connect with message: {}", e.getMessage());
            }
        }
    }

    @Test
    public void testWebsocketConnectionToBlackHoleServerTimesOut() throws Exception {
        try (NettyBlackHoleServer server = new NettyBlackHoleServer(new TransportOptions(), new SslOptions().sslEnabled(false))) {
            server.start();

            final int port = server.getServerPort();

            TransportOptions clientOptions = createTransportOptions();
            clientOptions.connectTimeout(25);

            final Transport transport = createTransport(clientOptions, createSSLOptions());

            assertTrue(transport instanceof WebSocketTransport);

            try {
                transport.connect(HOSTNAME, port, testListener).awaitConnect();
                fail("should not have connected");
            } catch (Exception e) {
                String message = e.getMessage();
                assertNotNull(message);
                assertTrue(message.contains("WebSocket handshake timed out"), "Unexpected message: " + message);
            }
        }
    }
}
