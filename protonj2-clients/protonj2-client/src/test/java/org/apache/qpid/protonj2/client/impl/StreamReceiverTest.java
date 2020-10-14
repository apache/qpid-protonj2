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
package org.apache.qpid.protonj2.client.impl;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.ConnectionOptions;
import org.apache.qpid.protonj2.client.DeliveryState;
import org.apache.qpid.protonj2.client.StreamDelivery;
import org.apache.qpid.protonj2.client.StreamReceiver;
import org.apache.qpid.protonj2.client.StreamReceiverMessage;
import org.apache.qpid.protonj2.client.StreamReceiverOptions;
import org.apache.qpid.protonj2.client.exceptions.ClientDeliveryAbortedException;
import org.apache.qpid.protonj2.client.exceptions.ClientLinkRemotelyClosedException;
import org.apache.qpid.protonj2.client.test.ImperativeClientTestCase;
import org.apache.qpid.protonj2.client.util.Wait;
import org.apache.qpid.protonj2.test.driver.codec.messaging.Accepted;
import org.apache.qpid.protonj2.test.driver.netty.NettyTestPeer;
import org.apache.qpid.protonj2.types.messaging.AmqpValue;
import org.apache.qpid.protonj2.types.messaging.Data;
import org.apache.qpid.protonj2.types.messaging.Header;
import org.apache.qpid.protonj2.types.transport.Role;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests the {@link ReceiveContext} implementation
 */
@Timeout(20)
class StreamReceiverTest extends ImperativeClientTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(StreamReceiverTest.class);

    @Test
    public void testStreamReceiverConfiguresSessionCapacity_1() throws Exception {
        // Read buffer is always halved by connection when creating new session for the stream
        doTestStreamReceiverSessionCapacity(100_000, 200_000, 1);
    }

    @Test
    public void testStreamReceiverConfiguresSessionCapacity_2() throws Exception {
        // Read buffer is always halved by connection when creating new session for the stream
        doTestStreamReceiverSessionCapacity(100_000, 400_000, 2);
    }

    @Test
    public void testStreamReceiverConfiguresSessionCapacity_3() throws Exception {
        // Read buffer is always halved by connection when creating new session for the stream
        doTestStreamReceiverSessionCapacity(100_000, 600_000, 3);
    }

    private void doTestStreamReceiverSessionCapacity(int maxFrameSize, int readBufferSize, int expectedSessionWindow) throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().withMaxFrameSize(maxFrameSize).respond();
            peer.expectBegin().withIncomingWindow(expectedSessionWindow).respond();
            peer.expectAttach().ofReceiver().respond();
            peer.expectFlow();
            peer.expectDetach().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            ConnectionOptions connectionOptions = new ConnectionOptions().maxFrameSize(maxFrameSize);
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), connectionOptions);
            StreamReceiverOptions streamOptions = new StreamReceiverOptions().readBufferSize(readBufferSize);
            StreamReceiver receiver = connection.openStreamReceiver("test-queue", streamOptions);

            receiver.openFuture().get();
            receiver.closeAsync();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testCreateStreamDeliveryWithoutAnyIncomingDeliveryPresent() throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.RECEIVER.getValue()).respond();
            peer.expectFlow();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            StreamReceiver receiver = connection.openStreamReceiver("test-queue");
            StreamDelivery delivery = receiver.receive(5, TimeUnit.MILLISECONDS);

            assertNull(delivery);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            peer.expectDetach().respond();
            peer.expectClose().respond();

            receiver.closeAsync();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testStreamReceiverAwaitTimedCanBePerformedMultipleTimes() throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.RECEIVER.getValue()).respond();
            peer.expectFlow();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            StreamReceiver receiver = connection.openStreamReceiver("test-queue");

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            assertNull(receiver.receive(3, TimeUnit.MILLISECONDS));
            assertNull(receiver.receive(3, TimeUnit.MILLISECONDS));
            assertNull(receiver.receive(3, TimeUnit.MILLISECONDS));

            peer.expectDetach().respond();
            peer.expectClose().respond();

            receiver.closeAsync();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testReceiveFailsWhenLinkRemotelyClosed() throws Exception {
        doTestReceiveFailsWhenLinkRemotelyClose(false);
    }

    @Test
    public void testTimedReceiveFailsWhenLinkRemotelyClosed() throws Exception {
        doTestReceiveFailsWhenLinkRemotelyClose(true);
    }

    private void doTestReceiveFailsWhenLinkRemotelyClose(boolean timed) throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.RECEIVER.getValue()).respond();
            peer.expectFlow();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            StreamReceiver receiver = connection.openStreamReceiver("test-queue");

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectDetach();
            peer.expectClose().respond();
            peer.remoteDetach().later(50);

            if (timed) {
                assertThrows(ClientLinkRemotelyClosedException.class, () -> receiver.receive(1, TimeUnit.MINUTES));
            } else {
                assertThrows(ClientLinkRemotelyClosedException.class, () -> receiver.receive());
            }

            receiver.closeAsync();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testStreamDeliveryUsesUnsettledDeliveryOnOpen() throws Exception {
        final byte[] payload = createEncodedMessage(new AmqpValue<>("Hello World"));

        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.RECEIVER.getValue()).respond();
            peer.expectFlow();
            peer.remoteTransfer().withHandle(0)
                                 .withDeliveryId(0)
                                 .withDeliveryTag(new byte[] { 1 })
                                 .withMore(false)
                                 .withMessageFormat(0)
                                 .withPayload(payload).queue();
            peer.remoteDisposition().withRole(Role.SENDER.getValue())
                                    .withFirst(0)
                                    .withSettled(true)
                                    .withState(Accepted.getInstance()).queue();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            final Client container = Client.create();
            final Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            final StreamReceiver receiver = connection.openStreamReceiver("test-queue");

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            final StreamDelivery delivery = receiver.receive();

            Wait.assertTrue("Should eventually be remotely settled", delivery::remoteSettled);
            Wait.assertTrue(() -> { return delivery.remoteState() == DeliveryState.accepted(); });

            peer.expectDetach().respond();
            peer.expectClose().respond();

            receiver.closeAsync();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testStreamDeliveryReceiveWithTransferAlreadyComplete() throws Exception {
        doTestStreamDeliveryReceiveWithTransferAlreadyComplete(false);
    }

    @Test
    public void testStreamDeliveryTryReceiveWithTransferAlreadyComplete() throws Exception {
        doTestStreamDeliveryReceiveWithTransferAlreadyComplete(true);
    }

    private void doTestStreamDeliveryReceiveWithTransferAlreadyComplete(boolean tryReceive) throws Exception {
        final byte[] payload = createEncodedMessage(new AmqpValue<>("Hello World"));

        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofReceiver().respond();
            peer.expectFlow();
            peer.remoteTransfer().withHandle(0)
                                 .withDeliveryId(0)
                                 .withDeliveryTag(new byte[] { 1 })
                                 .withMore(false)
                                 .withMessageFormat(0)
                                 .withPayload(payload).queue();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            final Client container = Client.create();
            final Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            final StreamReceiver receiver = connection.openStreamReceiver("test-queue");

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectBegin().respond();
            peer.expectAttach().ofSender().respond();

            // Ensures that stream receiver has the delivery in its queue.
            connection.openSender("test-sender").openFuture().get();

            final StreamDelivery delivery;

            if (tryReceive) {
                delivery = receiver.tryReceive();
            } else {
                delivery = receiver.receive();
            }

            assertNotNull(delivery);
            assertTrue(delivery.completed());
            assertFalse(delivery.aborted());

            peer.expectDetach().respond();
            peer.expectClose().respond();

            receiver.closeAsync();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testStreamDeliveryReceivedWhileTransferIsIncomplete() throws Exception {
        final byte[] payload = createEncodedMessage(new AmqpValue<>("Hello World"));

        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.RECEIVER.getValue()).respond();
            peer.expectFlow();
            peer.remoteTransfer().withHandle(0)
                                 .withDeliveryId(0)
                                 .withDeliveryTag(new byte[] { 1 })
                                 .withMore(true)
                                 .withMessageFormat(0)
                                 .withPayload(payload).queue();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            final Client container = Client.create();
            final Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            final StreamReceiver receiver = connection.openStreamReceiver("test-queue");
            final StreamDelivery delivery = receiver.receive();

            assertNotNull(delivery);
            assertFalse(delivery.completed());

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            peer.remoteTransfer().withHandle(0)
                                 .withDeliveryId(0)
                                 .withDeliveryTag(new byte[] { 1 })
                                 .withMore(false)
                                 .withMessageFormat(0)
                                 .withPayload(payload).now();

            Wait.assertTrue("Should eventually be marked as completed", delivery::completed);

            peer.expectDetach().respond();
            peer.expectClose().respond();

            receiver.closeAsync();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testStreamDeliveryRawInputStreamWithCompleteDeliveryReadByte() throws Exception {
        final byte[] payload = createEncodedMessage(new AmqpValue<>("Hello World"));

        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.RECEIVER.getValue()).respond();
            peer.expectFlow();
            peer.remoteTransfer().withHandle(0)
                                 .withDeliveryId(0)
                                 .withDeliveryTag(new byte[] { 1 })
                                 .withMore(false)
                                 .withMessageFormat(0)
                                 .withPayload(payload).queue();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            final Client container = Client.create();
            final Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            final StreamReceiver receiver = connection.openStreamReceiver("test-queue");

            final StreamDelivery delivery = receiver.receive();

            assertNotNull(delivery);
            assertTrue(delivery.completed());
            assertFalse(delivery.aborted());

            final InputStream stream = delivery.rawInputStream();
            assertNotNull(stream);

            assertEquals(payload.length, stream.available());
            final byte[] deliveryBytes = new byte[payload.length];
            for (int i = 0; i < payload.length; ++i) {
                deliveryBytes[i] = (byte) stream.read();
            }

            assertArrayEquals(payload, deliveryBytes);
            assertEquals(0, stream.available());
            assertEquals(-1, stream.read());

            stream.close();

            peer.expectDetach().respond();
            peer.expectClose().respond();

            receiver.closeAsync();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testStreamDeliveryRawInputStreamWithCompleteDeliveryReadBytes() throws Exception {
        final byte[] payload = createEncodedMessage(new AmqpValue<>("Hello World"));

        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.RECEIVER.getValue()).respond();
            peer.expectFlow();
            peer.remoteTransfer().withHandle(0)
                                 .withDeliveryId(0)
                                 .withDeliveryTag(new byte[] { 1 })
                                 .withMore(false)
                                 .withMessageFormat(0)
                                 .withPayload(payload).queue();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            final Client container = Client.create();
            final Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            final StreamReceiver receiver = connection.openStreamReceiver("test-queue");

            final StreamDelivery delivery = receiver.receive();

            assertNotNull(delivery);
            assertTrue(delivery.completed());
            assertFalse(delivery.aborted());

            final InputStream stream = delivery.rawInputStream();
            assertNotNull(stream);

            assertEquals(payload.length, stream.available());
            final byte[] deliveryBytes = new byte[payload.length];
            stream.read(deliveryBytes);

            assertArrayEquals(payload, deliveryBytes);
            assertEquals(0, stream.available());
            assertEquals(-1, stream.read());

            stream.close();

            peer.expectDetach().respond();
            peer.expectClose().respond();

            receiver.closeAsync();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testStreamDeliveryRawInputStreamWithInCompleteDeliveryReadBytes() throws Exception {
        final byte[] payload1 = createEncodedMessage(new Data(new byte[] { 0, 1, 2, 3, 4, 5 }));
        final byte[] payload2 = createEncodedMessage(new Data(new byte[] { 6, 7, 8, 9, 0 ,1 }));

        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.RECEIVER.getValue()).respond();
            peer.expectFlow();
            peer.remoteTransfer().withHandle(0)
                                 .withDeliveryId(0)
                                 .withDeliveryTag(new byte[] { 1 })
                                 .withMore(true)
                                 .withMessageFormat(0)
                                 .withPayload(payload1).queue();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            final Client container = Client.create();
            final Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            final StreamReceiver receiver = connection.openStreamReceiver("test-queue");

            final StreamDelivery delivery = receiver.receive();

            assertNotNull(delivery);
            assertFalse(delivery.completed());
            assertFalse(delivery.aborted());

            final InputStream stream = delivery.rawInputStream();
            assertNotNull(stream);

            assertEquals(payload1.length, stream.available());
            final byte[] deliveryBytes1 = new byte[payload1.length];
            stream.read(deliveryBytes1);

            assertArrayEquals(payload1, deliveryBytes1);
            assertEquals(0, stream.available());

            peer.remoteTransfer().withHandle(0)
                                 .withDeliveryId(0)
                                 .withDeliveryTag(new byte[] { 1 })
                                 .withMore(false)
                                 .withPayload(payload2).later(50);

            // Should block until more data arrives.
            final byte[] deliveryBytes2 = new byte[payload2.length];
            stream.read(deliveryBytes2);

            assertArrayEquals(payload2, deliveryBytes2);
            assertEquals(0, stream.available());

            assertTrue(delivery.completed());
            assertFalse(delivery.aborted());

            stream.close();

            peer.expectDetach().respond();
            peer.expectClose().respond();

            receiver.closeAsync();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testStreamDeliveryRawInputStreamReadBytesSignalsEOFOnEmptyCompleteTransfer() throws Exception {
        final byte[] payload1 = createEncodedMessage(new Data(new byte[] { 0, 1, 2, 3, 4, 5 }));

        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.RECEIVER.getValue()).respond();
            peer.expectFlow();
            peer.remoteTransfer().withHandle(0)
                                 .withDeliveryId(0)
                                 .withDeliveryTag(new byte[] { 1 })
                                 .withMore(true)
                                 .withMessageFormat(0)
                                 .withPayload(payload1).queue();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            final Client container = Client.create();
            final Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            final StreamReceiver receiver = connection.openStreamReceiver("test-queue");

            final StreamDelivery delivery = receiver.receive();

            assertNotNull(delivery);
            assertFalse(delivery.completed());
            assertFalse(delivery.aborted());

            final InputStream stream = delivery.rawInputStream();
            assertNotNull(stream);

            assertEquals(payload1.length, stream.available());
            final byte[] deliveryBytes1 = new byte[payload1.length];
            stream.read(deliveryBytes1);

            assertArrayEquals(payload1, deliveryBytes1);
            assertEquals(0, stream.available());

            peer.remoteTransfer().withHandle(0)
                                 .withDeliveryId(0)
                                 .withDeliveryTag(new byte[] { 1 })
                                 .withMore(false)
                                 .later(50);

            // Should block until more data arrives.
            final byte[] deliveryBytes2 = new byte[payload1.length];
            assertEquals(-1, stream.read(deliveryBytes2));
            assertEquals(0, stream.available());

            assertTrue(delivery.completed());
            assertFalse(delivery.aborted());

            stream.close();

            peer.expectDetach().respond();
            peer.expectClose().respond();

            receiver.closeAsync();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testStreamDeliveryRawInputStreamWithInCompleteDeliverySkipBytes() throws Exception {
        final byte[] payload1 = createEncodedMessage(new Data(new byte[] { 0, 1, 2, 3, 4, 5 }));
        final byte[] payload2 = createEncodedMessage(new Data(new byte[] { 6, 7, 8, 9, 0 ,1 }));

        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.RECEIVER.getValue()).respond();
            peer.expectFlow();
            peer.remoteTransfer().withHandle(0)
                                 .withDeliveryId(0)
                                 .withDeliveryTag(new byte[] { 1 })
                                 .withMore(true)
                                 .withMessageFormat(0)
                                 .withPayload(payload1).queue();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            final Client container = Client.create();
            final Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            final StreamReceiver receiver = connection.openStreamReceiver("test-queue");

            final StreamDelivery delivery = receiver.receive();

            assertNotNull(delivery);
            assertFalse(delivery.completed());
            assertFalse(delivery.aborted());

            final InputStream stream = delivery.rawInputStream();
            assertNotNull(stream);

            assertEquals(payload1.length, stream.available());
            stream.skip(payload1.length);
            assertEquals(0, stream.available());

            peer.remoteTransfer().withHandle(0)
                                 .withDeliveryId(0)
                                 .withDeliveryTag(new byte[] { 1 })
                                 .withMore(false)
                                 .withPayload(payload2).later(50);

            // Should block until more data arrives.
            stream.skip(payload2.length);
            assertEquals(0, stream.available());

            assertTrue(delivery.completed());
            assertFalse(delivery.aborted());

            stream.close();

            peer.expectDetach().respond();
            peer.expectClose().respond();

            receiver.closeAsync();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testStreamDeliveryRawInputStreamBlockedReadBytesAborted() throws Exception {
        final byte[] payload = createEncodedMessage(new AmqpValue<>("Hello World"));

        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.RECEIVER.getValue()).respond();
            peer.expectFlow();
            peer.remoteTransfer().withHandle(0)
                                 .withDeliveryId(0)
                                 .withDeliveryTag(new byte[] { 1 })
                                 .withMore(true)
                                 .withMessageFormat(0)
                                 .withPayload(payload).queue();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            final Client container = Client.create();
            final Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            final StreamReceiver receiver = connection.openStreamReceiver("test-queue");

            final StreamDelivery delivery = receiver.receive();

            assertNotNull(delivery);
            assertFalse(delivery.completed());
            assertFalse(delivery.aborted());

            final InputStream stream = delivery.rawInputStream();
            assertNotNull(stream);

            assertEquals(payload.length, stream.available());
            final byte[] deliveryBytes = new byte[payload.length * 2];

            peer.remoteTransfer().withHandle(0)
                                 .withDeliveryId(0)
                                 .withDeliveryTag(new byte[] { 1 })
                                 .withMore(false)
                                 .withAborted(true)
                                 .withMessageFormat(0).later(50);

            try {
                stream.read(deliveryBytes);
                fail("Delivery should have been aborted while waiting for more data.");
            } catch (IOException ioe) {
                assertTrue(ioe.getCause() instanceof ClientDeliveryAbortedException);
            }

            stream.close();

            peer.expectDetach().respond();
            peer.expectClose().respond();

            receiver.closeAsync();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testStreamSupportsMark() throws Exception {
        final byte[] payload = createEncodedMessage(new Data(new byte[] { 0, 1, 2, 3, 4, 5 }));

        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.RECEIVER.getValue()).respond();
            peer.expectFlow();
            peer.remoteTransfer().withHandle(0)
                                 .withDeliveryId(0)
                                 .withDeliveryTag(new byte[] { 1 })
                                 .withMore(false)
                                 .withMessageFormat(0)
                                 .withPayload(payload).queue();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            final Client container = Client.create();
            final Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            final StreamReceiver receiver = connection.openStreamReceiver("test-queue");

            final StreamDelivery delivery = receiver.receive();

            assertNotNull(delivery);
            assertTrue(delivery.completed());
            assertFalse(delivery.aborted());

            final InputStream stream = delivery.rawInputStream();
            assertNotNull(stream);
            assertTrue(stream.markSupported());

            assertEquals(payload.length, stream.available());
            stream.mark(payload.length);

            final byte[] deliveryBytes1 = new byte[payload.length];
            final byte[] deliveryBytes2 = new byte[payload.length];
            stream.read(deliveryBytes1);
            stream.reset();
            stream.read(deliveryBytes2);

            assertNotSame(deliveryBytes1, deliveryBytes2);
            assertArrayEquals(payload, deliveryBytes1);
            assertArrayEquals(payload, deliveryBytes2);
            assertEquals(0, stream.available());

            assertTrue(delivery.completed());
            assertFalse(delivery.aborted());

            stream.close();

            peer.expectDetach().respond();
            peer.expectClose().respond();

            receiver.closeAsync();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testStreamMessageWithHeaderOnly() throws Exception {
        final byte[] payload = createEncodedMessage(new Header().setDurable(true));

        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.RECEIVER.getValue()).respond();
            peer.expectFlow();
            peer.remoteTransfer().withHandle(0)
                                 .withDeliveryId(0)
                                 .withDeliveryTag(new byte[] { 1 })
                                 .withMore(false)
                                 .withMessageFormat(0)
                                 .withPayload(payload).queue();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            final Client container = Client.create();
            final Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            final StreamReceiver receiver = connection.openStreamReceiver("test-queue");
            final StreamDelivery delivery = receiver.receive();

            assertNotNull(delivery);
            assertTrue(delivery.completed());
            assertFalse(delivery.aborted());

            StreamReceiverMessage message = delivery.message();
            assertNotNull(message);
            Header header = message.header();
            assertNotNull(header);

            peer.expectDetach().respond();
            peer.expectClose().respond();

            receiver.closeAsync();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }
}
