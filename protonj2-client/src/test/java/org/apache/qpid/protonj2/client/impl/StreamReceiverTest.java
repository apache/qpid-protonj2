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

import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.ConnectionOptions;
import org.apache.qpid.protonj2.client.DeliveryState;
import org.apache.qpid.protonj2.client.ErrorCondition;
import org.apache.qpid.protonj2.client.Receiver;
import org.apache.qpid.protonj2.client.ReceiverOptions;
import org.apache.qpid.protonj2.client.SenderOptions;
import org.apache.qpid.protonj2.client.StreamDelivery;
import org.apache.qpid.protonj2.client.StreamReceiver;
import org.apache.qpid.protonj2.client.StreamReceiverMessage;
import org.apache.qpid.protonj2.client.StreamReceiverOptions;
import org.apache.qpid.protonj2.client.exceptions.ClientDeliveryAbortedException;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.exceptions.ClientIllegalStateException;
import org.apache.qpid.protonj2.client.exceptions.ClientLinkRemotelyClosedException;
import org.apache.qpid.protonj2.client.exceptions.ClientOperationTimedOutException;
import org.apache.qpid.protonj2.client.exceptions.ClientUnsupportedOperationException;
import org.apache.qpid.protonj2.client.test.ImperativeClientTestCase;
import org.apache.qpid.protonj2.client.test.Wait;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.test.driver.ProtonTestServer;
import org.apache.qpid.protonj2.test.driver.codec.messaging.Accepted;
import org.apache.qpid.protonj2.test.driver.codec.messaging.DeliveryAnnotations;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedInteger;
import org.apache.qpid.protonj2.types.messaging.AmqpSequence;
import org.apache.qpid.protonj2.types.messaging.AmqpValue;
import org.apache.qpid.protonj2.types.messaging.ApplicationProperties;
import org.apache.qpid.protonj2.types.messaging.Data;
import org.apache.qpid.protonj2.types.messaging.Footer;
import org.apache.qpid.protonj2.types.messaging.Header;
import org.apache.qpid.protonj2.types.messaging.MessageAnnotations;
import org.apache.qpid.protonj2.types.messaging.Properties;
import org.apache.qpid.protonj2.types.transport.Role;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests the {@link StreamReceiver} implementation
 */
@Timeout(20)
class StreamReceiverTest extends ImperativeClientTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(StreamReceiverTest.class);

    @Test
    public void testCreateReceiverAndClose() throws Exception {
        doTestCreateReceiverAndCloseOrDetachLink(true);
    }

    @Test
    public void testCreateReceiverAndDetach() throws Exception {
        doTestCreateReceiverAndCloseOrDetachLink(false);
    }

    private void doTestCreateReceiverAndCloseOrDetachLink(boolean close) throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofReceiver().respond();
            peer.expectFlow().withLinkCredit(10);
            peer.expectDetach().withClosed(close).respond();
            peer.expectEnd().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            StreamReceiver receiver = connection.openStreamReceiver("test-queue");
            receiver.openFuture().get(10, TimeUnit.SECONDS);

            assertSame(container, receiver.client());
            assertSame(connection, receiver.connection());

            if (close) {
                receiver.closeAsync().get(10, TimeUnit.SECONDS);
            } else {
                receiver.detachAsync().get(10, TimeUnit.SECONDS);
            }

            connection.closeAsync().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testCreateReceiverAndCloseSync() throws Exception {
        doTestCreateReceiverAndCloseOrDetachSyncLink(true);
    }

    @Test
    public void testCreateReceiverAndDetachSync() throws Exception {
        doTestCreateReceiverAndCloseOrDetachSyncLink(false);
    }

    private void doTestCreateReceiverAndCloseOrDetachSyncLink(boolean close) throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofReceiver().respond();
            peer.expectFlow().withLinkCredit(10);
            peer.expectDetach().withClosed(close).respond();
            peer.expectEnd().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            StreamReceiver receiver = connection.openStreamReceiver("test-queue");
            receiver.openFuture().get(10, TimeUnit.SECONDS);

            if (close) {
                receiver.close();
            } else {
                receiver.detach();
            }

            connection.closeAsync().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testCreateReceiverAndCloseWithErrorSync() throws Exception {
        doTestCreateReceiverAndCloseOrDetachWithErrorSync(true);
    }

    @Test
    public void testCreateReceiverAndDetachWithErrorSync() throws Exception {
        doTestCreateReceiverAndCloseOrDetachWithErrorSync(false);
    }

    private void doTestCreateReceiverAndCloseOrDetachWithErrorSync(boolean close) throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofReceiver().respond();
            peer.expectFlow();
            peer.expectDetach().withError("amqp-resource-deleted", "an error message").withClosed(close).respond();
            peer.expectEnd().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Sender test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            StreamReceiver receiver = connection.openStreamReceiver("test-queue");
            receiver.openFuture().get(10, TimeUnit.SECONDS);

            if (close) {
                receiver.close(ErrorCondition.create("amqp-resource-deleted", "an error message", null));
            } else {
                receiver.detach(ErrorCondition.create("amqp-resource-deleted", "an error message", null));
            }

            connection.closeAsync().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testCreateReceiverAndCloseWithErrorAsync() throws Exception {
        doTestCreateReceiverAndCloseOrDetachWithErrorAsync(true);
    }

    @Test
    public void testCreateReceiverAndDetachWithErrorAsync() throws Exception {
        doTestCreateReceiverAndCloseOrDetachWithErrorAsync(false);
    }

    private void doTestCreateReceiverAndCloseOrDetachWithErrorAsync(boolean close) throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofReceiver().respond();
            peer.expectFlow();
            peer.expectDetach().withError("amqp-resource-deleted", "an error message").withClosed(close).respond();
            peer.expectEnd().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Sender test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            StreamReceiver receiver = connection.openStreamReceiver("test-queue");
            receiver.openFuture().get(10, TimeUnit.SECONDS);

            if (close) {
                receiver.closeAsync(ErrorCondition.create("amqp-resource-deleted", "an error message", null)).get();
            } else {
                receiver.detachAsync(ErrorCondition.create("amqp-resource-deleted", "an error message", null)).get();
            }

            connection.closeAsync().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

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

    @Test
    public void testStreamReceiverConfiguresSessionCapacityIdenticalToMaxFrameSize() throws Exception {
        // Read buffer is always halved by connection when creating new session for the stream
        // unless it falls at the max frame size value which means only one is possible, in this
        // case the user configured session window the same as than max frame size so only one
        // frame is possible.
        doTestStreamReceiverSessionCapacity(100_000, 100_000, 1);
    }

    @Test
    public void testStreamReceiverConfiguresSessionCapacityLowerThanMaxFrameSize() throws Exception {
        // Read buffer is always halved by connection when creating new session for the stream
        // unless it falls at the max frame size value which means only one is possible, in this
        // case the user configured session window lower than max frame size and the client auto
        // adjusts that to one frame.
        doTestStreamReceiverSessionCapacity(100_000, 50_000, 1);
    }

    private void doTestStreamReceiverSessionCapacity(int maxFrameSize, int readBufferSize, int expectedSessionWindow) throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().withMaxFrameSize(maxFrameSize).respond();
            peer.expectBegin().withIncomingWindow(expectedSessionWindow).respond();
            peer.expectAttach().ofReceiver().respond();
            peer.expectFlow().withIncomingWindow(expectedSessionWindow);
            peer.expectDetach().respond();
            peer.expectEnd().respond();
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
            receiver.closeAsync().get();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testOpenStreamReceiverWithLinCapabilities() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.RECEIVER.getValue())
                               .withSource().withCapabilities("queue")
                               .withDistributionMode(nullValue())
                               .and().respond();
            peer.expectFlow();
            peer.expectDetach().respond();
            peer.expectEnd().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("StreamReceiver test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            StreamReceiverOptions receiverOptions = new StreamReceiverOptions();
            receiverOptions.sourceOptions().capabilities("queue");
            StreamReceiver receiver = connection.openStreamReceiver("test-queue", receiverOptions);

            receiver.openFuture().get();

            assertSame(container, receiver.client());
            assertSame(connection, receiver.connection());

            receiver.close();

            connection.closeAsync().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testCreateStreamDeliveryWithoutAnyIncomingDeliveryPresent() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
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
            peer.expectEnd().respond();
            peer.expectClose().respond();

            receiver.close();
            connection.close();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testStreamReceiverAwaitTimedCanBePerformedMultipleTimes() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
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
            peer.expectEnd().respond();
            peer.expectClose().respond();

            receiver.close();
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
        try (ProtonTestServer peer = new ProtonTestServer()) {
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
            peer.expectEnd().respond();
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

        try (ProtonTestServer peer = new ProtonTestServer()) {
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
            peer.expectEnd().respond();
            peer.expectClose().respond();

            receiver.close();
            connection.close();

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

        try (ProtonTestServer peer = new ProtonTestServer()) {
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
            peer.expectEnd().respond();
            peer.expectClose().respond();

            receiver.close();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testStreamDeliveryReceivedWhileTransferIsIncomplete() throws Exception {
        final byte[] payload = createEncodedMessage(new AmqpValue<>("Hello World"));

        try (ProtonTestServer peer = new ProtonTestServer()) {
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
                                 .withNullDeliveryTag()
                                 .withMore(false)
                                 .withMessageFormat(0)
                                 .withPayload(payload).now();

            Wait.assertTrue("Should eventually be marked as completed", delivery::completed);

            peer.expectDetach().respond();
            peer.expectEnd().respond();
            peer.expectClose().respond();

            receiver.closeAsync().get();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testStreamDeliveryRawInputStreamWithCompleteDeliveryReadByte() throws Exception {
        final byte[] payload = createEncodedMessage(new AmqpValue<>("Hello World"));

        try (ProtonTestServer peer = new ProtonTestServer()) {
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
            peer.expectDisposition().withState().accepted().withSettled(true);
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
            peer.expectEnd().respond();
            peer.expectClose().respond();

            receiver.close();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testStreamDeliveryRawInputStreamBehaviorAfterStreamClosed() throws Exception {
        final byte[] payload = createEncodedMessage(new AmqpValue<>("Hello World"));

        try (ProtonTestServer peer = new ProtonTestServer()) {
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

            stream.close();

            final byte[] scratch = new byte[10];

            assertThrows(IOException.class, () -> stream.available());
            assertThrows(IOException.class, () -> stream.skip(1));
            assertThrows(IOException.class, () -> stream.read());
            assertThrows(IOException.class, () -> stream.read(scratch));
            assertThrows(IOException.class, () -> stream.read(scratch, 0, scratch.length));

            peer.expectDetach().respond();
            peer.expectEnd().respond();
            peer.expectClose().respond();

            receiver.close();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testStreamDeliveryRawInputStreamWithCompleteDeliveryReadBytes() throws Exception {
        final byte[] payload = createEncodedMessage(new AmqpValue<>("Hello World"));

        try (ProtonTestServer peer = new ProtonTestServer()) {
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
            peer.expectDisposition().withState().accepted().withSettled(true);
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
            peer.expectEnd().respond();
            peer.expectClose().respond();

            receiver.close();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testStreamDeliveryRawInputStreamWithInCompleteDeliveryReadBytes() throws Exception {
        final byte[] payload1 = createEncodedMessage(new Data(new byte[] { 0, 1, 2, 3, 4, 5 }));
        final byte[] payload2 = createEncodedMessage(new Data(new byte[] { 6, 7, 8, 9, 0 ,1 }));

        try (ProtonTestServer peer = new ProtonTestServer()) {
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
            peer.expectDisposition().withState().accepted().withSettled(true);
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
            peer.expectEnd().respond();
            peer.expectClose().respond();

            receiver.close();
            connection.close();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testStreamDeliveryRawInputStreamReadBytesSignalsEOFOnEmptyCompleteTransfer() throws Exception {
        final byte[] payload1 = createEncodedMessage(new Data(new byte[] { 0, 1, 2, 3, 4, 5 }));

        try (ProtonTestServer peer = new ProtonTestServer()) {
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
            peer.expectDisposition().withState().accepted().withSettled(true);
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
            peer.expectEnd().respond();
            peer.expectClose().respond();

            receiver.close();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testStreamDeliveryRawInputStreamWithInCompleteDeliverySkipBytes() throws Exception {
        final byte[] payload1 = createEncodedMessage(new Data(new byte[] { 0, 1, 2, 3, 4, 5 }));
        final byte[] payload2 = createEncodedMessage(new Data(new byte[] { 6, 7, 8, 9, 0 ,1 }));

        try (ProtonTestServer peer = new ProtonTestServer()) {
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
            peer.expectDisposition().withState().accepted().withSettled(true);
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
            peer.expectEnd().respond();
            peer.expectClose().respond();

            receiver.close();
            connection.close();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testStreamDeliveryRawInputStreamReadOpensSessionWindowForAdditionalInput() throws Exception {
        final byte[] body1 = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        final byte[] body2 = new byte[] { 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0 };
        final byte[] payload1 = createEncodedMessage(new Data(body1));
        final byte[] payload2 = createEncodedMessage(new Data(body2));

        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().withMaxFrameSize(1000).respond();
            peer.expectBegin().withIncomingWindow(1).respond();
            peer.expectAttach().ofReceiver().respond();
            peer.expectFlow().withIncomingWindow(1).withLinkCredit(10);
            peer.remoteTransfer().withHandle(0)
                                 .withDeliveryId(0)
                                 .withDeliveryTag(new byte[] { 1 })
                                 .withMore(true)
                                 .withMessageFormat(0)
                                 .withPayload(payload1).queue();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            ConnectionOptions connectionOptions = new ConnectionOptions().maxFrameSize(1000);
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), connectionOptions);
            StreamReceiverOptions streamOptions = new StreamReceiverOptions().readBufferSize(2000);
            StreamReceiver receiver = connection.openStreamReceiver("test-queue", streamOptions);
            StreamDelivery delivery = receiver.receive();
            assertNotNull(delivery);
            InputStream rawStream = delivery.rawInputStream();
            assertNotNull(rawStream);

            // An initial frame has arrived but more than that is requested so the first chuck is pulled
            // from the incoming delivery and the session window opens which allows the second chunk to
            // arrive and again the session window will be opened as that chunk is moved to the reader's
            // buffer for return from the read request.
            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectFlow().withDeliveryCount(0).withIncomingWindow(1).withLinkCredit(10);
            peer.remoteTransfer().withHandle(0)
                                 .withDeliveryId(0)
                                 .withMore(false)
                                 .withMessageFormat(0)
                                 .withPayload(payload2).queue();
            peer.expectFlow().withDeliveryCount(1).withIncomingWindow(1).withLinkCredit(9);
            peer.expectDisposition().withFirst(0).withState().accepted().withSettled(true);

            byte[] combinedPayloads = new byte[payload1.length + payload2.length];
            rawStream.read(combinedPayloads);

            assertTrue(Arrays.equals(payload1, 0, payload1.length, combinedPayloads, 0, payload1.length));
            assertTrue(Arrays.equals(payload2, 0, payload2.length, combinedPayloads, payload1.length, payload1.length + payload2.length));

            rawStream.close();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectDetach().respond();
            peer.expectEnd().respond();
            peer.expectClose().respond();

            receiver.openFuture().get();
            receiver.closeAsync().get();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testStreamDeliveryRawInputStreamBlockedReadBytesAborted() throws Exception {
        final byte[] payload = createEncodedMessage(new AmqpValue<>("Hello World"));

        try (ProtonTestServer peer = new ProtonTestServer()) {
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
            peer.expectEnd().respond();
            peer.expectClose().respond();

            receiver.closeAsync().get();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testStreamDeliveryRawInputStreamClosedWithoutReadsConsumesTransfers() throws Exception {
        final byte[] body1 = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        final byte[] body2 = new byte[] { 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0 };
        final byte[] payload1 = createEncodedMessage(new Data(body1));
        final byte[] payload2 = createEncodedMessage(new Data(body2));

        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().withMaxFrameSize(1000).respond();
            peer.expectBegin().withIncomingWindow(1).respond();
            peer.expectAttach().ofReceiver().respond();
            peer.expectFlow().withIncomingWindow(1).withLinkCredit(10);
            peer.remoteTransfer().withHandle(0)
                                 .withDeliveryId(0)
                                 .withDeliveryTag(new byte[] { 1 })
                                 .withMore(true)
                                 .withMessageFormat(0)
                                 .withPayload(payload1).queue();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            ConnectionOptions connectionOptions = new ConnectionOptions().maxFrameSize(1000);
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), connectionOptions);
            StreamReceiverOptions streamOptions = new StreamReceiverOptions().readBufferSize(2000);
            StreamReceiver receiver = connection.openStreamReceiver("test-queue", streamOptions);
            StreamDelivery delivery = receiver.receive();
            assertNotNull(delivery);
            InputStream rawStream = delivery.rawInputStream();
            assertNotNull(rawStream);

            // An initial frame has arrived but no reads have been performed and then if closed
            // the delivery will be consumed to allow the session window to be opened and prevent
            // a stall due to an un-consumed delivery.  The stream delivery will not auto accept
            // or auto settle the delivery as the user closed early which should indicate they
            // are rejecting the message otherwise it is a programming error on their part.
            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectFlow().withDeliveryCount(0).withIncomingWindow(1).withLinkCredit(10);
            peer.remoteTransfer().withHandle(0)
                                 .withDeliveryId(0)
                                 .withMore(false)
                                 .withMessageFormat(0)
                                 .withPayload(payload2).queue();
            peer.expectFlow().withDeliveryCount(1).withIncomingWindow(1).withLinkCredit(9);

            rawStream.close();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectDetach().respond();
            peer.expectEnd().respond();
            peer.expectClose().respond();

            receiver.openFuture().get();
            receiver.closeAsync().get();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testStreamDeliveryRawInputStreamClosedWithoutReadsAllowsUserDisposition() throws Exception {
        final byte[] body1 = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        final byte[] body2 = new byte[] { 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0 };
        final byte[] payload1 = createEncodedMessage(new Data(body1));
        final byte[] payload2 = createEncodedMessage(new Data(body2));

        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().withMaxFrameSize(1000).respond();
            peer.expectBegin().withIncomingWindow(1).respond();
            peer.expectAttach().ofReceiver().respond();
            peer.expectFlow().withIncomingWindow(1).withLinkCredit(10);
            peer.remoteTransfer().withHandle(0)
                                 .withDeliveryId(0)
                                 .withDeliveryTag(new byte[] { 1 })
                                 .withMore(true)
                                 .withMessageFormat(0)
                                 .withPayload(payload1).queue();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            ConnectionOptions connectionOptions = new ConnectionOptions().maxFrameSize(1000);
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), connectionOptions);
            StreamReceiverOptions streamOptions = new StreamReceiverOptions().readBufferSize(2000);
            StreamReceiver receiver = connection.openStreamReceiver("test-queue", streamOptions);
            StreamDelivery delivery = receiver.receive();
            assertNotNull(delivery);
            InputStream rawStream = delivery.rawInputStream();
            assertNotNull(rawStream);

            // An initial frame has arrived but no reads have been performed and then if closed
            // the delivery will be consumed to allow the session window to be opened and prevent
            // a stall due to an un-consumed delivery.  The stream delivery will not auto accept
            // or auto settle the delivery as the user closed early which should indicate they
            // are rejecting the message otherwise it is a programming error on their part.
            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectFlow().withDeliveryCount(0).withIncomingWindow(1).withLinkCredit(10);
            peer.remoteTransfer().withHandle(0)
                                 .withDeliveryId(0)
                                 .withMore(false)
                                 .withMessageFormat(0)
                                 .withPayload(payload2).queue();
            peer.expectFlow().withDeliveryCount(1).withIncomingWindow(1).withLinkCredit(9);

            rawStream.close();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectDisposition().withState().rejected("invalid-format", "decode error").withSettled(true);
            peer.expectDetach().respond();
            peer.expectEnd().respond();
            peer.expectClose().respond();

            delivery.disposition(new ClientDeliveryState.ClientRejected("invalid-format", "decode error"), true);

            receiver.openFuture().get();
            receiver.closeAsync().get();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testStreamDeliveryUserAppliedDispositionBeforeStreamRead() throws Exception {
        final byte[] payload = createEncodedMessage(new AmqpValue<>("Hello World"));

        try (ProtonTestServer peer = new ProtonTestServer()) {
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
            peer.expectDisposition().withState().accepted().withSettled(true);
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

            delivery.disposition(ClientDeliveryState.ClientAccepted.getInstance(), true);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

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
            peer.expectEnd().respond();
            peer.expectClose().respond();

            receiver.close();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testStreamSupportsMark() throws Exception {
        final byte[] payload = createEncodedMessage(new Data(new byte[] { 0, 1, 2, 3, 4, 5 }));

        try (ProtonTestServer peer = new ProtonTestServer()) {
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
            peer.expectDisposition().withState().accepted().withSettled(true);
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
            peer.expectEnd().respond();
            peer.expectClose().respond();

            receiver.close();
            connection.close();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testStreamMessageWithHeaderOnly() throws Exception {
        final byte[] payload = createEncodedMessage(new Header().setDurable(true));

        try (ProtonTestServer peer = new ProtonTestServer()) {
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
            peer.expectDisposition().withState().accepted().withSettled(true);
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

            assertSame(receiver, message.receiver());
            assertSame(delivery, message.delivery());

            assertNull(message.properties());
            assertNull(message.annotations());
            assertNull(message.applicationProperties());
            assertNull(message.footer());
            assertTrue(message.completed());

            peer.expectDetach().respond();
            peer.expectEnd().respond();
            peer.expectClose().respond();

            receiver.close();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testReadHeaderFromStreamMessageWithoutHeaderSection() throws Exception {
        Map<Symbol, Object> annotationsMap = new HashMap<>();
        annotationsMap.put(Symbol.valueOf("test-1"), UUID.randomUUID());
        annotationsMap.put(Symbol.valueOf("test-2"), UUID.randomUUID());
        annotationsMap.put(Symbol.valueOf("test-3"), UUID.randomUUID());

        final byte[] payload = createEncodedMessage(new MessageAnnotations(annotationsMap));

        try (ProtonTestServer peer = new ProtonTestServer()) {
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
            assertNull(header);
            MessageAnnotations annotations = message.annotations();
            assertNotNull(annotations);
            assertEquals(annotationsMap, annotations.getValue());

            peer.expectDetach().respond();
            peer.expectEnd().respond();
            peer.expectClose().respond();

            receiver.closeAsync().get();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testTryReadSectionBeyondWhatIsEncodedIntoMessage() throws Exception {
        Map<Symbol, Object> annotationsMap = new HashMap<>();
        annotationsMap.put(Symbol.valueOf("test-1"), UUID.randomUUID());
        annotationsMap.put(Symbol.valueOf("test-2"), UUID.randomUUID());
        annotationsMap.put(Symbol.valueOf("test-3"), UUID.randomUUID());

        final byte[] payload = createEncodedMessage(new Header(), new MessageAnnotations(annotationsMap));

        try (ProtonTestServer peer = new ProtonTestServer()) {
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
            peer.expectDisposition().withFirst(0).withState().accepted().withSettled(true);
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

            Properties properties = message.properties();
            assertNull(properties);
            Header header = message.header();
            assertNotNull(header);
            MessageAnnotations annotations = message.annotations();
            assertNotNull(annotations);
            assertEquals(annotationsMap, annotations.getValue());

            peer.expectDetach().respond();
            peer.expectEnd().respond();
            peer.expectClose().respond();

            receiver.closeAsync().get();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testReadBytesFromBodyInputStreamUsingReadByteAPI() throws Exception {
        final byte[] body = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        final byte[] payload = createEncodedMessage(new Data(body));

        try (ProtonTestServer peer = new ProtonTestServer()) {
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
            peer.expectDisposition().withFirst(0).withState().accepted().withSettled(true);
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

            InputStream bodyStream = message.body();
            assertNotNull(bodyStream);

            assertNull(message.header());
            assertNull(message.annotations());
            assertNull(message.properties());
            assertNull(delivery.annotations());

            final byte[] receivedBody = new byte[body.length];
            for (int i = 0; i < body.length; ++i) {
                receivedBody[i] = (byte) bodyStream.read();
            }
            assertArrayEquals(body, receivedBody);
            assertEquals(-1, bodyStream.read());
            assertNull(message.footer());

            peer.expectDetach().respond();
            peer.expectEnd().respond();
            peer.expectClose().respond();

            receiver.closeAsync().get();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testReadBytesFromInputStreamUsingReadByteWithSingleByteSplitTransfers() throws Exception {
        testReadBytesFromBodyInputStreamWithSplitSingleByteTransfers(1);
    }

    @Test
    public void testReadBytesFromInputStreamUsingSingleReadBytesWithSingleByteSplitTransfers() throws Exception {
        testReadBytesFromBodyInputStreamWithSplitSingleByteTransfers(2);
    }

    @Test
    public void testSkipBytesFromInputStreamWithSingleByteSplitTransfers() throws Exception {
        testReadBytesFromBodyInputStreamWithSplitSingleByteTransfers(3);
    }

    private void testReadBytesFromBodyInputStreamWithSplitSingleByteTransfers(int option) throws Exception {
        final byte[] body = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        final byte[] payload = createEncodedMessage(new Data(body));

        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.RECEIVER.getValue()).respond();
            peer.expectFlow();
            for (int i = 0; i < payload.length; ++i) {
                peer.remoteTransfer().withHandle(0)
                                     .withDeliveryId(0)
                                     .withDeliveryTag(new byte[] { 1 })
                                     .withMore(true)
                                     .withMessageFormat(0)
                                     .withPayload(new byte[] { payload[i] }).afterDelay(3).queue();
            }
            peer.remoteTransfer().withHandle(0)
                                 .withDeliveryId(0)
                                 .withDeliveryTag(new byte[] { 1 })
                                 .withMore(false)
                                 .withMessageFormat(0).afterDelay(5).queue();
            peer.expectDisposition().withFirst(0).withSettled(true);
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            final Client container = Client.create();
            final Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            final StreamReceiver receiver = connection.openStreamReceiver("test-queue");
            final StreamDelivery delivery = receiver.receive();
            final StreamReceiverMessage message = delivery.message();
            final InputStream bodyStream = message.body();

            final byte[] receivedBody = new byte[body.length];

            if (option == 1) {
                for (int i = 0; i < body.length; ++i) {
                    receivedBody[i] = (byte) bodyStream.read();
                }
                assertArrayEquals(body, receivedBody);
            } else if (option == 2) {
                assertEquals(body.length, bodyStream.read(receivedBody));
                assertArrayEquals(body, receivedBody);
            } else if (option == 3) {
                assertEquals(body.length, bodyStream.skip(body.length));
            } else {
                fail("Unknown test option");
            }

            bodyStream.close();

            peer.expectDetach().respond();
            peer.expectEnd().respond();
            peer.expectClose().respond();

            receiver.close();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testStreamReceiverSessionCannotCreateNewResources() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
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

            assertThrows(ClientUnsupportedOperationException.class, () -> receiver.session().openReceiver("test"));
            assertThrows(ClientUnsupportedOperationException.class, () -> receiver.session().openReceiver("test", new ReceiverOptions()));
            assertThrows(ClientUnsupportedOperationException.class, () -> receiver.session().openDurableReceiver("test", "test"));
            assertThrows(ClientUnsupportedOperationException.class, () -> receiver.session().openDurableReceiver("test", "test", new ReceiverOptions()));
            assertThrows(ClientUnsupportedOperationException.class, () -> receiver.session().openDynamicReceiver());
            assertThrows(ClientUnsupportedOperationException.class, () -> receiver.session().openDynamicReceiver(new HashMap<>()));
            assertThrows(ClientUnsupportedOperationException.class, () -> receiver.session().openDynamicReceiver(new ReceiverOptions()));
            assertThrows(ClientUnsupportedOperationException.class, () -> receiver.session().openDynamicReceiver(new HashMap<>(), new ReceiverOptions()));
            assertThrows(ClientUnsupportedOperationException.class, () -> receiver.session().openSender("test"));
            assertThrows(ClientUnsupportedOperationException.class, () -> receiver.session().openSender("test", new SenderOptions()));
            assertThrows(ClientUnsupportedOperationException.class, () -> receiver.session().openAnonymousSender());
            assertThrows(ClientUnsupportedOperationException.class, () -> receiver.session().openAnonymousSender(new SenderOptions()));

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            peer.expectDetach().respond();
            peer.expectEnd().respond();
            peer.expectClose().respond();

            receiver.close();
            connection.close();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testReadByteArrayPayloadInChunksFromSingleTransferMessage() throws Exception {
        testReadPayloadInChunksFromLargerMessage(false);
    }

    @Test
    public void testReadBytesWithArgsPayloadInChunksFromSingleTransferMessage() throws Exception {
        testReadPayloadInChunksFromLargerMessage(true);
    }

    private void testReadPayloadInChunksFromLargerMessage(boolean readWithArgs) throws Exception {
        final byte[] body = new byte[100];
        final Random random = new Random();
        random.setSeed(System.currentTimeMillis());
        random.nextBytes(body);
        final byte[] payload = createEncodedMessage(new Data(body));

        try (ProtonTestServer peer = new ProtonTestServer()) {
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
            peer.expectDisposition().withFirst(0).withState().accepted().withSettled(true);
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
            assertEquals(0, delivery.messageFormat());

            StreamReceiverMessage message = delivery.message();
            assertNotNull(message);

            InputStream bodyStream = message.body();
            assertNotNull(bodyStream);

            assertThrows(ClientUnsupportedOperationException.class, () -> message.messageFormat(1));
            assertNull(message.header());
            assertNull(message.annotations());
            assertNull(message.properties());
            assertNull(delivery.annotations());

            final byte[] aggregateBody = new byte[body.length];
            final byte[] receivedBody = new byte[10];

            for (int i = 0; i < body.length; i += 10) {
                if (readWithArgs) {
                    bodyStream.read(receivedBody, 0, receivedBody.length);
                } else {
                    bodyStream.read(receivedBody);
                }

                System.arraycopy(receivedBody, 0, aggregateBody, i, receivedBody.length);
            }

            assertArrayEquals(body, aggregateBody);
            assertEquals(-1, bodyStream.read(receivedBody, 0, receivedBody.length));
            assertEquals(-1, bodyStream.read(receivedBody));
            assertEquals(-1, bodyStream.read());
            assertNull(message.footer());

            peer.expectDetach().respond();
            peer.expectEnd().respond();
            peer.expectClose().respond();

            receiver.closeAsync().get();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testStreamReceiverMessageThrowsOnAnyMessageModificationAPI() throws Exception {
        final byte[] body = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        final byte[] payload = createEncodedMessage(new Data(body));

        try (ProtonTestServer peer = new ProtonTestServer()) {
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
            peer.expectDisposition().withFirst(0).withState().accepted().withSettled(true);
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            final Client container = Client.create();
            final Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            final StreamReceiver receiver = connection.openStreamReceiver("test-queue");
            final StreamDelivery delivery = receiver.receive();
            final StreamReceiverMessage message = delivery.message();

            assertThrows(ClientUnsupportedOperationException.class, () -> message.header(new Header()));
            assertThrows(ClientUnsupportedOperationException.class, () -> message.properties(new Properties()));
            assertThrows(ClientUnsupportedOperationException.class, () -> message.applicationProperties(new ApplicationProperties(null)));
            assertThrows(ClientUnsupportedOperationException.class, () -> message.annotations(new MessageAnnotations(null)));
            assertThrows(ClientUnsupportedOperationException.class, () -> message.footer(new Footer(null)));

            assertThrows(ClientUnsupportedOperationException.class, () -> message.messageFormat(1));
            assertThrows(ClientUnsupportedOperationException.class, () -> message.durable(true));
            assertThrows(ClientUnsupportedOperationException.class, () -> message.priority((byte) 4));
            assertThrows(ClientUnsupportedOperationException.class, () -> message.timeToLive(128));
            assertThrows(ClientUnsupportedOperationException.class, () -> message.firstAcquirer(false));
            assertThrows(ClientUnsupportedOperationException.class, () -> message.deliveryCount(10));
            assertThrows(ClientUnsupportedOperationException.class, () -> message.messageId(10));
            assertThrows(ClientUnsupportedOperationException.class, () -> message.correlationId(10));
            assertThrows(ClientUnsupportedOperationException.class, () -> message.userId(new byte[] {1}));
            assertThrows(ClientUnsupportedOperationException.class, () -> message.to("test"));
            assertThrows(ClientUnsupportedOperationException.class, () -> message.subject("test"));
            assertThrows(ClientUnsupportedOperationException.class, () -> message.replyTo("test"));
            assertThrows(ClientUnsupportedOperationException.class, () -> message.contentType("test"));
            assertThrows(ClientUnsupportedOperationException.class, () -> message.contentEncoding("test"));
            assertThrows(ClientUnsupportedOperationException.class, () -> message.absoluteExpiryTime(10));
            assertThrows(ClientUnsupportedOperationException.class, () -> message.creationTime(10));
            assertThrows(ClientUnsupportedOperationException.class, () -> message.groupId("test"));
            assertThrows(ClientUnsupportedOperationException.class, () -> message.groupSequence(10));
            assertThrows(ClientUnsupportedOperationException.class, () -> message.replyToGroupId("test"));

            assertThrows(ClientUnsupportedOperationException.class, () -> message.annotation("test", 1));
            assertThrows(ClientUnsupportedOperationException.class, () -> message.removeAnnotation("test"));
            assertThrows(ClientUnsupportedOperationException.class, () -> message.property("test", 1));
            assertThrows(ClientUnsupportedOperationException.class, () -> message.removeProperty("test"));
            assertThrows(ClientUnsupportedOperationException.class, () -> message.footer("test", 1));
            assertThrows(ClientUnsupportedOperationException.class, () -> message.removeFooter("test"));

            assertThrows(ClientUnsupportedOperationException.class, () -> message.body(InputStream.nullInputStream()));
            assertThrows(ClientUnsupportedOperationException.class, () -> message.addBodySection(new AmqpValue<>("test")));
            assertThrows(ClientUnsupportedOperationException.class, () -> message.bodySections(Collections.emptyList()));
            assertThrows(ClientUnsupportedOperationException.class, () -> message.bodySections());
            assertThrows(ClientUnsupportedOperationException.class, () -> message.clearBodySections());
            assertThrows(ClientUnsupportedOperationException.class, () -> message.forEachBodySection((section) -> {}));
            assertThrows(ClientUnsupportedOperationException.class, () -> message.encode(Collections.emptyMap()));

            InputStream bodyStream = message.body();

            assertNotNull(bodyStream.readAllBytes());
            bodyStream.close();

            peer.expectDetach().respond();
            peer.expectEnd().respond();
            peer.expectClose().respond();

            receiver.closeAsync().get();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSkipPayloadInChunksFromSingleTransferMessage() throws Exception {
        final byte[] body = new byte[100];
        final Random random = new Random();
        random.setSeed(System.currentTimeMillis());
        random.nextBytes(body);
        final byte[] payload = createEncodedMessage(new Data(body));

        try (ProtonTestServer peer = new ProtonTestServer()) {
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
            peer.expectDisposition().withFirst(0).withState().accepted().withSettled(true);
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

            InputStream bodyStream = message.body();
            assertNotNull(bodyStream);

            assertNull(message.header());
            assertNull(message.annotations());
            assertNull(message.properties());
            assertNull(delivery.annotations());

            final int skipSize = 10;

            for (int i = 0; i < body.length; i += skipSize) {
                bodyStream.skip(10);
            }

            final byte[] scratchBuffer = new byte[10];

            assertEquals(-1, bodyStream.read(scratchBuffer, 0, scratchBuffer.length));
            assertEquals(-1, bodyStream.read(scratchBuffer));
            assertEquals(-1, bodyStream.read());
            assertNull(message.footer());

            peer.expectDetach().respond();
            peer.expectEnd().respond();
            peer.expectClose().respond();

            receiver.closeAsync().get();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testReadByteArrayPayloadInChunksFromMultipleTransfersMessage() throws Exception {
        testReadPayloadInChunksFromLargerMultiTransferMessage(false);
    }

    @Test
    public void testReadBytesWithArgsPayloadInChunksFromMultipleTransferMessage() throws Exception {
        testReadPayloadInChunksFromLargerMultiTransferMessage(true);
    }

    private void testReadPayloadInChunksFromLargerMultiTransferMessage(boolean readWithArgs) throws Exception {
        final Random random = new Random();
        final long seed = System.currentTimeMillis();
        final int numChunks = 4;
        final int chunkSize = 30;

        random.setSeed(seed);

        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.RECEIVER.getValue()).respond();
            peer.expectFlow();
            for (int i = 0; i < numChunks; ++i) {
                final byte[] chunk = new byte[chunkSize];
                random.nextBytes(chunk);
                peer.remoteTransfer().withHandle(0)
                                     .withDeliveryId(0)
                                     .withDeliveryTag(new byte[] { 1 })
                                     .withMore(true)
                                     .withMessageFormat(0)
                                     .withPayload(createEncodedMessage(new Data(chunk))).queue();
            }
            peer.remoteTransfer().withHandle(0).withMore(false).queue();
            peer.expectDisposition().withFirst(0).withState().accepted().withSettled(true);
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            final Client container = Client.create();
            final Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            final StreamReceiver receiver = connection.openStreamReceiver("test-queue");
            final StreamDelivery delivery = receiver.receive();

            assertNotNull(delivery);

            StreamReceiverMessage message = delivery.message();
            assertNotNull(message);

            InputStream bodyStream = message.body();
            assertNotNull(bodyStream);

            assertNull(message.header());
            assertNull(message.annotations());
            assertNull(message.properties());
            assertNull(delivery.annotations());

            final byte[] readChunk = new byte[chunkSize];
            final byte[] receivedBody = new byte[3];

            random.setSeed(seed);

            int totalBytesRead = 0;

            for (int i = 0; i < numChunks; ++i) {
                for (int j = 0; j < readChunk.length; j += receivedBody.length) {
                    int bytesRead = 0;
                    if (readWithArgs) {
                        bytesRead = bodyStream.read(receivedBody, 0, receivedBody.length);
                    } else {
                        bytesRead = bodyStream.read(receivedBody);
                    }

                    totalBytesRead += bytesRead;

                    System.arraycopy(receivedBody, 0, readChunk, j, bytesRead);
                }

                final byte[] chunk = new byte[chunkSize];
                random.nextBytes(chunk);
                assertArrayEquals(chunk, readChunk);
            }

            assertEquals(chunkSize * numChunks, totalBytesRead);
            assertEquals(-1, bodyStream.read(receivedBody, 0, receivedBody.length));
            assertEquals(-1, bodyStream.read(receivedBody));
            assertEquals(-1, bodyStream.read());
            assertNull(message.footer());

            peer.expectDetach().respond();
            peer.expectEnd().respond();
            peer.expectClose().respond();

            receiver.closeAsync().get();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testReadPayloadFromSplitFrameTransferWithBufferLargerThanTotalPayload() throws Exception {
        final Random random = new Random();
        final long seed = System.currentTimeMillis();
        final int numChunks = 4;
        final int chunkSize = 30;

        random.setSeed(seed);

        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.RECEIVER.getValue()).respond();
            peer.expectFlow();
            for (int i = 0; i < numChunks; ++i) {
                final byte[] chunk = new byte[chunkSize];
                random.nextBytes(chunk);
                peer.remoteTransfer().withHandle(0)
                                     .withDeliveryId(0)
                                     .withDeliveryTag(new byte[] { 1 })
                                     .withMore(true)
                                     .withMessageFormat(0)
                                     .withPayload(createEncodedMessage(new Data(chunk))).queue();
            }
            peer.remoteTransfer().withHandle(0).withMore(false).queue();
            peer.expectDisposition().withFirst(0).withState().accepted().withSettled(true);
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            final Client container = Client.create();
            final Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            final StreamReceiver receiver = connection.openStreamReceiver("test-queue");
            final StreamDelivery delivery = receiver.receive();

            assertNotNull(delivery);

            StreamReceiverMessage message = delivery.message();
            assertNotNull(message);

            InputStream bodyStream = message.body();
            assertNotNull(bodyStream);

            assertNull(message.header());
            assertNull(message.annotations());
            assertNull(message.properties());
            assertNull(delivery.annotations());

            final byte[] receivedBody = new byte[(chunkSize * numChunks) + 100];
            Arrays.fill(receivedBody, (byte) 0);
            final int totalBytesRead = bodyStream.read(receivedBody);

            assertEquals(chunkSize * numChunks, totalBytesRead);
            assertEquals(-1, bodyStream.read(receivedBody, 0, receivedBody.length));
            assertEquals(-1, bodyStream.read(receivedBody));
            assertEquals(-1, bodyStream.read());
            assertNull(message.footer());

            // Regenerate what should have been sent plus empty trailing section to
            // check that the read doesn't write anything into the area we gave beyond
            // what was expected payload size.
            random.setSeed(seed);
            final byte[] regeneratedPayload = new byte[numChunks * chunkSize + 100];
            Arrays.fill(regeneratedPayload, (byte) 0);
            for (int i = 0; i < numChunks; ++i) {
                final byte[] chunk = new byte[chunkSize];
                random.nextBytes(chunk);
                System.arraycopy(chunk, 0, regeneratedPayload, chunkSize * i, chunkSize);
            }

            assertArrayEquals(regeneratedPayload, receivedBody);

            peer.expectDetach().respond();
            peer.expectEnd().respond();
            peer.expectClose().respond();

            receiver.closeAsync().get();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testStreamReadOpensSessionWindowForAdditionalInput() throws Exception {
        final byte[] body1 = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        final byte[] body2 = new byte[] { 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0 };
        final byte[] payload1 = createEncodedMessage(new Data(body1));
        final byte[] payload2 = createEncodedMessage(new Data(body2));

        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().withMaxFrameSize(1000).respond();
            peer.expectBegin().withIncomingWindow(1).respond();
            peer.expectAttach().ofReceiver().respond();
            peer.expectFlow().withIncomingWindow(1).withLinkCredit(10);
            peer.remoteTransfer().withHandle(0)
                                 .withDeliveryId(0)
                                 .withDeliveryTag(new byte[] { 1 })
                                 .withMore(true)
                                 .withMessageFormat(0)
                                 .withPayload(payload1).queue();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            ConnectionOptions connectionOptions = new ConnectionOptions().maxFrameSize(1000);
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), connectionOptions);
            StreamReceiverOptions streamOptions = new StreamReceiverOptions().readBufferSize(2000);
            StreamReceiver receiver = connection.openStreamReceiver("test-queue", streamOptions);
            StreamDelivery delivery = receiver.receive();
            assertNotNull(delivery);
            StreamReceiverMessage message = delivery.message();
            assertNotNull(message);

            // Creating the input stream instance should read the first chunk of data from the incoming
            // delivery which should result in a new credit being available to expand the session window.
            // An additional transfer should be placed into the delivery buffer but not yet read since
            // the user hasn't read anything.
            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectFlow().withDeliveryCount(0).withIncomingWindow(1).withLinkCredit(10);
            peer.remoteTransfer().withHandle(0)
                                 .withDeliveryId(0)
                                 .withMore(false)
                                 .withMessageFormat(0)
                                 .withPayload(payload2).queue();

            InputStream bodyStream = message.body();
            assertNotNull(bodyStream);

            // Once the read of all data completes the session window should be opened and the
            // stream should mark the delivery as accepted and settled since we are in auto settle
            // mode and there is nothing more to read.
            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectFlow().withDeliveryCount(1).withIncomingWindow(1).withLinkCredit(9);
            peer.expectDisposition().withFirst(0).withState().accepted().withSettled(true);

            byte[] combinedPayloads = new byte[body1.length + body2.length];
            bodyStream.read(combinedPayloads);

            assertTrue(Arrays.equals(body1, 0, body1.length, combinedPayloads, 0, body1.length));
            assertTrue(Arrays.equals(body2, 0, body2.length, combinedPayloads, body1.length, body1.length + body2.length));

            bodyStream.close();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectDetach().respond();
            peer.expectEnd().respond();
            peer.expectClose().respond();

            receiver.openFuture().get();
            receiver.closeAsync().get();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testStreamReadOpensSessionWindowForAdditionalInputAndGrantsCreditOnClose() throws Exception {
        final byte[] body1 = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        final byte[] body2 = new byte[] { 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0 };
        final byte[] payload1 = createEncodedMessage(new Data(body1));
        final byte[] payload2 = createEncodedMessage(new Data(body2));

        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().withMaxFrameSize(1000).respond();
            peer.expectBegin().withIncomingWindow(1).respond();
            peer.expectAttach().ofReceiver().respond();
            peer.expectFlow().withIncomingWindow(1).withLinkCredit(1);
            peer.remoteTransfer().withHandle(0)
                                 .withDeliveryId(0)
                                 .withDeliveryTag(new byte[] { 1 })
                                 .withMore(true)
                                 .withMessageFormat(0)
                                 .withPayload(payload1).queue();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            ConnectionOptions connectionOptions = new ConnectionOptions().maxFrameSize(1000);
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), connectionOptions);
            StreamReceiverOptions streamOptions = new StreamReceiverOptions().readBufferSize(2000).creditWindow(1);
            StreamReceiver receiver = connection.openStreamReceiver("test-queue", streamOptions);
            StreamDelivery delivery = receiver.receive();
            assertNotNull(delivery);
            StreamReceiverMessage message = delivery.message();
            assertNotNull(message);

            // Creating the input stream instance should read the first chunk of data from the incoming
            // delivery which should result in a new credit being available to expand the session window.
            // An additional transfer should be placed into the delivery buffer but not yet read since
            // the user hasn't read anything.
            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectFlow().withDeliveryCount(0).withIncomingWindow(1).withLinkCredit(1);
            peer.remoteTransfer().withHandle(0)
                                 .withDeliveryId(0)
                                 .withMore(false)
                                 .withMessageFormat(0)
                                 .withPayload(payload2).queue();

            InputStream bodyStream = message.body();
            assertNotNull(bodyStream);

            // Once the read of all data completes the session window should be opened and the
            // stream should mark the delivery as accepted and settled since we are in auto settle
            // mode and there is nothing more to read.
            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectFlow().withDeliveryCount(1).withIncomingWindow(1).withLinkCredit(0);
            peer.expectDisposition().withFirst(0).withState().accepted().withSettled(true);
            peer.expectFlow().withDeliveryCount(1).withIncomingWindow(1).withLinkCredit(1);

            byte[] combinedPayloads = new byte[body1.length + body2.length];
            bodyStream.read(combinedPayloads);

            assertTrue(Arrays.equals(body1, 0, body1.length, combinedPayloads, 0, body1.length));
            assertTrue(Arrays.equals(body2, 0, body2.length, combinedPayloads, body1.length, body1.length + body2.length));

            bodyStream.close();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectDetach().respond();
            peer.expectEnd().respond();
            peer.expectClose().respond();

            receiver.openFuture().get();
            receiver.closeAsync().get();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testStreamReadOfAllPayloadConsumesTrailingFooterOnClose() throws Exception {
        final byte[] body1 = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        final byte[] body2 = new byte[] { 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0 };
        final byte[] payload1 = createEncodedMessage(new Data(body1));
        final byte[] payload2 = createEncodedMessage(new Data(body2));
        final Footer footers = new Footer(new HashMap<>());
        footers.getValue().put(Symbol.valueOf("footer-key"), "test");
        final byte[] payload3 = createEncodedMessage(footers);

        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().withMaxFrameSize(1000).respond();
            peer.expectBegin().withIncomingWindow(1).respond();
            peer.expectAttach().ofReceiver().respond();
            peer.expectFlow().withIncomingWindow(1).withLinkCredit(10);
            peer.remoteTransfer().withHandle(0)
                                 .withDeliveryId(0)
                                 .withDeliveryTag(new byte[] { 1 })
                                 .withMore(true)
                                 .withMessageFormat(0)
                                 .withPayload(payload1).queue();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            ConnectionOptions connectionOptions = new ConnectionOptions().maxFrameSize(1000);
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), connectionOptions);
            StreamReceiverOptions streamOptions = new StreamReceiverOptions().readBufferSize(2000);
            StreamReceiver receiver = connection.openStreamReceiver("test-queue", streamOptions);
            StreamDelivery delivery = receiver.receive();
            assertNotNull(delivery);
            StreamReceiverMessage message = delivery.message();
            assertNotNull(message);

            // Creating the input stream instance should read the first chunk of data from the incoming
            // delivery which should result in a new credit being available to expand the session window.
            // An additional transfer should be placed into the delivery buffer but not yet read since
            // the user hasn't read anything.
            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectFlow().withDeliveryCount(0).withIncomingWindow(1).withLinkCredit(10);
            peer.remoteTransfer().withHandle(0)
                                 .withDeliveryId(0)
                                 .withMore(true)
                                 .withMessageFormat(0)
                                 .withPayload(payload2).queue();

            InputStream bodyStream = message.body();
            assertNotNull(bodyStream);

            // Once the read of all data completes the session window should be opened and the
            // stream should mark the delivery as accepted and settled since we are in auto settle
            // mode and there is nothing more to read.
            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectFlow().withDeliveryCount(0).withIncomingWindow(1).withLinkCredit(10);
            peer.remoteTransfer().withHandle(0)
                                 .withDeliveryId(0)
                                 .withMore(false)
                                 .withMessageFormat(0)
                                 .withPayload(payload3).queue();
            peer.expectFlow().withDeliveryCount(1).withIncomingWindow(1).withLinkCredit(9);
            peer.expectDisposition().withFirst(0).withState().accepted().withSettled(true);

            byte[] combinedPayloads = new byte[body1.length + body2.length];
            bodyStream.read(combinedPayloads);

            assertTrue(Arrays.equals(body1, 0, body1.length, combinedPayloads, 0, body1.length));
            assertTrue(Arrays.equals(body2, 0, body2.length, combinedPayloads, body1.length, body1.length + body2.length));

            bodyStream.close();

            Footer footer = message.footer();
            assertNotNull(footer);
            assertFalse(footer.getValue().isEmpty());
            assertTrue(footer.getValue().containsKey(Symbol.valueOf("footer-key")));

            assertTrue(message.hasFooters());
            assertTrue(message.hasFooter("footer-key"));
            message.forEachFooter((key, value) -> {
                assertEquals(key, "footer-key");
                assertEquals(value, "test");
            });

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectDetach().respond();
            peer.expectEnd().respond();
            peer.expectClose().respond();

            receiver.openFuture().get();
            receiver.closeAsync().get();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testReadBytesFromBodyInputStreamWithinTransactedSession() throws Exception {
        final byte[] body = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        final byte[] payload = createEncodedMessage(new Data(body));
        final byte[] txnId = new byte[] { 0, 1, 2, 3 };

        try (ProtonTestServer peer = new ProtonTestServer()) {
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
            peer.expectCoordinatorAttach().respond();
            peer.remoteFlow().withLinkCredit(2).queue();
            peer.expectDeclare().accept(txnId);
            peer.expectDisposition().withSettled(true).withState().transactional().withTxnId(txnId).withAccepted();
            peer.expectDischarge().withFail(false).withTxnId(txnId).accept();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            final Client container = Client.create();
            final Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            final StreamReceiver receiver = connection.openStreamReceiver("test-queue");
            final StreamDelivery delivery = receiver.receive();

            receiver.session().beginTransaction();

            assertNotNull(delivery);
            assertTrue(delivery.completed());
            assertFalse(delivery.aborted());

            StreamReceiverMessage message = delivery.message();
            assertNotNull(message);

            InputStream bodyStream = message.body();
            assertNotNull(bodyStream);

            assertNull(message.header());
            assertNull(message.annotations());
            assertNull(message.properties());
            assertNull(delivery.annotations());

            final byte[] receivedBody = new byte[body.length];
            for (int i = 0; i < body.length; ++i) {
                receivedBody[i] = (byte) bodyStream.read();
            }
            assertArrayEquals(body, receivedBody);
            assertEquals(-1, bodyStream.read());

            receiver.session().commitTransaction();

            peer.expectDetach().respond();
            peer.expectEnd().respond();
            peer.expectClose().respond();

            receiver.closeAsync().get();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testStreamDeliveryHandlesInvalidHeaderEncoding() throws Exception {
        final byte[] payload = createInvalidHeaderEncoding();

        try (ProtonTestServer peer = new ProtonTestServer()) {
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

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectDisposition().withState().rejected("decode-error", "failed reading message header");

            final StreamDelivery delivery = receiver.receive();
            final StreamReceiverMessage message = delivery.message();

            assertThrows(ClientException.class, () -> message.header());
            assertThrows(ClientException.class, () -> message.body());

            delivery.reject("decode-error", "failed reading message header");

            peer.expectDetach().respond();
            peer.expectEnd().respond();
            peer.expectClose().respond();

            receiver.close();
            connection.close();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testStreamDeliveryHandlesInvalidDeliveryAnnotationsEncoding() throws Exception {
        final byte[] payload = createInvalidDeliveryAnnotationsEncoding();

        try (ProtonTestServer peer = new ProtonTestServer()) {
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

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectDisposition().withState().rejected("decode-error", "failed reading message header");

            final StreamDelivery delivery = receiver.receive();
            final StreamReceiverMessage message = delivery.message();

            assertThrows(ClientException.class, () -> delivery.annotations());
            assertThrows(ClientException.class, () -> message.body());

            delivery.reject("decode-error", "failed reading message header");

            peer.expectDetach().respond();
            peer.expectEnd().respond();
            peer.expectClose().respond();

            receiver.close();
            connection.close();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testStreamDeliveryHandlesInvalidMessageAnnotationsEncoding() throws Exception {
        final byte[] payload = createInvalidMessageAnnotationsEncoding();

        try (ProtonTestServer peer = new ProtonTestServer()) {
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

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectDisposition().withState().rejected("decode-error", "failed reading message header");

            final StreamDelivery delivery = receiver.receive();
            final StreamReceiverMessage message = delivery.message();

            assertThrows(ClientException.class, () -> message.annotations());
            assertThrows(ClientException.class, () -> message.body());

            delivery.reject("decode-error", "failed reading message header");

            peer.expectDetach().respond();
            peer.expectEnd().respond();
            peer.expectClose().respond();

            receiver.close();
            connection.close();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testStreamDeliveryHandlesInvalidPropertiesEncoding() throws Exception {
        final byte[] payload = createInvalidPropertiesEncoding();

        try (ProtonTestServer peer = new ProtonTestServer()) {
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

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectDisposition().withState().rejected("decode-error", "failed reading message header");

            final StreamDelivery delivery = receiver.receive();
            final StreamReceiverMessage message = delivery.message();

            assertThrows(ClientException.class, () -> message.properties());
            assertThrows(ClientException.class, () -> message.body());

            delivery.reject("decode-error", "failed reading message header");

            peer.expectDetach().respond();
            peer.expectEnd().respond();
            peer.expectClose().respond();

            receiver.close();
            connection.close();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testStreamDeliveryHandlesInvalidApplicationPropertiesEncoding() throws Exception {
        final byte[] payload = createInvalidApplicationPropertiesEncoding();

        try (ProtonTestServer peer = new ProtonTestServer()) {
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

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectDisposition().withState().rejected("decode-error", "failed reading message header");

            final StreamDelivery delivery = receiver.receive();
            final StreamReceiverMessage message = delivery.message();

            assertThrows(ClientException.class, () -> message.applicationProperties());
            assertThrows(ClientException.class, () -> message.body());

            delivery.reject("decode-error", "failed reading message header");

            peer.expectDetach().respond();
            peer.expectEnd().respond();
            peer.expectClose().respond();

            receiver.close();
            connection.close();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testStreamDeliveryHandlesInvalidHeaderEncodingDuringBodyStreamOpen() throws Exception {
        final byte[] payload = createInvalidHeaderEncoding();

        try (ProtonTestServer peer = new ProtonTestServer()) {
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

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectDisposition().withState().rejected("decode-error", "failed reading message header");

            final StreamDelivery delivery = receiver.receive();

            StreamReceiverMessage message = delivery.message();

            assertThrows(ClientException.class, () -> message.body());

            delivery.reject("decode-error", "failed reading message header");

            peer.expectDetach().respond();
            peer.expectEnd().respond();
            peer.expectClose().respond();

            receiver.close();
            connection.close();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testConnectionDropsDuringStreamedBodyRead() throws Exception {
        final byte[] body1 = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        final byte[] body2 = new byte[] { 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0 };
        final byte[] payload1 = createEncodedMessage(new Data(body1));
        final byte[] payload2 = createEncodedMessage(new Data(body2));

        final CountDownLatch disconnected = new CountDownLatch(1);

        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().withMaxFrameSize(1000).respond();
            peer.expectBegin().withIncomingWindow(1).respond();
            peer.expectAttach().ofReceiver().respond();
            peer.expectFlow().withIncomingWindow(1).withLinkCredit(1);
            peer.remoteTransfer().withHandle(0)
                                 .withDeliveryId(0)
                                 .withDeliveryTag(new byte[] { 1 })
                                 .withMore(true)
                                 .withMessageFormat(0)
                                 .withPayload(payload1).queue();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            ConnectionOptions connectionOptions = new ConnectionOptions().maxFrameSize(1000);
            connectionOptions.disconnectedHandler((conn, event) -> disconnected.countDown());
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), connectionOptions);
            StreamReceiverOptions streamOptions = new StreamReceiverOptions().readBufferSize(2000).creditWindow(1);
            StreamReceiver receiver = connection.openStreamReceiver("test-queue", streamOptions);
            StreamDelivery delivery = receiver.receive();
            StreamReceiverMessage message = delivery.message();

            // Creating the input stream instance should read the first chunk of data from the incoming
            // delivery which should result in a new credit being available to expand the session window.
            // An additional transfer should be placed into the delivery buffer but not yet read since
            // the user hasn't read anything.
            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectFlow().withDeliveryCount(0).withIncomingWindow(1).withLinkCredit(1);
            peer.remoteTransfer().withHandle(0)
                                 .withDeliveryId(0)
                                 .withMore(true)
                                 .withMessageFormat(0)
                                 .withPayload(payload2).queue();
            peer.dropAfterLastHandler();

            InputStream bodyStream = message.body();
            assertNotNull(bodyStream);

            assertTrue(disconnected.await(5, TimeUnit.SECONDS));

            byte[] readPayload = new byte[body1.length + body2.length];

            try {
                bodyStream.read(readPayload);
                fail("Should not be able to read from closed connection stream");
            } catch (IOException ioe) {
                // Connection should be down now.
            }

            bodyStream.close();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testFrameSizeViolationWhileWaitingForIncomingStreamReceiverContent() throws Exception {
        byte[] overFrameSizeLimitFrameHeader = new byte[] { 0x00, (byte) 0xA0, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00 };

        final byte[] body = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        final byte[] payload = createEncodedMessage(new Data(body));

        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().withMaxFrameSize(65535).respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofReceiver().respond();
            peer.expectFlow().withLinkCredit(1);
            peer.remoteTransfer().withHandle(0)
                                 .withDeliveryId(0)
                                 .withDeliveryTag(new byte[] { 1 })
                                 .withMore(true)
                                 .withMessageFormat(0)
                                 .withPayload(payload).queue();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            ConnectionOptions connectionOptions = new ConnectionOptions().maxFrameSize(65535);
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), connectionOptions);
            StreamReceiverOptions streamOptions = new StreamReceiverOptions().creditWindow(1);
            StreamReceiver receiver = connection.openStreamReceiver("test-queue", streamOptions);
            StreamDelivery delivery = receiver.receive();
            StreamReceiverMessage message = delivery.message();
            InputStream stream = message.body();

            peer.waitForScriptToComplete();
            peer.expectClose().respond();
            peer.remoteBytes().withBytes(overFrameSizeLimitFrameHeader).later(10);

            byte[] bytesToRead = new byte[body.length * 2];

            try {
                stream.read(bytesToRead);
                fail("Should throw an error indicating issue with read of payload");
            } catch (IOException ioe) {
                // Expected
            }

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testStreamReceiverTryReadAmqpSequenceBytes() throws Exception {
        final List<String> stringList = new ArrayList<>();
        stringList.add("Hello World");
        final byte[] payload = createEncodedMessage(new AmqpSequence<>(stringList));

        doTestStreamReceiverReadsNonDataSectionBody(payload);
    }

    @Test
    public void testStreamReceiverTryReadAmqpValueBytes() throws Exception {
        final byte[] payload = createEncodedMessage(new AmqpValue<>("Hello World"));

        doTestStreamReceiverReadsNonDataSectionBody(payload);
    }

    private void doTestStreamReceiverReadsNonDataSectionBody(byte[] payload) throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.RECEIVER.getValue()).respond();
            peer.expectFlow();
            peer.remoteTransfer().withHandle(0)
                                 .withDeliveryId(0)
                                 .withDeliveryTag(new byte[] { 1 })
                                 .withSettled(true)
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

            final StreamDelivery delivery = receiver.receive();
            final StreamReceiverMessage message = delivery.message();
            try {
                message.body();
                fail("Should not return a stream since we cannot read this type");
            } catch (ClientException cliEx) {
                // Expected
            }

            peer.expectDetach().respond();
            peer.expectEnd().respond();
            peer.expectClose().respond();

            receiver.close();
            connection.close();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testReadMessageHeaderFromStreamReceiverMessage() throws Exception {
        final Header header = new Header();

        header.setDeliveryCount(UnsignedInteger.MAX_VALUE.longValue());
        header.setDurable(true);
        header.setFirstAcquirer(false);
        header.setPriority((byte) 255);
        header.setTimeToLive(Integer.MAX_VALUE);

        final byte[] payload = createEncodedMessage(header);

        try (ProtonTestServer peer = new ProtonTestServer()) {
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
            peer.expectDisposition().withFirst(0).withState().accepted().withSettled(true);
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

            Header readHeader = message.header();
            assertNotNull(readHeader);
            assertNull(message.body());

            assertEquals(Integer.toUnsignedLong(Integer.MAX_VALUE), message.timeToLive());
            assertEquals(true, message.durable());
            assertEquals(false, message.firstAcquirer());
            assertEquals((byte) 255, message.priority());
            assertEquals(Integer.toUnsignedLong(Integer.MAX_VALUE), message.timeToLive());

            peer.expectDetach().respond();
            peer.expectEnd().respond();
            peer.expectClose().respond();

            receiver.closeAsync().get();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testReadMessagePropertiesFromStreamReceiverMessage() throws Exception {
        final Properties properties = new Properties();

        properties.setAbsoluteExpiryTime(Integer.MAX_VALUE);
        properties.setContentEncoding("utf8");
        properties.setContentType("text/plain");
        properties.setCorrelationId(new byte[] { 1, 2, 3 });
        properties.setCreationTime(Short.MAX_VALUE);
        properties.setGroupId("Group");
        properties.setGroupSequence(UnsignedInteger.MAX_VALUE.longValue());
        properties.setMessageId(UUID.randomUUID());
        properties.setReplyTo("replyTo");
        properties.setReplyToGroupId("group-1");
        properties.setSubject("test");
        properties.setTo("queue");
        properties.setUserId(new byte[] { 0, 1, 5, 6, 9 });

        final byte[] payload = createEncodedMessage(new Header(), properties);

        try (ProtonTestServer peer = new ProtonTestServer()) {
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
            peer.expectDisposition().withFirst(0).withState().accepted().withSettled(true);
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

            assertFalse(message.hasProperties());
            assertFalse(message.hasFooters());
            assertFalse(message.hasAnnotations());

            Properties readProperties = message.properties();
            assertNotNull(readProperties);
            Header header = message.header();
            assertNotNull(header);
            assertNull(message.body());

            assertEquals(Integer.MAX_VALUE, message.absoluteExpiryTime());
            assertEquals("utf8", message.contentEncoding());
            assertEquals("text/plain", message.contentType());
            assertArrayEquals(new byte[] { 1, 2, 3 }, (byte[]) message.correlationId());
            assertEquals("utf8", message.contentEncoding());
            assertEquals("utf8", message.contentEncoding());
            assertEquals("utf8", message.contentEncoding());
            assertEquals(Short.MAX_VALUE, message.creationTime());
            assertEquals(UnsignedInteger.MAX_VALUE.intValue(), message.groupSequence());
            assertEquals(properties.getMessageId(), message.messageId());
            assertEquals("replyTo", message.replyTo());
            assertEquals("group-1", message.replyToGroupId());
            assertEquals("test", message.subject());
            assertEquals("queue", message.to());
            assertArrayEquals(new byte[] { 0, 1, 5, 6, 9 }, message.userId());

            peer.expectDetach().respond();
            peer.expectEnd().respond();
            peer.expectClose().respond();

            receiver.closeAsync().get();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testReadApplicationPropertiesStreamReceiverMessage() throws Exception {
        final Map<String, Object> propertiesMap = new HashMap<>();
        final ApplicationProperties appProperties = new ApplicationProperties(propertiesMap);

        propertiesMap.put("property1", UnsignedInteger.MAX_VALUE);
        propertiesMap.put("property2", UnsignedInteger.ONE);
        propertiesMap.put("property3", UnsignedInteger.ZERO);

        final byte[] payload = createEncodedMessage(appProperties);

        try (ProtonTestServer peer = new ProtonTestServer()) {
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
            peer.expectDisposition().withFirst(0).withState().accepted().withSettled(true);
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

            assertTrue(message.hasProperties());
            assertFalse(message.hasFooters());
            assertFalse(message.hasAnnotations());

            assertFalse(message.hasProperty("property"));
            assertEquals(UnsignedInteger.MAX_VALUE, message.property("property1"));
            assertEquals(UnsignedInteger.ONE, message.property("property2"));
            assertEquals(UnsignedInteger.ZERO, message.property("property3"));

            message.forEachProperty((key, value) -> {
                assertTrue(propertiesMap.containsKey(key));
                assertEquals(value, propertiesMap.get(key));
            });

            peer.expectDetach().respond();
            peer.expectEnd().respond();
            peer.expectClose().respond();

            receiver.closeAsync().get();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testDrainFutureSignalsFailureWhenDrainTimeoutExceeded() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofReceiver().respond();
            peer.expectFlow();
            peer.expectFlow().withDrain(true);
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            StreamReceiverOptions receiverOptions = new StreamReceiverOptions().drainTimeout(15);
            Receiver receiver = connection.openStreamReceiver("test-queue", receiverOptions).openFuture().get();

            try {
                receiver.drain().get();
                fail("Drain call should fail timeout exceeded.");
            } catch (ExecutionException cliEx) {
                LOG.debug("Receiver threw error on drain call", cliEx);
                assertTrue(cliEx.getCause() instanceof ClientOperationTimedOutException);
            }

            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testDrainFutureSignalsFailureWhenConnectionDrainTimeoutExceeded() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofReceiver().respond();
            peer.expectFlow();
            peer.expectFlow().withDrain(true);
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            ConnectionOptions connectionOptions = new ConnectionOptions().drainTimeout(20);
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), connectionOptions);
            Receiver receiver = connection.openStreamReceiver("test-queue").openFuture().get();

            try {
                receiver.drain().get();
                fail("Drain call should fail timeout exceeded.");
            } catch (ExecutionException cliEx) {
                LOG.debug("Receiver threw error on drain call", cliEx);
                assertTrue(cliEx.getCause() instanceof ClientOperationTimedOutException);
            }

            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testDrainCompletesWhenReceiverHasNoCredit() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.RECEIVER.getValue()).respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Receiver receiver = connection.openStreamReceiver("test-queue", new StreamReceiverOptions().creditWindow(0));
            receiver.openFuture().get(5, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            Future<? extends Receiver> draining = receiver.drain();
            draining.get(5, TimeUnit.SECONDS);

            // Close things down
            peer.expectClose().respond();
            connection.closeAsync().get(5, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(1, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testDrainAdditionalDrainCallThrowsWhenReceiverStillDraining() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofReceiver().respond();
            peer.expectFlow();
            peer.expectFlow().withDrain(true);
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            StreamReceiverOptions receiverOptions = new StreamReceiverOptions();
            Receiver receiver = connection.openStreamReceiver("test-queue", receiverOptions).openFuture().get();

            receiver.drain();

            try {
                receiver.drain().get();
                fail("Drain call should fail timeout exceeded.");
            } catch (ExecutionException cliEx) {
                LOG.debug("Receiver threw error on drain call", cliEx);
                assertTrue(cliEx.getCause() instanceof ClientIllegalStateException);
            }

            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testReceiverGetRemotePropertiesWaitsForRemoteAttach() throws Exception {
        tryReadReceiverRemoteProperties(true);
    }

    @Test
    public void testReceiverGetRemotePropertiesFailsAfterOpenTimeout() throws Exception {
        tryReadReceiverRemoteProperties(false);
    }

    private void tryReadReceiverRemoteProperties(boolean attachResponse) throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.RECEIVER.getValue());
            peer.expectFlow();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            StreamReceiverOptions options = new StreamReceiverOptions().openTimeout(150, TimeUnit.MILLISECONDS);
            StreamReceiver receiver = connection.openStreamReceiver("test-receiver", options);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            Map<String, Object> expectedProperties = new HashMap<>();
            expectedProperties.put("TEST", "test-property");

            if (attachResponse) {
                peer.expectDetach().respond();
                peer.expectEnd().respond();
                peer.respondToLastAttach().withPropertiesMap(expectedProperties).later(10);
            } else {
                peer.expectDetach();
                peer.expectEnd();
            }

            if (attachResponse) {
                assertNotNull(receiver.properties(), "Remote should have responded with a remote properties value");
                assertEquals(expectedProperties, receiver.properties());
            } else {
                try {
                    receiver.properties();
                    fail("Should failed to get remote state due to no attach response");
                } catch (ClientException ex) {
                    LOG.debug("Caught expected exception from blocking call", ex);
                }
            }

            try {
                receiver.closeAsync().get();
            } catch (ExecutionException ex) {
                LOG.debug("Caught unexpected exception from close call", ex);
                fail("Should not fail to close when connection not closed and detach sent");
            }

            peer.expectClose().respond();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testReceiverGetRemoteOfferedCapabilitiesWaitsForRemoteAttach() throws Exception {
        tryReadReceiverRemoteOfferedCapabilities(true);
    }

    @Test
    public void testReceiverGetRemoteOfferedCapabilitiesFailsAfterOpenTimeout() throws Exception {
        tryReadReceiverRemoteOfferedCapabilities(false);
    }

    private void tryReadReceiverRemoteOfferedCapabilities(boolean attachResponse) throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.RECEIVER.getValue());
            peer.expectFlow();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            StreamReceiverOptions options = new StreamReceiverOptions().openTimeout(150, TimeUnit.MILLISECONDS);
            StreamReceiver receiver = connection.openStreamReceiver("test-receiver", options);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            if (attachResponse) {
                peer.expectDetach().respond();
                peer.expectEnd().respond();
                peer.respondToLastAttach().withOfferedCapabilities("QUEUE").later(10);
            } else {
                peer.expectDetach();
                peer.expectEnd();
            }

            if (attachResponse) {
                assertNotNull(receiver.offeredCapabilities(), "Remote should have responded with a remote offered Capabilities value");
                assertEquals(1, receiver.offeredCapabilities().length);
                assertEquals("QUEUE", receiver.offeredCapabilities()[0]);
            } else {
                try {
                    receiver.offeredCapabilities();
                    fail("Should failed to get remote state due to no attach response");
                } catch (ClientException ex) {
                    LOG.debug("Caught expected exception from blocking call", ex);
                }
            }

            try {
                receiver.closeAsync().get();
            } catch (ExecutionException ex) {
                LOG.debug("Caught unexpected exception from close call", ex);
                fail("Should not fail to close when connection not closed and detach sent");
            }

            peer.expectClose().respond();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testReceiverGetRemoteDesiredCapabilitiesWaitsForRemoteAttach() throws Exception {
        tryReadReceiverRemoteDesiredCapabilities(true);
    }

    @Test
    public void testReceiverGetRemoteDesiredCapabilitiesFailsAfterOpenTimeout() throws Exception {
        tryReadReceiverRemoteDesiredCapabilities(false);
    }

    private void tryReadReceiverRemoteDesiredCapabilities(boolean attachResponse) throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.RECEIVER.getValue());
            peer.expectFlow();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            StreamReceiverOptions options = new StreamReceiverOptions().openTimeout(150, TimeUnit.MILLISECONDS);
            StreamReceiver receiver = connection.openStreamReceiver("test-receiver", options);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            if (attachResponse) {
                peer.expectDetach().respond();
                peer.expectEnd().respond();
                peer.respondToLastAttach().withDesiredCapabilities("Error-Free").later(10);
            } else {
                peer.expectDetach();
                peer.expectEnd();
            }

            if (attachResponse) {
                assertNotNull(receiver.desiredCapabilities(), "Remote should have responded with a remote desired Capabilities value");
                assertEquals(1, receiver.desiredCapabilities().length);
                assertEquals("Error-Free", receiver.desiredCapabilities()[0]);
            } else {
                try {
                    receiver.desiredCapabilities();
                    fail("Should failed to get remote state due to no attach response");
                } catch (ClientException ex) {
                    LOG.debug("Caught expected exception from blocking call", ex);
                }
            }

            try {
                receiver.closeAsync().get();
            } catch (ExecutionException ex) {
                LOG.debug("Caught unexpected exception from close call", ex);
                fail("Should not fail to close when connection not closed and detach sent");
            }

            peer.expectClose().respond();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testReceiverGetTargetWaitsForRemoteAttach() throws Exception {
        tryReadReceiverTarget(true);
    }

    @Test
    public void testReceiverGetTargetFailsAfterOpenTimeout() throws Exception {
        tryReadReceiverTarget(false);
    }

    private void tryReadReceiverTarget(boolean attachResponse) throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.RECEIVER.getValue());
            peer.expectFlow();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            StreamReceiverOptions options = new StreamReceiverOptions().openTimeout(150, TimeUnit.MILLISECONDS);
            StreamReceiver receiver = connection.openStreamReceiver("test-receiver", options);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            if (attachResponse) {
                peer.expectDetach().respond();
                peer.expectEnd().respond();
                peer.respondToLastAttach().later(10);
            } else {
                peer.expectDetach();
                peer.expectEnd();
            }

            if (attachResponse) {
                assertNotNull(receiver.target(), "Remote should have responded with a Target value");
            } else {
                try {
                    receiver.target();
                    fail("Should failed to get remote source due to no attach response");
                } catch (ClientException ex) {
                    LOG.debug("Caught expected exception from blocking call", ex);
                }
            }

            try {
                receiver.closeAsync().get();
            } catch (ExecutionException ex) {
                LOG.debug("Caught unexpected exception from close call", ex);
                fail("Should not fail to close when connection not closed and detach sent");
            }

            peer.expectClose().respond();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testReceiverGetSourceWaitsForRemoteAttach() throws Exception {
        tryReadReceiverSource(true);
    }

    @Test
    public void testReceiverGetSourceFailsAfterOpenTimeout() throws Exception {
        tryReadReceiverSource(false);
    }

    private void tryReadReceiverSource(boolean attachResponse) throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.RECEIVER.getValue());
            peer.expectFlow();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            StreamReceiverOptions options = new StreamReceiverOptions().openTimeout(150, TimeUnit.MILLISECONDS);
            StreamReceiver receiver = connection.openStreamReceiver("test-receiver", options);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            if (attachResponse) {
                peer.expectDetach().respond();
                peer.expectEnd().respond();
                peer.respondToLastAttach().later(10);
            } else {
                peer.expectDetach();
                peer.expectEnd();
            }

            if (attachResponse) {
                assertNotNull(receiver.source(), "Remote should have responded with a Source value");
                assertEquals("test-receiver", receiver.source().address());
            } else {
                try {
                    receiver.source();
                    fail("Should failed to get remote source due to no attach response");
                } catch (ClientException ex) {
                    LOG.debug("Caught expected exception from blocking call", ex);
                }
            }

            try {
                receiver.closeAsync().get();
            } catch (ExecutionException ex) {
                LOG.debug("Caught unexpected exception from close call", ex);
                fail("Should not fail to close when connection not closed and detach sent");
            }

            peer.expectClose().respond();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    private byte[] createInvalidHeaderEncoding() {
        final byte[] buffer = new byte[12];

        buffer[0] = 0; // Described Type Indicator
        buffer[1] = EncodingCodes.SMALLULONG;
        buffer[2] = Header.DESCRIPTOR_CODE.byteValue();
        buffer[3] = EncodingCodes.MAP32; // Should be list based

        return buffer;
    }

    private byte[] createInvalidDeliveryAnnotationsEncoding() {
        final byte[] buffer = new byte[12];

        buffer[0] = 0; // Described Type Indicator
        buffer[1] = EncodingCodes.SMALLULONG;
        buffer[2] = DeliveryAnnotations.DESCRIPTOR_CODE.byteValue();
        buffer[3] = EncodingCodes.LIST32; // Should be Map based

        return buffer;
    }

    private byte[] createInvalidMessageAnnotationsEncoding() {
        final byte[] buffer = new byte[12];

        buffer[0] = 0; // Described Type Indicator
        buffer[1] = EncodingCodes.SMALLULONG;
        buffer[2] = MessageAnnotations.DESCRIPTOR_CODE.byteValue();
        buffer[3] = EncodingCodes.LIST32; // Should be Map based

        return buffer;
    }

    private byte[] createInvalidPropertiesEncoding() {
        final byte[] buffer = new byte[12];

        buffer[0] = 0; // Described Type Indicator
        buffer[1] = EncodingCodes.SMALLULONG;
        buffer[2] = Properties.DESCRIPTOR_CODE.byteValue();
        buffer[3] = EncodingCodes.MAP32; // Should be list based

        return buffer;
    }

    private byte[] createInvalidApplicationPropertiesEncoding() {
        final byte[] buffer = new byte[12];

        buffer[0] = 0; // Described Type Indicator
        buffer[1] = EncodingCodes.SMALLULONG;
        buffer[2] = ApplicationProperties.DESCRIPTOR_CODE.byteValue();
        buffer[3] = EncodingCodes.LIST32; // Should be map based

        return buffer;
    }
}
