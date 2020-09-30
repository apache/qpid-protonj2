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

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.net.URI;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.ConnectionOptions;
import org.apache.qpid.protonj2.client.DeliveryState;
import org.apache.qpid.protonj2.client.StreamDelivery;
import org.apache.qpid.protonj2.client.StreamReceiver;
import org.apache.qpid.protonj2.client.StreamReceiverOptions;
import org.apache.qpid.protonj2.client.exceptions.ClientOperationTimedOutException;
import org.apache.qpid.protonj2.client.test.ImperativeClientTestCase;
import org.apache.qpid.protonj2.client.util.Wait;
import org.apache.qpid.protonj2.test.driver.codec.messaging.Accepted;
import org.apache.qpid.protonj2.test.driver.netty.NettyTestPeer;
import org.apache.qpid.protonj2.types.messaging.AmqpValue;
import org.apache.qpid.protonj2.types.transport.Role;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests the {@link ReceiveContext} implementation
 */
@Disabled
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
            receiver.close();
            connection.close().get();

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

            receiver.close();
            connection.close().get();

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

            try {
                receiver.receive(5, TimeUnit.MILLISECONDS);
                fail("Should time out waiting on a delivery");
            } catch (ClientOperationTimedOutException ex) {
            }

            try {
                receiver.receive(5, TimeUnit.MILLISECONDS);
                fail("Should time out waiting on a delivery");
            } catch (ClientOperationTimedOutException ex) {
            }

            peer.expectDetach().respond();
            peer.expectClose().respond();

            receiver.close();
            connection.close().get();

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

            receiver.close();
            connection.close().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }
}
