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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.net.URI;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.Delivery;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.ReceiveContext;
import org.apache.qpid.protonj2.client.ReceiveContextOptions;
import org.apache.qpid.protonj2.client.Receiver;
import org.apache.qpid.protonj2.client.Session;
import org.apache.qpid.protonj2.client.test.ImperativeClientTestCase;
import org.apache.qpid.protonj2.client.util.Wait;
import org.apache.qpid.protonj2.test.driver.netty.NettyTestPeer;
import org.apache.qpid.protonj2.types.messaging.AmqpValue;
import org.apache.qpid.protonj2.types.transport.Role;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests the {@link ReceiveContext} implementation
 */
@Timeout(20)
class ReceiveContextTest extends ImperativeClientTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(ReceiveContextTest.class);

    @Test
    public void testOpenReceiveContextsDoNotBlockDeliveryWhenNotInitiaited() throws Exception {
        final byte[] payload = createEncodedMessage(new AmqpValue<>("Hello World"));

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
            Session session = connection.openSession();
            Receiver receiver = session.openReceiver("test-queue");

            ReceiveContext rcvContext1 = receiver.openReceiveContext(new ReceiveContextOptions());
            ReceiveContext rcvContext2 = receiver.openReceiveContext(new ReceiveContextOptions());

            assertNotNull(rcvContext1);
            assertNotNull(rcvContext2);

            assertNull(rcvContext1.delivery());
            assertNull(rcvContext2.delivery());

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            peer.expectDisposition().withSettled(true).withState().accepted();
            peer.remoteTransfer().withHandle(0)
                                 .withDeliveryId(0)
                                 .withDeliveryTag(new byte[] { 1 })
                                 .withMore(false)
                                 .withMessageFormat(0)
                                 .withPayload(payload).now();

            Delivery delivery = receiver.receive(5, TimeUnit.SECONDS);
            assertNotNull(delivery);
            Message<String> message = delivery.message();

            assertEquals("Hello World", message.body());
            assertNull(rcvContext1.delivery());
            assertNull(rcvContext2.delivery());

            peer.expectDetach().respond();
            peer.expectClose().respond();

            receiver.close();
            connection.close().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testReceiveContextTakesDeliveryFromPrefetch() throws Exception {
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

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession();
            Receiver receiver = session.openReceiver("test-queue");
            ReceiveContext rcvContext = receiver.openReceiveContext(new ReceiveContextOptions());

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectDisposition().withSettled(true).withState().accepted();

            Wait.assertTrue(() -> receiver.prefetchedCount() == 1);

            Delivery delivery = rcvContext.awaitDelivery().delivery();
            assertNotNull(delivery);

            Message<String> message = delivery.message();
            assertNotNull(message);

            assertEquals("Hello World", message.body());

            peer.expectDetach().respond();
            peer.expectClose().respond();

            receiver.close();
            connection.close().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }
}
