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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.ConnectionOptions;
import org.apache.qpid.protonj2.client.StreamDelivery;
import org.apache.qpid.protonj2.client.StreamReceiver;
import org.apache.qpid.protonj2.client.test.ImperativeClientTestCase;
import org.apache.qpid.protonj2.test.driver.ProtonTestServer;
import org.apache.qpid.protonj2.types.messaging.AmqpValue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Tests that validate Stream Receiver behavior after a client reconnection.
 */
@Timeout(20)
class ReconnectStreamReceiverTest extends ImperativeClientTestCase {

    @Test
    public void testStreamReceiverRecoversAndDeliveryReceived() throws Exception {
        final byte[] payload = createEncodedMessage(new AmqpValue<>("Hello World"));

        try (ProtonTestServer firstPeer = new ProtonTestServer();
             ProtonTestServer finalPeer = new ProtonTestServer()) {

            firstPeer.expectSASLAnonymousConnect();
            firstPeer.expectOpen().respond();
            firstPeer.expectBegin().respond();
            firstPeer.expectAttach().ofReceiver().respond();
            firstPeer.expectFlow();
            firstPeer.dropAfterLastHandler();
            firstPeer.start();

            finalPeer.expectSASLAnonymousConnect();
            finalPeer.expectOpen().respond();
            finalPeer.expectBegin().respond();
            finalPeer.expectAttach().ofReceiver().respond();
            finalPeer.expectFlow();
            finalPeer.remoteTransfer().withHandle(0)
                                      .withDeliveryId(0)
                                      .withDeliveryTag(new byte[] { 1 })
                                      .withMore(false)
                                      .withMessageFormat(0)
                                      .withPayload(payload).queue();
            finalPeer.start();

            final URI primaryURI = firstPeer.getServerURI();
            final URI backupURI = finalPeer.getServerURI();

            ConnectionOptions options = new ConnectionOptions();
            options.reconnectOptions().reconnectEnabled(true);
            options.reconnectOptions().addReconnectLocation(backupURI.getHost(), backupURI.getPort());

            Client container = Client.create();
            Connection connection = container.connect(primaryURI.getHost(), primaryURI.getPort(), options);
            final StreamReceiver receiver = connection.openStreamReceiver("test-queue");
            final StreamDelivery delivery = receiver.receive();

            firstPeer.waitForScriptToComplete();
            finalPeer.waitForScriptToComplete();

            assertNotNull(delivery);
            assertTrue(delivery.completed());
            assertFalse(delivery.aborted());

            finalPeer.expectDetach().respond();
            finalPeer.expectEnd().respond();
            finalPeer.expectClose().respond();

            receiver.close();
            connection.closeAsync().get();

            finalPeer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testCannotReceiveFromStreamStartedBeforeReconnection() throws Exception {
        final byte[] payload = createEncodedMessage(new AmqpValue<>("Hello World"));

        try (ProtonTestServer firstPeer = new ProtonTestServer();
             ProtonTestServer finalPeer = new ProtonTestServer()) {

            firstPeer.expectSASLAnonymousConnect();
            firstPeer.expectOpen().respond();
            firstPeer.expectBegin().respond();
            firstPeer.expectAttach().ofReceiver().respond();
            firstPeer.expectFlow();
            firstPeer.remoteTransfer().withHandle(0)
                                      .withDeliveryId(0)
                                      .withDeliveryTag(new byte[] { 1 })
                                      .withMore(true)
                                      .withMessageFormat(0)
                                      .withPayload(payload).queue();
            firstPeer.dropAfterLastHandler();
            firstPeer.start();

            finalPeer.expectSASLAnonymousConnect();
            finalPeer.expectOpen().respond();
            finalPeer.expectBegin().respond();
            finalPeer.expectAttach().ofReceiver().respond();
            finalPeer.expectFlow();
            finalPeer.start();

            final URI primaryURI = firstPeer.getServerURI();
            final URI backupURI = finalPeer.getServerURI();

            ConnectionOptions options = new ConnectionOptions();
            options.reconnectOptions().reconnectEnabled(true);
            options.reconnectOptions().addReconnectLocation(backupURI.getHost(), backupURI.getPort());

            Client container = Client.create();
            Connection connection = container.connect(primaryURI.getHost(), primaryURI.getPort(), options);
            final StreamReceiver receiver = connection.openStreamReceiver("test-queue");
            final StreamDelivery delivery = receiver.receive();

            firstPeer.waitForScriptToComplete();
            finalPeer.waitForScriptToComplete();

            assertNotNull(delivery);
            assertFalse(delivery.completed());
            assertFalse(delivery.aborted());

            assertThrows(IOException.class, () -> delivery.rawInputStream().read());

            finalPeer.expectDetach().respond();
            finalPeer.expectEnd().respond();
            finalPeer.expectClose().respond();

            receiver.close();
            connection.closeAsync().get();

            finalPeer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }
}
