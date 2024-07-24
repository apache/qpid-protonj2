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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.net.URI;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.ConnectionOptions;
import org.apache.qpid.protonj2.client.DeliveryMode;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.Sender;
import org.apache.qpid.protonj2.client.SenderOptions;
import org.apache.qpid.protonj2.client.Session;
import org.apache.qpid.protonj2.client.Tracker;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.test.driver.ProtonTestServer;
import org.apache.qpid.protonj2.test.driver.ProtonTestServerOptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for the Connection class connecting over WebSockets
 */
@Timeout(20)
public class WsConnectionTest extends ConnectionTest {

    private static final Logger LOG = LoggerFactory.getLogger(WsConnectionTest.class);

    @Override
    protected ProtonTestServerOptions testServerOptions() {
        return new ProtonTestServerOptions().setUseWebSockets(true);
    }

    @Override
    protected ConnectionOptions connectionOptions() {
        ConnectionOptions options = new ConnectionOptions();
        options.transportOptions().useWebSockets(true);

        return options;
    }

    @Override
    protected ConnectionOptions connectionOptions(String user, String password) {
        ConnectionOptions options = new ConnectionOptions();
        options.transportOptions().useWebSockets(true);
        options.user(user);
        options.password(password);

        return options;
    }

    @Test
    public void testWSConnectFailsDueToServerListeningOverTCP() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer(testServerOptions().setUseWebSockets(false))) {
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("WebSocket Connect test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            ConnectionOptions options = connectionOptions();

            try {
                Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), options);
                connection.openFuture().get();
                fail("Should fail to connect");
            } catch (ExecutionException ex) {
                LOG.info("Connection create failed due to: ", ex);
                assertTrue(ex.getCause() instanceof ClientException);
            }

            peer.waitForScriptToCompleteIgnoreErrors();
        }
    }

    @Test
    public void testSendMessageWithLargeStringBodyWithCompressionEnabled() throws Exception {
        final int BODY_SIZE = 16384;

        final String payload = new String("A").repeat(BODY_SIZE);

        try (ProtonTestServer peer = new ProtonTestServer(testServerOptions().setWebSocketCompression(true))) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofSender().respond();
            peer.remoteFlow().withLinkCredit(10).queue();
            peer.expectAttach().respond();  // Open a receiver to ensure sender link has processed
            peer.expectFlow();              // the inbound flow frame we sent previously before send.
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Sender test started, peer listening on: {}", remoteURI);

            ConnectionOptions connectOptions = new ConnectionOptions();
            connectOptions.transportOptions().webSocketCompression(true);
            connectOptions.transportOptions().useWebSockets(true);
            connectOptions.traceFrames(true);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), connectOptions).openFuture().get();

            Session session = connection.openSession().openFuture().get();
            SenderOptions options = new SenderOptions().deliveryMode(DeliveryMode.AT_MOST_ONCE);
            Sender sender = session.openSender("test-qos", options);

            // Gates send on remote flow having been sent and received
            session.openReceiver("dummy").openFuture().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectTransfer().withMessage().withValue(payload);
            peer.expectDetach().respond();
            peer.expectClose().respond();

            final Message<String> message = Message.create(payload);
            final Tracker tracker = sender.send(message);

            assertNotNull(tracker);
            assertNotNull(tracker.settlementFuture().isDone());
            assertNotNull(tracker.settlementFuture().get().settled());

            sender.closeAsync().get(10, TimeUnit.SECONDS);

            connection.closeAsync().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }
}
