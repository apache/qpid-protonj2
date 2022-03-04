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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.net.URI;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.ClientOptions;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.ConnectionOptions;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.exceptions.ClientIllegalStateException;
import org.apache.qpid.protonj2.client.test.ImperativeClientTestCase;
import org.apache.qpid.protonj2.test.driver.ProtonTestServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the Client API implementation
 */
@Timeout(20)
public class ClientTest extends ImperativeClientTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(ClientTest.class);

    @Test
    public void testCreateWithNoContainerIdIsAllowed() {
        ClientOptions options = new ClientOptions();
        assertNull(options.id());

        try {
            Client instance = Client.create(options);

            assertNull(instance.containerId());
        } catch (NullPointerException npe) {
            fail("Should not enforce user supplied container Id");
        }
    }

    @Test
    public void testCreateWithContainerId() {
        final String id = "test-id";

        ClientOptions options = new ClientOptions();
        options.id(id);
        assertNotNull(options.id());

        Client client = Client.create(options);
        assertNotNull(client.containerId());
        assertEquals(id, client.containerId());
    }

    @Test
    public void testCloseClientAndConnectShouldFail() throws ClientException {
        Client client = Client.create();
        assertTrue(client.closeAsync().isDone());

        try {
            client.connect("localhost");
            fail("Should enforce no new connections on Client close");
        } catch (ClientIllegalStateException closed) {
            // Expected
        }

        try {
            client.connect("localhost", new ConnectionOptions());
            fail("Should enforce no new connections on Client close");
        } catch (ClientIllegalStateException closed) {
            // Expected
        }

        try {
            client.connect("localhost", 5672);
            fail("Should enforce no new connections on Client close");
        } catch (ClientIllegalStateException closed) {
            // Expected
        }

        try {
            client.connect("localhost", 5672, new ConnectionOptions());
            fail("Should enforce no new connections on Client close");
        } catch (ClientIllegalStateException closed) {
            // Expected
        }
    }

    @Test
    public void testCloseAllConnectionWhenNonCreatedDoesNotBlock() throws Exception {
        Client.create().close();
    }

    @Test
    public void testCloseAsyncDoesNotBlockWaitingOnConnectionClose() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectClose();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Connect test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());

            connection.openFuture().get();

            Future<Client> closedFuture = container.closeAsync();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            try {
                closedFuture.get(50, TimeUnit.MILLISECONDS);
                fail("should have timed out waiting on close.");
            } catch (TimeoutException ex) {
            }

            peer.remoteClose().now();
            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            assertNotNull(closedFuture.get(1, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testCloseAllConnectionAndWait() throws Exception {
        try (ProtonTestServer firstPeer = new ProtonTestServer();
             ProtonTestServer secondPeer = new ProtonTestServer()) {

            firstPeer.expectSASLAnonymousConnect();
            firstPeer.expectOpen().respond();
            firstPeer.start();

            secondPeer.expectSASLAnonymousConnect();
            secondPeer.expectOpen().respond();
            secondPeer.start();

            final URI firstURI = firstPeer.getServerURI();
            final URI secondURI = secondPeer.getServerURI();

            Client container = Client.create();
            Connection connection1 = container.connect(firstURI.getHost(), firstURI.getPort());
            Connection connection2 = container.connect(secondURI.getHost(), secondURI.getPort());

            connection1.openFuture().get();
            connection2.openFuture().get();

            firstPeer.waitForScriptToComplete();
            secondPeer.waitForScriptToComplete();

            firstPeer.expectClose().respond().afterDelay(10);
            secondPeer.expectClose().respond().afterDelay(15);

            container.closeAsync().get(5, TimeUnit.SECONDS);

            firstPeer.waitForScriptToComplete();
            secondPeer.waitForScriptToComplete();
        }
    }
}
