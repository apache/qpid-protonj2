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
import org.apache.qpid.protonj2.client.Receiver;
import org.apache.qpid.protonj2.client.Session;
import org.apache.qpid.protonj2.client.SessionOptions;
import org.apache.qpid.protonj2.client.test.ImperativeClientTestCase;
import org.apache.qpid.protonj2.test.driver.ProtonTestServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests that validate client Session behavior after a client reconnection.
 */
@Timeout(20)
class ReconnectSessionTest extends ImperativeClientTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(ReconnectSessionTest.class);

    @Test
    public void testOpenedSessionRecoveredAfterConnectionDropped() throws Exception {
        try (ProtonTestServer firstPeer = new ProtonTestServer();
             ProtonTestServer finalPeer = new ProtonTestServer()) {

            firstPeer.expectSASLAnonymousConnect();
            firstPeer.expectOpen().respond();
            firstPeer.expectBegin().respond();
            firstPeer.dropAfterLastHandler(5);
            firstPeer.start();

            finalPeer.expectSASLAnonymousConnect();
            finalPeer.expectOpen().respond();
            finalPeer.expectBegin().respond();
            finalPeer.start();

            final URI primaryURI = firstPeer.getServerURI();
            final URI backupURI = finalPeer.getServerURI();

            ConnectionOptions options = new ConnectionOptions();
            options.idleTimeout(5, TimeUnit.SECONDS);
            options.reconnectOptions().reconnectEnabled(true);
            options.reconnectOptions().addReconnectLocation(backupURI.getHost(), backupURI.getPort());

            Client container = Client.create();
            Connection connection = container.connect(primaryURI.getHost(), primaryURI.getPort(), options);
            Session session = connection.openSession().openFuture().get();

            firstPeer.waitForScriptToComplete();

            connection.openFuture().get();

            finalPeer.waitForScriptToComplete();
            finalPeer.expectEnd().respond();
            finalPeer.expectClose().respond();

            session.close();
            connection.close();

            finalPeer.waitForScriptToComplete();
        }
    }

    @Test
    public void testSessionCreationRecoversAfterDropWithNoBeginResponse() throws Exception {
        try (ProtonTestServer firstPeer = new ProtonTestServer();
             ProtonTestServer finalPeer = new ProtonTestServer()) {

            firstPeer.expectSASLAnonymousConnect();
            firstPeer.expectOpen().respond();
            firstPeer.expectBegin();
            firstPeer.dropAfterLastHandler(20);
            firstPeer.start();

            finalPeer.expectSASLAnonymousConnect();
            finalPeer.expectOpen().respond();
            finalPeer.expectBegin().respond();
            finalPeer.expectAttach().ofReceiver().respond();
            finalPeer.expectFlow();
            finalPeer.expectClose().respond();
            finalPeer.start();

            final URI primaryURI = firstPeer.getServerURI();
            final URI backupURI = finalPeer.getServerURI();

            ConnectionOptions options = new ConnectionOptions();
            options.reconnectOptions().reconnectEnabled(true);
            options.reconnectOptions().addReconnectLocation(backupURI.getHost(), backupURI.getPort());

            Client container = Client.create();
            Connection connection = container.connect(primaryURI.getHost(), primaryURI.getPort(), options);
            Session session = connection.openSession();

            firstPeer.waitForScriptToComplete();

            Receiver receiver = session.openFuture().get().openReceiver("queue").openFuture().get();

            assertNull(receiver.tryReceive());

            connection.close();

            finalPeer.waitForScriptToComplete(1000);
        }
    }

    @Test
    public void testMultipleSessionCreationRecoversAfterDropWithNoBeginResponse() throws Exception {
        try (ProtonTestServer firstPeer = new ProtonTestServer();
             ProtonTestServer intermediatePeer = new ProtonTestServer();
             ProtonTestServer finalPeer = new ProtonTestServer()) {

            firstPeer.expectSASLAnonymousConnect();
            firstPeer.expectOpen().respond();
            firstPeer.expectBegin().respond();
            firstPeer.expectBegin();
            firstPeer.dropAfterLastHandler(20);
            firstPeer.start();

            intermediatePeer.expectSASLAnonymousConnect();
            intermediatePeer.expectOpen().respond();
            intermediatePeer.expectBegin().respond();
            intermediatePeer.expectBegin();
            intermediatePeer.dropAfterLastHandler();
            intermediatePeer.start();

            finalPeer.expectSASLAnonymousConnect();
            finalPeer.expectOpen().respond();
            finalPeer.expectBegin().respond();
            finalPeer.expectBegin().respond();
            finalPeer.expectAttach().ofReceiver().respond();
            finalPeer.expectFlow();
            finalPeer.expectAttach().ofReceiver().respond();
            finalPeer.expectFlow();
            finalPeer.expectClose().respond();
            finalPeer.start();

            final URI primaryURI = firstPeer.getServerURI();
            final URI intermediateURI = intermediatePeer.getServerURI();
            final URI backupURI = finalPeer.getServerURI();

            ConnectionOptions options = new ConnectionOptions();
            options.reconnectOptions().reconnectEnabled(true);
            options.idleTimeout(5, TimeUnit.SECONDS);
            options.reconnectOptions().addReconnectLocation(intermediateURI.getHost(), intermediateURI.getPort());
            options.reconnectOptions().addReconnectLocation(backupURI.getHost(), backupURI.getPort());

            Client container = Client.create();
            Connection connection = container.connect(primaryURI.getHost(), primaryURI.getPort(), options);
            Session session1 = connection.openSession();
            Session session2 = connection.openSession();

            firstPeer.waitForScriptToComplete();
            intermediatePeer.waitForScriptToComplete();

            // Await both being open before doing work to make the outcome predictable
            session1.openFuture().get();
            session2.openFuture().get();

            Receiver receiver1 = session1.openReceiver("queue").openFuture().get();
            Receiver receiver2 = session2.openReceiver("queue").openFuture().get();

            assertNull(receiver1.tryReceive());
            assertNull(receiver2.tryReceive());

            connection.close();

            finalPeer.waitForScriptToComplete(1000);
        }
    }

    @Test
    public void testSessionOpenTimeoutWhenNoRemoteBeginArrivesTimeoutWithReconnection() throws Exception {
        doTestSessionOpenTimeoutWhenNoRemoteBeginArrives(true);
    }

    @Test
    public void testSessionOpenTimeoutWhenNoRemoteBeginArrivesNoTimeoutWithReconnection() throws Exception {
        doTestSessionOpenTimeoutWhenNoRemoteBeginArrives(false);
    }

    /*
     * Tests that session open timeout is preserved across reconnection boundaries
     */
    private void doTestSessionOpenTimeoutWhenNoRemoteBeginArrives(boolean timeout) throws Exception {
        try (ProtonTestServer firstPeer = new ProtonTestServer();
             ProtonTestServer finalPeer = new ProtonTestServer()) {

            firstPeer.expectSASLAnonymousConnect();
            firstPeer.expectOpen().respond();
            firstPeer.expectBegin();
            firstPeer.dropAfterLastHandler();
            firstPeer.start();

            finalPeer.expectSASLAnonymousConnect();
            finalPeer.expectOpen().respond();
            finalPeer.expectBegin().optional();  // Might not arrive if timed out already
            finalPeer.expectEnd().optional();
            finalPeer.expectClose().respond();
            finalPeer.start();

            final URI firstURI = firstPeer.getServerURI();
            final URI finalURI = finalPeer.getServerURI();

            LOG.info("Test started, peer listening on: {}", firstURI);

            ConnectionOptions options = new ConnectionOptions();
            options.reconnectOptions().reconnectEnabled(true);
            options.reconnectOptions().addReconnectLocation(finalURI.getHost(), finalURI.getPort());

            Client container = Client.create();
            Connection connection = container.connect(firstURI.getHost(), firstURI.getPort(), options);
            Session session = connection.openSession(new SessionOptions().openTimeout(500));

            try {
                if (timeout) {
                    session.openFuture().get(500, TimeUnit.MILLISECONDS);
                } else {
                    session.openFuture().get();
                }

                fail("Session Open should timeout when no Begin response and complete future with error.");
            } catch (Throwable error) {
                LOG.info("Session open failed with error: ", error);
            }

            connection.closeAsync().get();

            firstPeer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            finalPeer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }
}
