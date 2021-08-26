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

import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.ClientOptions;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.ConnectionOptions;
import org.apache.qpid.protonj2.client.ErrorCondition;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.Receiver;
import org.apache.qpid.protonj2.client.Sender;
import org.apache.qpid.protonj2.client.Session;
import org.apache.qpid.protonj2.client.Tracker;
import org.apache.qpid.protonj2.client.exceptions.ClientConnectionRedirectedException;
import org.apache.qpid.protonj2.client.exceptions.ClientConnectionRemotelyClosedException;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.exceptions.ClientIOException;
import org.apache.qpid.protonj2.client.exceptions.ClientUnsupportedOperationException;
import org.apache.qpid.protonj2.client.test.ImperativeClientTestCase;
import org.apache.qpid.protonj2.test.driver.ProtonTestServer;
import org.apache.qpid.protonj2.test.driver.ProtonTestServerOptions;
import org.apache.qpid.protonj2.test.driver.codec.messaging.TerminusDurability;
import org.apache.qpid.protonj2.test.driver.codec.messaging.TerminusExpiryPolicy;
import org.apache.qpid.protonj2.test.driver.matchers.messaging.SourceMatcher;
import org.apache.qpid.protonj2.types.transport.AMQPHeader;
import org.apache.qpid.protonj2.types.transport.AmqpError;
import org.apache.qpid.protonj2.types.transport.ConnectionError;
import org.apache.qpid.protonj2.types.transport.Role;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for the Connection class
 */
@Timeout(20)
public class ConnectionTest extends ImperativeClientTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectionTest.class);

    @Test
    public void testConnectFailsDueToServerStopped() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer(testServerOptions())) {
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Connect test started, peer listening on: {}", remoteURI);

            peer.close();

            Client container = Client.create();

            try {
                Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), connectionOptions());
                connection.openFuture().get();
                fail("Should fail to connect");
            } catch (ExecutionException ex) {
                LOG.info("Connection create failed due to: ", ex);
                assertTrue(ex.getCause() instanceof ClientException);
            }

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testCreateTwoDistinctConnectionsFromSingleClientInstance() throws Exception {
        try (ProtonTestServer firstPeer = new ProtonTestServer(testServerOptions());
             ProtonTestServer secondPeer = new ProtonTestServer(testServerOptions())) {

            firstPeer.expectSASLAnonymousConnect();
            firstPeer.expectOpen().withContainerId(Matchers.any(String.class)).respond();
            firstPeer.expectClose().respond();
            firstPeer.start();

            secondPeer.expectSASLAnonymousConnect();
            secondPeer.expectOpen().withContainerId(Matchers.any(String.class)).respond();
            secondPeer.expectClose().respond();
            secondPeer.start();

            final URI firstURI = firstPeer.getServerURI();
            final URI secondURI = secondPeer.getServerURI();

            Client container = Client.create();
            Connection connection1 = container.connect(firstURI.getHost(), firstURI.getPort(), connectionOptions());
            Connection connection2 = container.connect(secondURI.getHost(), secondURI.getPort(), connectionOptions());

            connection1.openFuture().get();
            connection2.openFuture().get();

            connection1.closeAsync().get();
            connection2.closeAsync().get();

            firstPeer.waitForScriptToComplete();
            secondPeer.waitForScriptToComplete();
        }
    }

    @Test
    public void testCreateConnectionToNonSaslPeer() throws Exception {
        doConnectionWithUnexpectedHeaderTestImpl(AMQPHeader.getAMQPHeader().toArray());
    }

    @Test
    public void testCreateConnectionToNonAmqpPeer() throws Exception {
        doConnectionWithUnexpectedHeaderTestImpl(new byte[] { 'N', 'O', 'T', '-', 'A', 'M', 'Q', 'P' });
    }

    private void doConnectionWithUnexpectedHeaderTestImpl(byte[] responseHeader) throws Exception, IOException {
        try (ProtonTestServer peer = new ProtonTestServer(testServerOptions())) {
            peer.expectSASLHeader().respondWithBytes(responseHeader);
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Connect test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            ConnectionOptions options = connectionOptions("guest", "guest");
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), options);

            try {
                connection.openFuture().get(10, TimeUnit.SECONDS);
            } catch (ExecutionException ex) {}

            connection.close();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testCreateConnectionString() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer(testServerOptions())) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Connect test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), connectionOptions());

            connection.openFuture().get(10, TimeUnit.SECONDS);
            connection.closeAsync().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testCreateConnectionSignalsEvent() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer(testServerOptions())) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectClose().respond();
            peer.start();

            final URI remoteURI = peer.getServerURI();
            final CountDownLatch connected = new CountDownLatch(1);

            LOG.info("Connect test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(
                remoteURI.getHost(), remoteURI.getPort(), connectionOptions().connectedHandler((conn, event) -> connected.countDown()));

            connection.openFuture().get(10, TimeUnit.SECONDS);

            assertTrue(connected.await(5, TimeUnit.SECONDS));

            connection.closeAsync().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testCreateConnectionWithConfiguredContainerId() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer(testServerOptions())) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().withContainerId("container-id-test").respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Connect test started, peer listening on: {}", remoteURI);

            ClientOptions options = new ClientOptions().id("container-id-test");
            Client container = Client.create(options);
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), connectionOptions());

            connection.openFuture().get(10, TimeUnit.SECONDS);
            connection.closeAsync().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testCreateConnectionStringWithDefaultTcpPort() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer(testServerOptions())) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Connect test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            ConnectionOptions options = connectionOptions();
            options.transportOptions().defaultTcpPort(remoteURI.getPort());
            Connection connection = container.connect(remoteURI.getHost(), options);

            connection.openFuture().get(10, TimeUnit.SECONDS);
            connection.closeAsync().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testCreateConnectionEstablishedHandlerGetsCalled() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer(testServerOptions())) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Connect test started, peer listening on: {}", remoteURI);

            final CountDownLatch established = new CountDownLatch(1);
            ConnectionOptions options = connectionOptions();

            options.connectedHandler((connection, location) -> {
                LOG.info("Connection signaled that it was established");
                established.countDown();
            });

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), options);

            assertTrue(established.await(10, TimeUnit.SECONDS));

            connection.openFuture().get(10, TimeUnit.SECONDS);
            connection.closeAsync().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testCreateConnectionFailedHandlerGetsCalled() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer(testServerOptions())) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin();
            peer.dropAfterLastHandler(10);
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            final CountDownLatch failed = new CountDownLatch(1);
            ConnectionOptions options = connectionOptions();

            options.disconnectedHandler((connection, location) -> {
                LOG.info("Connection signaled that it has failed");
                failed.countDown();
            });

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), options);

            connection.openFuture().get(10, TimeUnit.SECONDS);
            connection.openSession();

            assertTrue(failed.await(10, TimeUnit.SECONDS));

            connection.close();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testCreateConnectionWithCredentialsChoosesSASLPlainIfOffered() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer(testServerOptions())) {
            peer.expectSASLPlainConnect("user", "pass");
            peer.expectOpen().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Connect test started, peer listening on: {}", remoteURI);

            final CountDownLatch established = new CountDownLatch(1);
            ConnectionOptions options = connectionOptions("user", "pass");

            options.connectedHandler((connection, location) -> {
                LOG.info("Connection signaled that it was established");
                established.countDown();
            });

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), options);

            assertTrue(established.await(10, TimeUnit.SECONDS));

            connection.openFuture().get(10, TimeUnit.SECONDS);
            connection.closeAsync().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testCreateConnectionWithSASLDisabledToSASLEnabledHost() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer(testServerOptions())) {
            peer.expectAMQPHeader().respondWithSASLPHeader();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Connect test started, peer listening on: {}", remoteURI);

            final ConnectionOptions options = connectionOptions();
            options.saslOptions().saslEnabled(false);

            final Client container = Client.create();
            final Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), options);

            try {
                connection.openFuture().get(10, TimeUnit.SECONDS);
                fail("Should not successfully connect to remote");
            } catch(ExecutionException ex) {
                assertTrue(ex.getCause() instanceof ClientConnectionRemotelyClosedException);
            }

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testConnectionCloseGetsResponseWithErrorDoesNotThrowTimedGet() throws Exception {
        doTestConnectionCloseGetsResponseWithErrorDoesNotThrow(true);
    }

    @Test
    public void testConnectionCloseGetsResponseWithErrorDoesNotThrowUntimedGet() throws Exception {
        doTestConnectionCloseGetsResponseWithErrorDoesNotThrow(false);
    }

    protected void doTestConnectionCloseGetsResponseWithErrorDoesNotThrow(boolean timeout) throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer(testServerOptions())) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectClose().respond().withErrorCondition(ConnectionError.CONNECTION_FORCED.toString(), "Not accepting connections");
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Connect test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), connectionOptions());

            if (timeout) {
                connection.openFuture().get(10, TimeUnit.SECONDS);
                // Should close normally and not throw error as we initiated the close.
                connection.closeAsync().get(10, TimeUnit.SECONDS);
            } else {
                connection.openFuture().get();
                // Should close normally and not throw error as we initiated the close.
                connection.closeAsync().get();
            }

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testRemotelyCloseConnectionWithRedirect() throws Exception {
        final String redirectVhost = "vhost";
        final String redirectNetworkHost = "localhost";
        final int redirectPort = 5677;
        final String redirectScheme = "wss";
        final String redirectPath = "/websockets";

        // Tell the test peer to close the connection when executing its last handler
        final Map<String, Object> errorInfo = new HashMap<>();
        errorInfo.put(ClientConstants.OPEN_HOSTNAME.toString(), redirectVhost);
        errorInfo.put(ClientConstants.NETWORK_HOST.toString(), redirectNetworkHost);
        errorInfo.put(ClientConstants.PORT.toString(), redirectPort);
        errorInfo.put(ClientConstants.SCHEME.toString(), redirectScheme);
        errorInfo.put(ClientConstants.PATH.toString(), redirectPath);

        try (ProtonTestServer peer = new ProtonTestServer(testServerOptions())) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().reject(ConnectionError.REDIRECT.toString(), "Not accepting connections", errorInfo);
            peer.expectBegin().optional();
            peer.expectClose();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), connectionOptions());

            try {
                connection.defaultSession().openFuture().get();
                fail("Should not be able to connect since the connection is redirected.");
            } catch (Exception ex) {
                LOG.debug("Received expected exception from session open: {}", ex.getMessage());
                Throwable cause = ex.getCause();
                assertTrue(cause instanceof ClientConnectionRedirectedException);

                ClientConnectionRedirectedException connectionRedirect = (ClientConnectionRedirectedException) ex.getCause();

                assertEquals(redirectVhost, connectionRedirect.getHostname());
                assertEquals(redirectNetworkHost, connectionRedirect.getNetworkHost());
                assertEquals(redirectPort, connectionRedirect.getPort());
                assertEquals(redirectScheme, connectionRedirect.getScheme());
                assertEquals(redirectPath, connectionRedirect.getPath());

                URI redirect = connectionRedirect.getRedirectionURI();

                assertEquals(redirectNetworkHost, redirect.getHost());
                assertEquals(redirectPort, redirect.getPort());
                assertEquals(redirectScheme, redirect.getScheme());
                assertEquals(redirectPath, redirect.getPath());
            }

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testConnectionBlockingCloseGetsResponseWithErrorDoesNotThrow() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer(testServerOptions())) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectClose().respond().withErrorCondition(ConnectionError.CONNECTION_FORCED.toString(), "Not accepting connections");
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Connect test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), connectionOptions());

            connection.openFuture().get();
            // Should close normally and not throw error as we initiated the close.
            connection.close();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testConnectionClosedWithErrorToRemoteSync() throws Exception {
        doTestConnectionClosedWithErrorToRemote(false);
    }

    @Test
    public void testConnectionClosedWithErrorToRemoteAsync() throws Exception {
        doTestConnectionClosedWithErrorToRemote(true);
    }

    private void doTestConnectionClosedWithErrorToRemote(boolean async) throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer(testServerOptions())) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectClose().withError(ConnectionError.CONNECTION_FORCED.toString(), "Closed").respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Connect test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), connectionOptions());

            connection.openFuture().get();
            if (async) {
                connection.closeAsync(ErrorCondition.create(ConnectionError.CONNECTION_FORCED.toString(), "Closed")).get();
            } else {
                connection.close(ErrorCondition.create(ConnectionError.CONNECTION_FORCED.toString(), "Closed"));
            }

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testConnectionRemoteClosedAfterOpened() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer(testServerOptions())) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().reject(ConnectionError.CONNECTION_FORCED.toString(), "Not accepting connections");
            peer.expectClose();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Connect test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), connectionOptions());

            connection.openFuture().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            connection.closeAsync().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete();
        }
    }

    @Test
    public void testConnectionRemoteClosedAfterOpenedWithEmptyErrorConditionDescription() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer(testServerOptions())) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().reject(ConnectionError.CONNECTION_FORCED.toString(), (String) null);
            peer.expectClose();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Connect test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), connectionOptions());

            connection.openFuture().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            connection.closeAsync().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete();
        }
    }

    @Test
    public void testConnectionRemoteClosedAfterOpenedWithNoRemoteErrorCondition() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer(testServerOptions())) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().reject();
            peer.expectClose();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Connect test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), connectionOptions());

            connection.openFuture().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            connection.closeAsync().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete();
        }
    }

    @Test
    public void testConnectionOpenFutureWaitCancelledOnConnectionDropWithTimeout() throws Exception {
        doTestConnectionOpenFutureWaitCancelledOnConnectionDrop(true);
    }

    @Test
    public void testConnectionOpenFutureWaitCancelledOnConnectionDropNoTimeout() throws Exception {
        doTestConnectionOpenFutureWaitCancelledOnConnectionDrop(false);
    }

    protected void doTestConnectionOpenFutureWaitCancelledOnConnectionDrop(boolean timeout) throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer(testServerOptions())) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Connect test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), connectionOptions());

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.close();

            try {
                if (timeout) {
                    connection.openFuture().get(10, TimeUnit.SECONDS);
                } else {
                    connection.openFuture().get();
                }
                fail("Should have thrown an execution error due to connection drop");
            } catch (ExecutionException error) {
                LOG.info("connection open failed with error: ", error);
            }

            try {
                if (timeout) {
                    connection.closeAsync().get(10, TimeUnit.SECONDS);
                } else {
                    connection.closeAsync().get();
                }
            } catch (Throwable error) {
                LOG.info("connection close failed with error: ", error);
                fail("Close should ignore connect error and complete without error.");
            }

            peer.waitForScriptToComplete();
        }
    }

    @Test
    public void testRemotelyCloseConnectionDuringSessionCreation() throws Exception {
        final String BREAD_CRUMB = "ErrorMessageBreadCrumb";

        try (ProtonTestServer peer = new ProtonTestServer(testServerOptions())) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin();
            peer.remoteClose().withErrorCondition(AmqpError.NOT_ALLOWED.toString(), BREAD_CRUMB).queue();
            peer.expectClose();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), connectionOptions());
            connection.openFuture().get();

            Session session = connection.openSession();

            try {
                session.openFuture().get();
                fail("Open should throw error when waiting for remote open and connection remotely closed.");
            } catch (ExecutionException error) {
                LOG.info("Session open failed with error: ", error);
                assertNotNull(error.getMessage(), "Expected exception to have a message");
                assertTrue(error.getMessage().contains(BREAD_CRUMB), "Expected breadcrumb to be present in message");
                assertNotNull(error.getCause(), "Execution error should convey the cause");
                assertTrue(error.getCause() instanceof ClientConnectionRemotelyClosedException);
            }

            session.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testConnectionOpenTimeoutWhenNoRemoteOpenArrivesTimeout() throws Exception {
        doTestConnectionOpenTimeoutWhenNoRemoteOpenArrives(true);
    }

    @Test
    public void testConnectionOpenTimeoutWhenNoRemoteOpenArrivesNoTimeout() throws Exception {
        doTestConnectionOpenTimeoutWhenNoRemoteOpenArrives(false);
    }

    private void doTestConnectionOpenTimeoutWhenNoRemoteOpenArrives(boolean timeout) throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer(testServerOptions())) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen();
            peer.expectClose();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            final ConnectionOptions options = connectionOptions().openTimeout(75);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), options);

            try {
                if (timeout) {
                    connection.openFuture().get(10, TimeUnit.SECONDS);
                } else {
                    connection.openFuture().get();
                }

                fail("Open should timeout when no open response and complete future with error.");
            } catch (Throwable error) {
                LOG.info("connection open failed with error: ", error);
            }

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testConnectionOpenWaitWithTimeoutCanceledWhenConnectionDrops() throws Exception {
        doTestConnectionOpenWaitCanceledWhenConnectionDrops(true);
    }

    @Test
    public void testConnectionOpenWaitWithNoTimeoutCanceledWhenConnectionDrops() throws Exception {
        doTestConnectionOpenWaitCanceledWhenConnectionDrops(false);
    }

    private void doTestConnectionOpenWaitCanceledWhenConnectionDrops(boolean timeout) throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer(testServerOptions())) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen();
            peer.dropAfterLastHandler(10);
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);
            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), connectionOptions());

            try {
                if (timeout) {
                    connection.openFuture().get(10, TimeUnit.SECONDS);
                } else {
                    connection.openFuture().get();
                }

                fail("Open should timeout when no open response and complete future with error.");
            } catch (ExecutionException error) {
                LOG.info("connection open failed with error: ", error);
                assertTrue(error.getCause() instanceof ClientIOException);
            }

            connection.client();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testConnectionCloseTimeoutWhenNoRemoteCloseArrivesTimeout() throws Exception {
        doTestConnectionCloseTimeoutWhenNoRemoteCloseArrives(true);
    }

    @Test
    public void testConnectionCloseTimeoutWhenNoRemoteCloseArrivesNoTimeout() throws Exception {
        doTestConnectionCloseTimeoutWhenNoRemoteCloseArrives(false);
    }

    private void doTestConnectionCloseTimeoutWhenNoRemoteCloseArrives(boolean timeout) throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer(testServerOptions())) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectClose();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            final ConnectionOptions options = connectionOptions().closeTimeout(75);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), options);

            connection.openFuture().get(10, TimeUnit.SECONDS);

            // Shouldn't throw from close, nothing to be done anyway.
            try {
                if (timeout) {
                    connection.closeAsync().get(10, TimeUnit.SECONDS);
                } else {
                    connection.closeAsync().get();
                }
            } catch (Throwable error) {
                LOG.info("connection close failed with error: ", error);
                fail("Close should ignore lack of close response and complete without error.");
            }

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testConnectionCloseWaitWithTimeoutCompletesAfterRemoteConnectionDrops() throws Exception {
        doTestConnectionCloseWaitCompletesAfterRemoteConnectionDrops(true);
    }

    @Test
    public void testConnectionCloseWaitWithNoTimeoutCompletesAfterRemoteConnectionDrops() throws Exception {
        doTestConnectionCloseWaitCompletesAfterRemoteConnectionDrops(false);
    }

    private void doTestConnectionCloseWaitCompletesAfterRemoteConnectionDrops(boolean timeout) throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer(testServerOptions())) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectClose();
            peer.dropAfterLastHandler(10);
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), connectionOptions());

            connection.openFuture().get(10, TimeUnit.SECONDS);

            // Shouldn't throw from close, nothing to be done anyway.
            try {
                if (timeout) {
                    connection.closeAsync().get(10, TimeUnit.SECONDS);
                } else {
                    connection.closeAsync().get();
                }
            } catch (Throwable error) {
                LOG.info("connection close failed with error: ", error);
                fail("Close should treat Connection drop as success and complete without error.");
            }

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testCreateDefaultSenderFailsOnConnectionWithoutSupportForAnonymousRelay() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer(testServerOptions())) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Connect test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(
                remoteURI.getHost(), remoteURI.getPort(), connectionOptions()).openFuture().get();

            try {
                connection.defaultSender();
                fail("Should not be able to get the default sender when remote does not offer anonymous relay");
            } catch (ClientUnsupportedOperationException unsupported) {
                LOG.info("Caught expected error: ", unsupported);
            }

            connection.close();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testCreateDefaultSenderOnConnectionWithSupportForAnonymousRelay() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer(testServerOptions())) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().withDesiredCapabilities(ClientConstants.ANONYMOUS_RELAY.toString())
                             .respond()
                             .withOfferedCapabilities(ClientConstants.ANONYMOUS_RELAY.toString());
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.SENDER.getValue()).respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Connect test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), connectionOptions());
            connection.openFuture().get(10, TimeUnit.SECONDS);

            Sender defaultSender = connection.defaultSender().openFuture().get(5, TimeUnit.SECONDS);
            assertNotNull(defaultSender);

            connection.close();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testConnectionRecreatesAnonymousRelaySenderAfterRemoteCloseOfSender() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer(testServerOptions())) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().withDesiredCapabilities(ClientConstants.ANONYMOUS_RELAY.toString())
                             .respond()
                             .withOfferedCapabilities(ClientConstants.ANONYMOUS_RELAY.toString());
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.SENDER.getValue()).respond();
            peer.remoteDetach().queue();
            peer.expectDetach();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Connect test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), connectionOptions());
            connection.openFuture().get(10, TimeUnit.SECONDS);

            Sender defaultSender = connection.defaultSender().openFuture().get(5, TimeUnit.SECONDS);
            assertNotNull(defaultSender);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectAttach().withRole(Role.SENDER.getValue()).respond();
            peer.expectClose().respond();

            defaultSender = connection.defaultSender().openFuture().get(5, TimeUnit.SECONDS);
            assertNotNull(defaultSender);

            connection.close();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testCreateDynamicReceiver() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer(testServerOptions())) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.RECEIVER.getValue())
                               .withSource(new SourceMatcher().withDynamic(true).withAddress(nullValue()))
                               .respond();
            peer.expectFlow();
            peer.expectDetach().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Connect test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), connectionOptions());
            connection.openFuture().get(10, TimeUnit.SECONDS);

            Receiver receiver = connection.openDynamicReceiver();
            receiver.openFuture().get(10, TimeUnit.SECONDS);

            assertNotNull(receiver.address(), "Remote should have assigned the address for the dynamic receiver");

            receiver.closeAsync().get(10, TimeUnit.SECONDS);

            connection.closeAsync().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testConnectionSenderOpenHeldUntilConnectionOpenedAndRelaySupportConfirmed() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer(testServerOptions())) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen();
            peer.expectBegin();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), connectionOptions());
            Sender sender = connection.defaultSender();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            // This should happen after we inject the held open and attach
            peer.expectAttach().withRole(Role.SENDER.getValue()).withTarget().withAddress(Matchers.nullValue()).and().respond();
            peer.expectClose().respond();

            // Inject held responses to get the ball rolling again
            peer.remoteOpen().withOfferedCapabilities("ANONYMOUS-RELAY").now();
            peer.respondToLastBegin().now();

            try {
                sender.openFuture().get();
            } catch (ExecutionException ex) {
                fail("Open of Sender failed waiting for response: " + ex.getCause());
            }

            connection.close();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testConnectionSenderIsSingleton() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer(testServerOptions())) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond().withOfferedCapabilities("ANONYMOUS-RELAY");
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.SENDER.getValue()).withTarget().withAddress(Matchers.nullValue()).and().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), connectionOptions());
            Sender sender1 = connection.defaultSender();
            Sender sender2 = connection.defaultSender();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectClose().respond();

            try {
                sender1.openFuture().get();
            } catch (ExecutionException ex) {
                fail("Open of Sender failed waiting for response: " + ex.getCause());
            }

            try {
                sender2.openFuture().get();
            } catch (ExecutionException ex) {
                fail("Open of Sender failed waiting for response: " + ex.getCause());
            }

            assertSame(sender1, sender2);

            connection.close();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testConnectionSenderOpenFailsWhenAnonymousRelayNotSupported() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer(testServerOptions())) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), connectionOptions());
            Sender sender = connection.defaultSender();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectClose().respond();

            try {
                sender.openFuture().get();
                fail("Open of Sender should have failed waiting for response when anonymous relay not supported");
            } catch (ExecutionException ex) {
            }

            connection.close();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testConnectionGetRemotePropertiesWaitsForRemoteBegin() throws Exception {
        tryReadConnectionRemoteProperties(true);
    }

    @Test
    public void testConnectionGetRemotePropertiesFailsAfterOpenTimeout() throws Exception {
        tryReadConnectionRemoteProperties(false);
    }

    private void tryReadConnectionRemoteProperties(boolean openResponse) throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer(testServerOptions())) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Connect test started, peer listening on: {}", remoteURI);

            Client container = Client.create();

            ConnectionOptions options = connectionOptions().openTimeout(100);
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), options);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            Map<String, Object> expectedProperties = new HashMap<>();
            expectedProperties.put("TEST", "test-property");

            if (openResponse) {
                peer.expectClose().respond();
                peer.remoteOpen().withProperties(expectedProperties).later(10);
            } else {
                peer.expectClose();
            }

            if (openResponse) {
                assertNotNull(connection.properties(), "Remote should have responded with a remote properties value");
                assertEquals(expectedProperties, connection.properties());
            } else {
                try {
                    connection.properties();
                    fail("Should failed to get remote state due to no open response");
                } catch (ClientException ex) {
                    LOG.debug("Caught expected exception from blocking call", ex);
                }
            }

            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testConnectionGetRemoteOfferedCapabilitiesWaitsForRemoteBegin() throws Exception {
        tryReadConnectionRemoteOfferedCapabilities(true);
    }

    @Test
    public void testConnectionGetRemoteOfferedCapabilitiesFailsAfterOpenTimeout() throws Exception {
        tryReadConnectionRemoteOfferedCapabilities(false);
    }

    private void tryReadConnectionRemoteOfferedCapabilities(boolean openResponse) throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer(testServerOptions())) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Connect test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            ConnectionOptions options = connectionOptions().openTimeout(100);
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), options);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            if (openResponse) {
                peer.expectClose().respond();
                peer.remoteOpen().withOfferedCapabilities("transactions").later(10);
            } else {
                peer.expectClose();
            }

            if (openResponse) {
                assertNotNull(connection.offeredCapabilities(), "Remote should have responded with a remote offered Capabilities value");
                assertEquals(1, connection.offeredCapabilities().length);
                assertEquals("transactions", connection.offeredCapabilities()[0]);
            } else {
                try {
                    connection.offeredCapabilities();
                    fail("Should failed to get remote state due to no open response");
                } catch (ClientException ex) {
                    LOG.debug("Caught expected exception from blocking call", ex);
                }
            }

            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testConnectionGetRemoteDesiredCapabilitiesWaitsForRemoteBegin() throws Exception {
        tryReadConnectionRemoteDesiredCapabilities(true);
    }

    @Test
    public void testConnectionGetRemoteDesiredCapabilitiesFailsAfterOpenTimeout() throws Exception {
        tryReadConnectionRemoteDesiredCapabilities(false);
    }

    private void tryReadConnectionRemoteDesiredCapabilities(boolean openResponse) throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer(testServerOptions())) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Connect test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            ConnectionOptions options = connectionOptions().openTimeout(100);
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), options);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            if (openResponse) {
                peer.expectClose().respond();
                peer.remoteOpen().withDesiredCapabilities("Error-Free").later(10);
            } else {
                peer.expectClose();
            }

            if (openResponse) {
                assertNotNull(connection.desiredCapabilities(), "Remote should have responded with a remote desired Capabilities value");
                assertEquals(1, connection.desiredCapabilities().length);
                assertEquals("Error-Free", connection.desiredCapabilities()[0]);
            } else {
                try {
                    connection.desiredCapabilities();
                    fail("Should failed to get remote state due to no open response");
                } catch (ClientException ex) {
                    LOG.debug("Caught expected exception from blocking call", ex);
                }
            }

            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testCloseWithErrorCondition() throws Exception {
        final String condition = "amqp:precondition-failed";
        final String description = "something bad happened.";

        try (ProtonTestServer peer = new ProtonTestServer(testServerOptions())) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectClose().withError(condition, description).respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(
                remoteURI.getHost(), remoteURI.getPort(), connectionOptions()).openFuture().get();

            connection.close(ErrorCondition.create(condition, description, null));

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testAnonymousSenderOpenHeldUntilConnectionOpenedAndSupportConfirmed() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen();
            peer.expectBegin();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Sender sender = connection.openAnonymousSender();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            // This should happen after we inject the held open and attach
            peer.expectAttach().ofSender().withTarget().withAddress(Matchers.nullValue()).and().respond();
            peer.expectClose().respond();

            // Inject held responses to get the ball rolling again
            peer.remoteOpen().withOfferedCapabilities("ANONYMOUS-RELAY").now();
            peer.respondToLastBegin().now();

            try {
                sender.openFuture().get();
            } catch (ExecutionException ex) {
                fail("Open of Sender failed waiting for response: " + ex.getCause());
            }

            connection.closeAsync();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSendHeldUntilConnectionOpenedAndSupportConfirmed() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond().withOfferedCapabilities("ANONYMOUS-RELAY");
            peer.expectBegin().respond();
            peer.expectAttach().ofSender().withTarget().withAddress(nullValue()).and().respond();
            peer.remoteFlow().withLinkCredit(1).queue();
            peer.expectTransfer().withNonNullPayload()
                                 .withDeliveryTag(new byte[] {0}).respond().withSettled(true).withState().accepted();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());

            try {
                Tracker tracker = connection.send(Message.create("Hello World"));
                assertNotNull(tracker);
                tracker.awaitAccepted();
            } catch (ClientException ex) {
                fail("Open of Sender failed waiting for response: " + ex.getCause());
            }

            connection.close();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testConnectionLevelSendFailsWhenAnonymousRelayNotAdvertisedByRemote() throws Exception {
        doTestConnectionLevelSendFailsWhenAnonymousRelayNotAdvertisedByRemote(false);
    }

    @Test
    public void testConnectionLevelSendFailsWhenAnonymousRelayNotAdvertisedByRemoteAfterAlreadyOpened() throws Exception {
        doTestConnectionLevelSendFailsWhenAnonymousRelayNotAdvertisedByRemote(true);
    }

    private void doTestConnectionLevelSendFailsWhenAnonymousRelayNotAdvertisedByRemote(boolean openWait) throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            // Ensures that the Begin arrives regard of a race on open without anonymous relay support
            connection.defaultSession();

            if (openWait) {
                connection.openFuture().get();
            }

            try {
                connection.send(Message.create("Hello World"));
                fail("Open of Sender should fail as remote did not advertise anonymous relay support: ");
            } catch (ClientUnsupportedOperationException ex) {
            }

            connection.close();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testOpenAnonymousSenderFailsWhenAnonymousRelayNotAdvertisedByRemote() throws Exception {
        doTestOpenAnonymousSenderFailsWhenAnonymousRelayNotAdvertisedByRemote(false);
    }

    @Test
    public void testOpenAnonymousSenderFailsWhenAnonymousRelayNotAdvertisedByRemoteAfterAlreadyOpened() throws Exception {
        doTestOpenAnonymousSenderFailsWhenAnonymousRelayNotAdvertisedByRemote(true);
    }

    private void doTestOpenAnonymousSenderFailsWhenAnonymousRelayNotAdvertisedByRemote(boolean openWait) throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            // Ensures that the Begin arrives regard of a race on open without anonymous relay support
            connection.defaultSession();

            if (openWait) {
                connection.openFuture().get();
            }

            try {
                connection.openAnonymousSender().openFuture().get();
                fail("Open of Sender should fail as remote did not advertise anonymous relay support: ");
            } catch (ClientUnsupportedOperationException ex) {
            } catch (ExecutionException ex) {
                assertTrue(ex.getCause() instanceof ClientUnsupportedOperationException);
            }

            connection.close();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testOpenDurableReceiverFromConnection() throws Exception {
        final String address = "test-topic";
        final String subscriptionName = "mySubscriptionName";

        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofReceiver()
                               .withName(subscriptionName)
                               .withSource()
                                   .withAddress(address)
                                   .withDurable(TerminusDurability.UNSETTLED_STATE)
                                   .withExpiryPolicy(TerminusExpiryPolicy.NEVER)
                                   .withDistributionMode("copy")
                               .and().respond();
            peer.expectFlow();
            peer.expectDetach().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Receiver receiver = connection.openDurableReceiver(address, subscriptionName);

            receiver.openFuture().get();
            receiver.closeAsync().get();

            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Disabled("Disabled due to requirement of hard coded port")
    @Test
    public void testLocalPortOption() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            final int localPort = 5671;

            Client container = Client.create();
            ConnectionOptions options = new ConnectionOptions();
            options.transportOptions().localPort(localPort);
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), options);

            connection.openFuture().get();

            assertEquals(localPort, peer.getConnectionRemotePort());

            connection.close();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    protected ProtonTestServerOptions testServerOptions() {
        return new ProtonTestServerOptions();
    }

    protected ConnectionOptions connectionOptions() {
        return new ConnectionOptions();
    }

    protected ConnectionOptions connectionOptions(String user, String password) {
        return new ConnectionOptions().user(user).password(password);
    }
}
