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

import static org.hamcrest.Matchers.any;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.ConnectionOptions;
import org.apache.qpid.protonj2.client.Session;
import org.apache.qpid.protonj2.client.exceptions.ClientConnectionRemotelyClosedException;
import org.apache.qpid.protonj2.client.exceptions.ClientConnectionSecurityException;
import org.apache.qpid.protonj2.client.exceptions.ClientConnectionSecuritySaslException;
import org.apache.qpid.protonj2.client.exceptions.ClientIllegalStateException;
import org.apache.qpid.protonj2.client.test.ImperativeClientTestCase;
import org.apache.qpid.protonj2.client.util.StopWatch;
import org.apache.qpid.protonj2.test.driver.ProtonTestServer;
import org.apache.qpid.protonj2.types.security.SaslCode;
import org.apache.qpid.protonj2.types.transport.AmqpError;
import org.apache.qpid.protonj2.types.transport.ConnectionError;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Test client implementation of basic connection recovery and its configuration.
 */
@Timeout(20)
public class ReconnectTest extends ImperativeClientTestCase {

    @Test
    public void testConnectionNotifiesReconnectionLifecycleEvents() throws Exception {
        try (ProtonTestServer firstPeer = new ProtonTestServer();
             ProtonTestServer finalPeer = new ProtonTestServer()) {

            firstPeer.expectSASLAnonymousConnect();
            firstPeer.expectOpen().withContainerId(any(String.class)).respond();
            firstPeer.shutdownAfterLastHandler(5);
            firstPeer.start();

            finalPeer.expectSASLAnonymousConnect();
            finalPeer.expectOpen().withContainerId(any(String.class)).respond();
            finalPeer.start();

            final URI primaryURI = firstPeer.getServerURI();
            final URI backupURI = finalPeer.getServerURI();

            final CountDownLatch connected = new CountDownLatch(1);
            final CountDownLatch disconnected = new CountDownLatch(1);
            final CountDownLatch reconnected = new CountDownLatch(1);
            final CountDownLatch failed = new CountDownLatch(1);

            ConnectionOptions options = new ConnectionOptions();
            options.reconnectOptions().reconnectEnabled(true);
            options.reconnectOptions().maxReconnectAttempts(5);
            options.reconnectOptions().reconnectDelay(10);
            options.reconnectOptions().useReconnectBackOff(false);
            options.reconnectOptions().addReconnectLocation(backupURI.getHost(), backupURI.getPort());
            options.connectedHandler((connection, context) -> {
                connected.countDown();
            });
            options.interruptedHandler((connection, context) -> {
                disconnected.countDown();
            });
            options.reconnectedHandler((connection, context) -> {
                reconnected.countDown();
            });
            options.disconnectedHandler((connection, context) -> {
                failed.countDown();
            });

            Client container = Client.create();
            Connection connection = container.connect(primaryURI.getHost(), primaryURI.getPort(), options);

            firstPeer.waitForScriptToComplete();

            connection.openFuture().get();

            finalPeer.waitForScriptToComplete();
            finalPeer.expectBegin().respond();
            finalPeer.expectEnd().respond();
            finalPeer.shutdownAfterLastHandler(10);

            Session session = connection.openSession().openFuture().get();

            session.close();

            finalPeer.waitForScriptToComplete();

            assertTrue(connected.await(5, TimeUnit.SECONDS));
            assertTrue(disconnected.await(5, TimeUnit.SECONDS));
            assertTrue(reconnected.await(5, TimeUnit.SECONDS));
            assertTrue(failed.await(5, TimeUnit.SECONDS));

            connection.close();

            finalPeer.waitForScriptToComplete();
        }
    }

    @Test
    public void testConnectThrowsSecurityViolationOnFailureSaslAuth() throws Exception {
        doTestConnectThrowsSecurityViolationOnFailureSaslExchange(SaslCode.AUTH.byteValue());
    }

    @Test
    public void testConnectThrowsSecurityViolationOnFailureSaslSys() throws Exception {
        doTestConnectThrowsSecurityViolationOnFailureSaslExchange(SaslCode.SYS.byteValue());
    }

    @Test
    public void testConnectThrowsSecurityViolationOnFailureSaslSysPerm() throws Exception {
        doTestConnectThrowsSecurityViolationOnFailureSaslExchange(SaslCode.SYS_PERM.byteValue());
    }

    private void doTestConnectThrowsSecurityViolationOnFailureSaslExchange(byte saslCode) throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectFailingSASLPlainConnect(saslCode);
            peer.dropAfterLastHandler(10);
            peer.start();

            ConnectionOptions options = new ConnectionOptions();
            options.reconnectOptions().reconnectEnabled(true);
            options.user("test");
            options.password("pass");

            URI remoteURI = peer.getServerURI();

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), options);

            try {
                connection.openFuture().get();
            } catch (ExecutionException exe) {
                assertTrue(exe.getCause() instanceof ClientConnectionSecuritySaslException);
            }

            connection.close();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testReconnectStopsAfterSaslAuthFailure() throws Exception {
        testReconnectStopsAfterSaslPermFailure(SaslCode.AUTH.byteValue());
    }

    @Test
    public void testReconnectStopsAfterSaslSysFailure() throws Exception {
        testReconnectStopsAfterSaslPermFailure(SaslCode.SYS.byteValue());
    }

    @Test
    public void testReconnectStopsAfterSaslPermFailure() throws Exception {
        testReconnectStopsAfterSaslPermFailure(SaslCode.SYS_PERM.byteValue());
    }

    private void testReconnectStopsAfterSaslPermFailure(byte saslCode) throws Exception {
        try (ProtonTestServer firstPeer = new ProtonTestServer();
             ProtonTestServer secondPeer = new ProtonTestServer();
             ProtonTestServer thirdPeer = new ProtonTestServer()) {

            firstPeer.expectSASLAnonymousConnect();
            firstPeer.expectOpen().respond();
            firstPeer.dropAfterLastHandler();
            firstPeer.start();

            secondPeer.expectSASLAnonymousConnect();
            secondPeer.expectOpen();
            secondPeer.dropAfterLastHandler();
            secondPeer.start();

            thirdPeer.expectFailingSASLPlainConnect(saslCode);
            thirdPeer.dropAfterLastHandler();
            thirdPeer.start();

            final CountDownLatch connected = new CountDownLatch(1);
            final CountDownLatch disconnected = new CountDownLatch(1);
            final CountDownLatch reconnected = new CountDownLatch(1);
            final CountDownLatch failed = new CountDownLatch(1);

            final URI firstURI = firstPeer.getServerURI();
            final URI secondURI = secondPeer.getServerURI();
            final URI thirdURI = thirdPeer.getServerURI();

            Client container = Client.create();
            ConnectionOptions options = new ConnectionOptions();
            options.user("test");
            options.password("pass");
            options.reconnectOptions().reconnectEnabled(true);
            options.reconnectOptions().addReconnectLocation(secondURI.getHost(), secondURI.getPort())
                                      .addReconnectLocation(thirdURI.getHost(), thirdURI.getPort());
            options.connectedHandler((connection, context) -> {
                connected.countDown();
            });
            options.interruptedHandler((connection, context) -> {
                disconnected.countDown();
            });
            options.reconnectedHandler((connection, context) -> {
                reconnected.countDown();  // This one should not be triggered
            });
            options.disconnectedHandler((connection, context) -> {
                failed.countDown();
            });

            Connection connection = container.connect(firstURI.getHost(), firstURI.getPort(), options);

            connection.openFuture().get();

            firstPeer.waitForScriptToComplete(6, TimeUnit.SECONDS);
            secondPeer.waitForScriptToComplete(6, TimeUnit.SECONDS);
            thirdPeer.waitForScriptToComplete(6, TimeUnit.SECONDS);

            // Should connect, then fail and attempt to connect to second and third before stopping
            assertTrue(connected.await(5, TimeUnit.SECONDS));
            assertTrue(disconnected.await(5, TimeUnit.SECONDS));
            assertTrue(failed.await(5, TimeUnit.SECONDS));
            assertEquals(1, reconnected.getCount());

            connection.close();

            thirdPeer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testConnectHandlesSaslTempFailureAndReconnects() throws Exception {
        try (ProtonTestServer firstPeer = new ProtonTestServer();
             ProtonTestServer finalPeer = new ProtonTestServer()) {

            firstPeer.expectFailingSASLPlainConnect(SaslCode.SYS_TEMP.byteValue());
            firstPeer.dropAfterLastHandler();
            firstPeer.start();

            finalPeer.expectSASLPlainConnect("test", "pass");
            finalPeer.expectOpen().respond();
            finalPeer.start();

            final URI primaryURI = firstPeer.getServerURI();
            final URI backupURI = finalPeer.getServerURI();

            final CountDownLatch connected = new CountDownLatch(1);
            final AtomicReference<String> connectedHost = new AtomicReference<>();
            final AtomicReference<Integer> connectedPort = new AtomicReference<>();

            ConnectionOptions options = new ConnectionOptions();
            options.user("test");
            options.password("pass");
            options.reconnectOptions().reconnectEnabled(true);
            options.reconnectOptions().addReconnectLocation(backupURI.getHost(), backupURI.getPort());
            options.connectedHandler((connection, event) -> {
                connectedHost.set(event.host());
                connectedPort.set(event.port());
                connected.countDown();
            });

            Client container = Client.create();
            Connection connection = container.connect(primaryURI.getHost(), primaryURI.getPort(), options);

            firstPeer.waitForScriptToComplete();

            connection.openFuture().get();

            assertTrue(connected.await(5, TimeUnit.SECONDS));

            // Should never have connected and exchanged Open performatives with first peer
            // so we won't have had a connection established event there.
            assertEquals(backupURI.getHost(), connectedHost.get());
            assertEquals(backupURI.getPort(), connectedPort.get());

            finalPeer.waitForScriptToComplete();

            finalPeer.expectClose().respond();
            connection.close();

            finalPeer.waitForScriptToComplete();
        }
    }

    @Test
    public void testConnectThrowsSecurityViolationOnFailureFromOpen() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().reject(AmqpError.UNAUTHORIZED_ACCESS.toString(), "Anonymous connections not allowed");
            peer.expectBegin().optional();  // Could arrive if remote open response not processed in time
            peer.expectClose();
            peer.start();

            ConnectionOptions options = new ConnectionOptions();
            options.reconnectOptions().reconnectEnabled(true);

            URI remoteURI = peer.getServerURI();

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), options);

            try {
                connection.openFuture().get();
            } catch (ExecutionException exe) {
                // Possible based on time of rejecting open arrival.
                assertTrue(exe.getCause() instanceof ClientConnectionSecurityException);
            }

            try {
                connection.defaultSession().openFuture().get();
                fail("Should fail connection since remote rejected open with auth error");
            } catch (ClientConnectionSecurityException cliEx) {
            } catch (ExecutionException exe) {
                assertTrue(exe.getCause() instanceof ClientConnectionSecurityException);
            }

            connection.close();

            try {
                connection.defaultSession();
                fail("Should fail as illegal state as connection was closed.");
            } catch (ClientIllegalStateException exe) {
            }

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testReconnectHandlesDropThenRejectionCloseAfterConnect() throws Exception {
        try (ProtonTestServer firstPeer = new ProtonTestServer();
            ProtonTestServer secondPeer = new ProtonTestServer();
            ProtonTestServer thirdPeer = new ProtonTestServer()) {

           firstPeer.expectSASLAnonymousConnect();
           firstPeer.expectOpen().respond();
           firstPeer.start();

           secondPeer.expectSASLAnonymousConnect();
           secondPeer.expectOpen().reject(AmqpError.INVALID_FIELD.toString(), "Connection configuration has invalid field");
           secondPeer.expectClose();
           secondPeer.start();

           thirdPeer.expectSASLAnonymousConnect();
           thirdPeer.expectOpen().respond();
           thirdPeer.start();

           final CountDownLatch connected = new CountDownLatch(1);
           final CountDownLatch disconnected = new CountDownLatch(2);
           final CountDownLatch reconnected = new CountDownLatch(2);
           final CountDownLatch failed = new CountDownLatch(1);

           final URI firstURI = firstPeer.getServerURI();
           final URI secondURI = secondPeer.getServerURI();
           final URI thirdURI = thirdPeer.getServerURI();

           ConnectionOptions options = new ConnectionOptions();
           options.reconnectOptions().reconnectEnabled(true);
           options.reconnectOptions().addReconnectLocation(secondURI.getHost(), secondURI.getPort())
                                     .addReconnectLocation(thirdURI.getHost(), thirdURI.getPort());
           options.connectedHandler((connection, context) -> {
               connected.countDown();
           });
           options.interruptedHandler((connection, context) -> {
               disconnected.countDown();
           });
           options.reconnectedHandler((connection, context) -> {
               reconnected.countDown();
           });
           options.disconnectedHandler((connection, context) -> {
               failed.countDown();  // Not expecting any failure in this test case
           });

           Connection connection = Client.create().connect(firstURI.getHost(), firstURI.getPort(), options);

           firstPeer.waitForScriptToComplete(5, TimeUnit.SECONDS);

           connection.openFuture().get();

           firstPeer.close();

           secondPeer.waitForScriptToComplete(5, TimeUnit.SECONDS);

           // Should connect, then fail and attempt to connect to second and be rejected then reconnect to third.
           assertTrue(connected.await(5, TimeUnit.SECONDS));
           assertTrue(disconnected.await(5, TimeUnit.SECONDS));
           assertTrue(reconnected.await(5, TimeUnit.SECONDS));
           assertEquals(1, failed.getCount());

           thirdPeer.expectClose().respond();
           connection.close();

           thirdPeer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testClientReconnectsWhenConnectionDropsAfterOpenReceived() throws Exception {
        doTestClientReconnectsWhenConnectionDropsAfterOpenReceived(0);
    }

    @Test
    public void testClientReconnectsWhenConnectionDropsAfterDelayAfterOpenReceived() throws Exception {
        doTestClientReconnectsWhenConnectionDropsAfterOpenReceived(20);
    }

    private void doTestClientReconnectsWhenConnectionDropsAfterOpenReceived(int dropDelay) throws Exception {
        try (ProtonTestServer firstPeer = new ProtonTestServer();
             ProtonTestServer finalPeer = new ProtonTestServer()) {

            firstPeer.expectSASLAnonymousConnect();
            firstPeer.expectOpen();
            if (dropDelay > 0) {
                firstPeer.dropAfterLastHandler(dropDelay);
            } else {
                firstPeer.dropAfterLastHandler();
            }
            firstPeer.start();

            finalPeer.expectSASLAnonymousConnect();
            finalPeer.expectOpen().respond();
            finalPeer.start();

            final URI primaryURI = firstPeer.getServerURI();
            final URI backupURI = finalPeer.getServerURI();

            final CountDownLatch connected = new CountDownLatch(1);
            final AtomicReference<String> connectedHost = new AtomicReference<>();
            final AtomicReference<Integer> connectedPort = new AtomicReference<>();

            ConnectionOptions options = new ConnectionOptions();
            options.reconnectOptions().reconnectEnabled(true);
            options.reconnectOptions().addReconnectLocation(backupURI.getHost(), backupURI.getPort());
            options.connectedHandler((connection, event) -> {
                connectedHost.set(event.host());
                connectedPort.set(event.port());
                connected.countDown();
            });

            Client container = Client.create();
            Connection connection = container.connect(primaryURI.getHost(), primaryURI.getPort(), options);

            firstPeer.waitForScriptToComplete();

            connection.openFuture().get();

            assertTrue(connected.await(5, TimeUnit.SECONDS));
            assertEquals(backupURI.getHost(), connectedHost.get());
            assertEquals(backupURI.getPort(), connectedPort.get());

            finalPeer.waitForScriptToComplete();

            finalPeer.expectClose().respond();
            connection.close();

            finalPeer.waitForScriptToComplete();
        }
    }

    @Test
    public void testClientReconnectsWhenOpenRejected() throws Exception {
        try (ProtonTestServer firstPeer = new ProtonTestServer();
             ProtonTestServer finalPeer = new ProtonTestServer()) {

            firstPeer.expectSASLAnonymousConnect();
            firstPeer.expectOpen().reject(AmqpError.INVALID_FIELD.toString(), "Error with client Open performative");
            firstPeer.expectClose();
            firstPeer.start();

            finalPeer.expectSASLAnonymousConnect();
            finalPeer.expectOpen().respond();
            finalPeer.start();

            final URI primaryURI = firstPeer.getServerURI();
            final URI backupURI = finalPeer.getServerURI();

            final CountDownLatch connected = new CountDownLatch(1);
            final AtomicReference<String> connectedHost = new AtomicReference<>();
            final AtomicReference<Integer> connectedPort = new AtomicReference<>();

            ConnectionOptions options = new ConnectionOptions();
            options.reconnectOptions().reconnectEnabled(true);
            options.reconnectOptions().addReconnectLocation(backupURI.getHost(), backupURI.getPort());
            options.connectedHandler((connection, event) -> {
                connectedHost.set(event.host());
                connectedPort.set(event.port());
                connected.countDown();
            });

            Client container = Client.create();
            Connection connection = container.connect(primaryURI.getHost(), primaryURI.getPort(), options);

            firstPeer.waitForScriptToComplete();

            connection.openFuture().get();

            assertTrue(connected.await(5, TimeUnit.SECONDS));
            assertEquals(primaryURI.getHost(), connectedHost.get());
            assertEquals(primaryURI.getPort(), connectedPort.get());

            finalPeer.waitForScriptToComplete();

            finalPeer.expectClose().respond();
            connection.close();

            finalPeer.waitForScriptToComplete();
        }
    }

    @Test
    public void testClientReconnectsWhenConnectionRemotelyClosedWithForced() throws Exception {
        try (ProtonTestServer firstPeer = new ProtonTestServer();
             ProtonTestServer finalPeer = new ProtonTestServer()) {

            firstPeer.expectSASLAnonymousConnect();
            firstPeer.expectOpen().respond();
            firstPeer.expectBegin();
            firstPeer.remoteClose().withErrorCondition(ConnectionError.CONNECTION_FORCED.toString(), "Forced disconnect").queue();
            firstPeer.expectClose();
            firstPeer.start();

            finalPeer.expectSASLAnonymousConnect();
            finalPeer.expectOpen().respond();
            finalPeer.expectBegin().respond();
            finalPeer.start();

            final URI primaryURI = firstPeer.getServerURI();
            final URI backupURI = finalPeer.getServerURI();

            final CountDownLatch connected = new CountDownLatch(1);
            final CountDownLatch disconnected = new CountDownLatch(1);
            final CountDownLatch reconnected = new CountDownLatch(1);
            final CountDownLatch failed = new CountDownLatch(1);

            ConnectionOptions options = new ConnectionOptions();
            options.reconnectOptions().reconnectEnabled(true);
            options.reconnectOptions().addReconnectLocation(backupURI.getHost(), backupURI.getPort());
            options.connectedHandler((connection, context) -> {
                connected.countDown();
            });
            options.interruptedHandler((connection, context) -> {
                disconnected.countDown();
            });
            options.reconnectedHandler((connection, context) -> {
                reconnected.countDown();
            });
            options.disconnectedHandler((connection, context) -> {
                failed.countDown();  // Not expecting any failure in this test case
            });

            Client container = Client.create();
            Connection connection = container.connect(primaryURI.getHost(), primaryURI.getPort(), options);
            Session session = connection.openSession();

            connection.openFuture().get();

            firstPeer.waitForScriptToComplete();

            try {
                session.openFuture().get();
            } catch (Exception ex) {
                fail("Should eventually succeed in opening this Session");
            }

            // Should connect, then be remotely closed and reconnect to the alternate
            assertTrue(connected.await(5, TimeUnit.SECONDS));
            assertTrue(disconnected.await(5, TimeUnit.SECONDS));
            assertTrue(reconnected.await(5, TimeUnit.SECONDS));
            assertEquals(1, failed.getCount());

            finalPeer.waitForScriptToComplete();
            finalPeer.expectClose().respond();

            connection.close();

            finalPeer.waitForScriptToComplete();
        }
    }

    @Test
    public void testInitialReconnectDelayDoesNotApplyToInitialConnect() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectClose().respond();
            peer.start();

            ConnectionOptions options = new ConnectionOptions();
            options.reconnectOptions().reconnectEnabled(true);

            final URI remoteURI = peer.getServerURI();
            final int delay = 20000;
            final StopWatch watch = new StopWatch();

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), options);

            connection.openFuture().get();

            long taken = watch.taken();

            final String message = "Initial connect should not have delayed for the specified initialReconnectDelay." +
                                   "Elapsed=" + taken + ", delay=" + delay;
            assertTrue(taken < delay, message);
            assertTrue(taken < 5000, "Connection took longer than reasonable: " + taken);

            connection.close();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testConnectionReportsFailedAfterMaxinitialReconnectAttempts() throws Exception {
        try (ProtonTestServer firstPeer = new ProtonTestServer()) {
            firstPeer.start();

            final URI primaryURI = firstPeer.getServerURI();

            firstPeer.close();

            ConnectionOptions options = new ConnectionOptions();
            options.reconnectOptions().reconnectEnabled(true);
            options.reconnectOptions().maxReconnectAttempts(-1); // Try forever if connect succeeds once.
            options.reconnectOptions().maxInitialConnectionAttempts(3);
            options.reconnectOptions().warnAfterReconnectAttempts(5);
            options.reconnectOptions().reconnectDelay(10);
            options.reconnectOptions().useReconnectBackOff(false);

            Client container = Client.create();
            Connection connection = container.connect(primaryURI.getHost(), primaryURI.getPort(), options);

            try {
                connection.openFuture().get();
                fail("Should not successfully connect.");
            } catch (ExecutionException exe) {
                assertTrue(exe.getCause() instanceof ClientConnectionRemotelyClosedException);
            }

            try {
                connection.defaultSender();
                fail("Connection should be in a failed state now.");
            } catch (ClientConnectionRemotelyClosedException cliEx) {
            }

            connection.close();

            try {
                connection.defaultSender();
                fail("Connection should be in a closed state now.");
            } catch (ClientIllegalStateException cliEx) {
            }
        }
    }

    @Test
    public void testConnectionReportsFailedAfterMaxinitialReconnectAttemptsWithBackOff() throws Exception {
        try (ProtonTestServer firstPeer = new ProtonTestServer()) {
            firstPeer.start();

            final URI primaryURI = firstPeer.getServerURI();

            firstPeer.close();

            ConnectionOptions options = new ConnectionOptions();
            options.reconnectOptions().reconnectEnabled(true);
            options.reconnectOptions().maxReconnectAttempts(-1); // Try forever if connect succeeds once.
            options.reconnectOptions().maxInitialConnectionAttempts(10);
            options.reconnectOptions().warnAfterReconnectAttempts(2);
            options.reconnectOptions().reconnectDelay(10);
            options.reconnectOptions().useReconnectBackOff(true);
            options.reconnectOptions().maxReconnectDelay(100);

            Client container = Client.create();
            Connection connection = container.connect(primaryURI.getHost(), primaryURI.getPort(), options);

            try {
                connection.openFuture().get();
                fail("Should not successfully connect.");
            } catch (ExecutionException exe) {
                assertTrue(exe.getCause() instanceof ClientConnectionRemotelyClosedException);
            }

            try {
                connection.defaultSender();
                fail("Connection should be in a failed state now.");
            } catch (ClientConnectionRemotelyClosedException cliEx) {
            }

            connection.close();

            try {
                connection.defaultSender();
                fail("Connection should be in a closed state now.");
            } catch (ClientIllegalStateException cliEx) {
            }
        }
    }
}
