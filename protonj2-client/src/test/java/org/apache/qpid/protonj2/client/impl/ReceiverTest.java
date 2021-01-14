package org.apache.qpid.protonj2.client.impl;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.protonj2.client.AdvancedMessage;
import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.ConnectionOptions;
import org.apache.qpid.protonj2.client.Delivery;
import org.apache.qpid.protonj2.client.DeliveryMode;
import org.apache.qpid.protonj2.client.ErrorCondition;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.Receiver;
import org.apache.qpid.protonj2.client.ReceiverOptions;
import org.apache.qpid.protonj2.client.Session;
import org.apache.qpid.protonj2.client.SessionOptions;
import org.apache.qpid.protonj2.client.StreamDelivery;
import org.apache.qpid.protonj2.client.StreamReceiver;
import org.apache.qpid.protonj2.client.exceptions.ClientConnectionRemotelyClosedException;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.exceptions.ClientIOException;
import org.apache.qpid.protonj2.client.exceptions.ClientIllegalStateException;
import org.apache.qpid.protonj2.client.exceptions.ClientLinkRemotelyClosedException;
import org.apache.qpid.protonj2.client.exceptions.ClientOperationTimedOutException;
import org.apache.qpid.protonj2.client.test.ImperativeClientTestCase;
import org.apache.qpid.protonj2.test.driver.ProtonTestServer;
import org.apache.qpid.protonj2.types.messaging.AmqpValue;
import org.apache.qpid.protonj2.types.messaging.Data;
import org.apache.qpid.protonj2.types.messaging.Section;
import org.apache.qpid.protonj2.types.transport.AmqpError;
import org.apache.qpid.protonj2.types.transport.ReceiverSettleMode;
import org.apache.qpid.protonj2.types.transport.Role;
import org.apache.qpid.protonj2.types.transport.SenderSettleMode;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Timeout(20)
public class ReceiverTest extends ImperativeClientTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(ReceiverTest.class);

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
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());

            connection.openFuture().get(10, TimeUnit.SECONDS);

            Session session = connection.openSession();
            session.openFuture().get(10, TimeUnit.SECONDS);

            Receiver receiver = session.openReceiver("test-queue");
            receiver.openFuture().get(10, TimeUnit.SECONDS);

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
        doTestCreateReceiverAndCloseOrDetachLinkSync(true);
    }

    @Test
    public void testCreateReceiverAndDetachSync() throws Exception {
        doTestCreateReceiverAndCloseOrDetachLinkSync(false);
    }

    private void doTestCreateReceiverAndCloseOrDetachLinkSync(boolean close) throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofReceiver().respond();
            peer.expectFlow().withLinkCredit(10);
            peer.expectDetach().withClosed(close).respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());

            connection.openFuture().get(10, TimeUnit.SECONDS);

            Session session = connection.openSession();
            session.openFuture().get(10, TimeUnit.SECONDS);

            Receiver receiver = session.openReceiver("test-queue");
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
    public void testReceiverOpenRejectedByRemote() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().respond().withNullSource();
            peer.expectFlow();
            peer.remoteDetach().withErrorCondition(AmqpError.UNAUTHORIZED_ACCESS.toString(), "Cannot read from this address").queue();
            peer.expectDetach();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());

            connection.openFuture().get();

            Session session = connection.openSession();
            session.openFuture().get();

            Receiver receiver = session.openReceiver("test-queue");
            try {
                receiver.openFuture().get();
                fail("Open of receiver should fail due to remote indicating pending close.");
            } catch (ExecutionException exe) {
                assertNotNull(exe.getCause());
                assertTrue(exe.getCause() instanceof ClientLinkRemotelyClosedException);
                ClientLinkRemotelyClosedException linkClosed = (ClientLinkRemotelyClosedException) exe.getCause();
                assertNotNull(linkClosed.getErrorCondition());
                assertEquals(AmqpError.UNAUTHORIZED_ACCESS.toString(), linkClosed.getErrorCondition().condition());
            }

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            // Should not result in any close being sent now, already closed.
            receiver.closeAsync().get();

            peer.expectClose().respond();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(1, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testOpenReceiverTimesOutWhenNoAttachResponseReceivedTimeout() throws Exception {
        doTestOpenReceiverTimesOutWhenNoAttachResponseReceived(true);
    }

    @Test
    public void testOpenReceiverTimesOutWhenNoAttachResponseReceivedNoTimeout() throws Exception {
        doTestOpenReceiverTimesOutWhenNoAttachResponseReceived(false);
    }

    private void doTestOpenReceiverTimesOutWhenNoAttachResponseReceived(boolean timeout) throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.RECEIVER.getValue());
            peer.expectFlow();
            peer.expectDetach();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Receiver test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession().openFuture().get(10, TimeUnit.SECONDS);
            Receiver receiver = session.openReceiver("test-queue", new ReceiverOptions().openTimeout(10));

            try {
                if (timeout) {
                    receiver.openFuture().get(10, TimeUnit.SECONDS);
                } else {
                    receiver.openFuture().get();
                }

                fail("Should not complete the open future without an error");
            } catch (ExecutionException exe) {
                Throwable cause = exe.getCause();
                assertTrue(cause instanceof ClientOperationTimedOutException);
            }

            connection.closeAsync().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testOpenReceiverWaitWithTimeoutFailsWhenConnectionDrops() throws Exception {
        doTestOpenReceiverWaitFailsWhenConnectionDrops(true);
    }

    @Test
    public void testOpenReceiverWaitWithNoTimeoutFailsWhenConnectionDrops() throws Exception {
        doTestOpenReceiverWaitFailsWhenConnectionDrops(false);
    }

    private void doTestOpenReceiverWaitFailsWhenConnectionDrops(boolean timeout) throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofReceiver();
            peer.expectFlow();
            peer.dropAfterLastHandler(10);
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Sender test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession();
            Receiver receiver = session.openReceiver("test-queue");

            try {
                if (timeout) {
                    receiver.openFuture().get(10, TimeUnit.SECONDS);
                } else {
                    receiver.openFuture().get();
                }

                fail("Should not complete the open future without an error");
            } catch (ExecutionException exe) {
                Throwable cause = exe.getCause();
                assertTrue(cause instanceof ClientIOException);
            }

            connection.closeAsync().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testCloseReceiverTimesOutWhenNoCloseResponseReceivedTimeout() throws Exception {
        doTestCloseOrDetachReceiverTimesOutWhenNoCloseResponseReceived(true, true);
    }

    @Test
    public void testCloseReceiverTimesOutWhenNoCloseResponseReceivedNoTimeout() throws Exception {
        doTestCloseOrDetachReceiverTimesOutWhenNoCloseResponseReceived(true, false);
    }

    @Test
    public void testDetachReceiverTimesOutWhenNoCloseResponseReceivedTimeout() throws Exception {
        doTestCloseOrDetachReceiverTimesOutWhenNoCloseResponseReceived(false, true);
    }

    @Test
    public void testDetachReceiverTimesOutWhenNoCloseResponseReceivedNoTimeout() throws Exception {
        doTestCloseOrDetachReceiverTimesOutWhenNoCloseResponseReceived(false, false);
    }

    private void doTestCloseOrDetachReceiverTimesOutWhenNoCloseResponseReceived(boolean close, boolean timeout) throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.RECEIVER.getValue()).respond();
            peer.expectFlow();
            peer.expectDetach();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Receiver test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            ConnectionOptions options = new ConnectionOptions();
            options.closeTimeout(5);
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), options);

            connection.openFuture().get(10, TimeUnit.SECONDS);

            Session session = connection.openSession();
            session.openFuture().get(10, TimeUnit.SECONDS);

            Receiver receiver = session.openReceiver("test-queue");
            receiver.openFuture().get(10, TimeUnit.SECONDS);

            try {
                if (close) {
                    if (timeout) {
                        receiver.closeAsync().get(10, TimeUnit.SECONDS);
                    } else {
                        receiver.closeAsync().get();
                    }
                } else {
                    if (timeout) {
                        receiver.detachAsync().get(10, TimeUnit.SECONDS);
                    } else {
                        receiver.detachAsync().get();
                    }
                }

                fail("Should not complete the close or detach future without an error");
            } catch (ExecutionException exe) {
                Throwable cause = exe.getCause();
                assertTrue(cause instanceof ClientOperationTimedOutException);
            }

            connection.closeAsync().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testReceiverDrainAllOutstanding() throws Exception {
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

            connection.openFuture().get(5, TimeUnit.SECONDS);

            Session session = connection.openSession();
            session.openFuture().get(5, TimeUnit.SECONDS);

            Receiver receiver = session.openReceiver("test-queue", new ReceiverOptions().creditWindow(0));
            receiver.openFuture().get(5, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            // Add some credit, verify not draining
            int credit = 7;
            Matcher<Boolean> drainMatcher = anyOf(equalTo(false), nullValue());
            peer.expectFlow().withDrain(drainMatcher).withLinkCredit(credit).withDeliveryCount(0);

            receiver.addCredit(credit);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            // Drain all the credit
            peer.expectFlow().withDrain(true).withLinkCredit(credit).withDeliveryCount(0)
                             .respond()
                             .withDrain(true).withLinkCredit(0).withDeliveryCount(credit);

            Future<? extends Receiver> draining = receiver.drain();
            draining.get(5, TimeUnit.SECONDS);

            // Close things down
            peer.expectClose().respond();
            connection.closeAsync().get(5, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(1, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testAddCreditFailsWhileDrainPending() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.RECEIVER.getValue()).respond().withInitialDeliveryCount(20);
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession();
            Receiver receiver = session.openReceiver("test-queue", new ReceiverOptions().creditWindow(0));

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            // Add some credit, verify not draining
            int credit = 7;
            Matcher<Boolean> drainMatcher = anyOf(equalTo(false), nullValue());
            peer.expectFlow().withDrain(drainMatcher).withLinkCredit(credit);

            receiver.addCredit(credit);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            // Drain all the credit
            peer.expectFlow().withDrain(true).withLinkCredit(credit).withDeliveryCount(20);
            peer.expectClose().respond();

            Future<? extends Receiver> draining = receiver.drain();
            assertFalse(draining.isDone());

            try {
                receiver.addCredit(1);
                fail("Should not allow add credit when drain is pending");
            } catch (ClientIllegalStateException ise) {
                // Expected
            }

            connection.closeAsync().get();

            peer.waitForScriptToComplete(1, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testAddCreditFailsWhenCreditWindowEnabled() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.RECEIVER.getValue()).respond();
            peer.expectFlow().withLinkCredit(10);
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession();
            Receiver receiver = session.openReceiver("test-queue", new ReceiverOptions().creditWindow(10));

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectClose().respond();

            try {
                receiver.addCredit(1);
                fail("Should not allow add credit when credit window configured");
            } catch (ClientIllegalStateException ise) {
                // Expected
            }

            connection.close();

            peer.waitForScriptToComplete(1, TimeUnit.SECONDS);
        }
    }
    @Test
    public void testCreateDynamicReceiver() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.RECEIVER.getValue())
                               .withSource().withDynamic(true).withAddress((String) null)
                               .and().respond()
                               .withSource().withDynamic(true).withAddress("test-dynamic-node");
            peer.expectFlow();
            peer.expectDetach().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            connection.openFuture().get(10, TimeUnit.SECONDS);

            Session session = connection.openSession();
            session.openFuture().get(10, TimeUnit.SECONDS);

            Receiver receiver = session.openDynamicReceiver();
            receiver.openFuture().get(10, TimeUnit.SECONDS);

            assertNotNull("Remote should have assigned the address for the dynamic receiver", receiver.address());
            assertEquals("test-dynamic-node", receiver.address());

            receiver.closeAsync().get(10, TimeUnit.SECONDS);

            connection.closeAsync().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testDynamicReceiverAddressWaitsForRemoteAttach() throws Exception {
        tryReadDynamicReceiverAddress(true);
    }

    @Test
    public void testDynamicReceiverAddressFailsAfterOpenTimeout() throws Exception {
        tryReadDynamicReceiverAddress(false);
    }

    private void tryReadDynamicReceiverAddress(boolean attachResponse) throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.RECEIVER.getValue())
                               .withSource().withDynamic(true).withAddress((String) null);
            peer.expectFlow();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            ConnectionOptions options = new ConnectionOptions().openTimeout(100);
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), options);
            connection.openFuture().get();

            Session session = connection.openSession();
            session.openFuture().get();

            Receiver receiver = session.openDynamicReceiver();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            if (attachResponse) {
                peer.expectDetach().respond();
                peer.respondToLastAttach().withSource().withAddress("test-dynamic-node").and().later(10);
            } else {
                peer.expectDetach();
            }

            if (attachResponse) {
                assertNotNull("Remote should have assigned the address for the dynamic receiver", receiver.address());
                assertEquals("test-dynamic-node", receiver.address());
            } else {
                try {
                    receiver.address();
                    fail("Should failed to get address due to no attach response");
                } catch (ClientException ex) {
                    LOG.debug("Caught expected exception from address call", ex);
                }
            }

            try {
                receiver.closeAsync().get();
            } catch (ExecutionException ex) {
                LOG.debug("Caught unexpected exception from close call", ex);
                fail("Should not fail close whenn connection not closed and detach sent");
            }

            peer.expectClose().respond();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testCreateReceiverWithQoSOfAtMostOnce() throws Exception {
        doTestCreateReceiverWithConfiguredQoS(DeliveryMode.AT_MOST_ONCE);
    }

    @Test
    public void testCreateReceiverWithQoSOfAtLeastOnce() throws Exception {
        doTestCreateReceiverWithConfiguredQoS(DeliveryMode.AT_LEAST_ONCE);
    }

    private void doTestCreateReceiverWithConfiguredQoS(DeliveryMode qos) throws Exception {
        byte sndMode = qos == DeliveryMode.AT_MOST_ONCE ? SenderSettleMode.SETTLED.byteValue() : SenderSettleMode.UNSETTLED.byteValue();

        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.RECEIVER.getValue())
                               .withSndSettleMode(sndMode)
                               .withRcvSettleMode(ReceiverSettleMode.FIRST.byteValue())
                               .respond()
                               .withSndSettleMode(sndMode)
                               .withRcvSettleMode(ReceiverSettleMode.FIRST.byteValue());
            peer.expectFlow();
            peer.expectDetach().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            connection.openFuture().get(10, TimeUnit.SECONDS);

            Session session = connection.openSession();
            session.openFuture().get(10, TimeUnit.SECONDS);

            ReceiverOptions options = new ReceiverOptions().deliveryMode(qos);
            Receiver receiver = session.openReceiver("test-qos", options);
            receiver.openFuture().get(10, TimeUnit.SECONDS);

            assertEquals("test-qos", receiver.address());

            receiver.closeAsync().get(10, TimeUnit.SECONDS);

            connection.closeAsync().get(10, TimeUnit.SECONDS);

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
            ConnectionOptions options = new ConnectionOptions().openTimeout(100);
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), options);
            connection.openFuture().get();

            Session session = connection.openSession();
            session.openFuture().get();

            Receiver receiver = session.openReceiver("test-receiver");

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            if (attachResponse) {
                peer.expectDetach().respond();
                peer.respondToLastAttach().later(10);
            } else {
                peer.expectDetach();
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
                fail("Should not fail close whenn connection not closed and detach sent");
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
            ConnectionOptions options = new ConnectionOptions().openTimeout(100);
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), options);
            connection.openFuture().get();

            Session session = connection.openSession();
            session.openFuture().get();

            Receiver receiver = session.openReceiver("test-receiver");

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            if (attachResponse) {
                peer.expectDetach().respond();
                peer.respondToLastAttach().later(10);
            } else {
                peer.expectDetach();
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
                fail("Should not fail close whenn connection not closed and detach sent");
            }

            peer.expectClose().respond();
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
            Session session = connection.openSession();
            session.openFuture().get();

            ReceiverOptions options = new ReceiverOptions().openTimeout(100);
            Receiver receiver = session.openReceiver("test-receiver", options);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            Map<String, Object> expectedProperties = new HashMap<>();
            expectedProperties.put("TEST", "test-property");

            if (attachResponse) {
                peer.expectDetach().respond();
                peer.respondToLastAttach().withPropertiesMap(expectedProperties).later(10);
            } else {
                peer.expectDetach();
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
                fail("Should not fail close whenn connection not closed and detach sent");
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
            ConnectionOptions options = new ConnectionOptions().openTimeout(100);
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), options);
            connection.openFuture().get();

            Session session = connection.openSession();
            session.openFuture().get();

            Receiver receiver = session.openReceiver("test-receiver");

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            if (attachResponse) {
                peer.expectDetach().respond();
                peer.respondToLastAttach().withOfferedCapabilities("QUEUE").later(10);
            } else {
                peer.expectDetach();
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
                fail("Should not fail close whenn connection not closed and detach sent");
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
            ConnectionOptions options = new ConnectionOptions().openTimeout(100);
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), options);
            connection.openFuture().get();

            Session session = connection.openSession();
            session.openFuture().get();

            Receiver receiver = session.openReceiver("test-receiver");

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            if (attachResponse) {
                peer.expectDetach().respond();
                peer.respondToLastAttach().withDesiredCapabilities("Error-Free").later(10);
            } else {
                peer.expectDetach();
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
                fail("Should not fail close whenn connection not closed and detach sent");
            }

            peer.expectClose().respond();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testBlockingReceiveCancelledWhenReceiverClosed() throws Exception {
        doTtestBlockingReceiveCancelledWhenReceiverClosedOrDetached(true);
    }

    @Test
    public void testBlockingReceiveCancelledWhenReceiverDetached() throws Exception {
        doTtestBlockingReceiveCancelledWhenReceiverClosedOrDetached(false);
    }

    public void doTtestBlockingReceiveCancelledWhenReceiverClosedOrDetached(boolean close) throws Exception {
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
            Session session = connection.openSession();
            ReceiverOptions options = new ReceiverOptions().creditWindow(0);
            Receiver receiver = session.openReceiver("test-queue", options);
            receiver.openFuture().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectFlow().withLinkCredit(10);
            peer.execute(() -> {
                if (close) {
                    receiver.closeAsync();
                } else {
                    receiver.detachAsync();
                }
            }).queue();
            peer.expectDetach().withClosed(close).respond();
            peer.expectClose().respond();

            receiver.addCredit(10);

            try {
                assertNull(receiver.receive());
            } catch (ClientException ise) {
                // Can happen if receiver closed before the receive call gets executed.
            }

            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testBlockingReceiveCancelledWhenReceiverRemotelyClosed() throws Exception {
        doTtestBlockingReceiveCancelledWhenReceiverClosedOrDetached(true);
    }

    @Test
    public void testBlockingReceiveCancelledWhenReceiverRemotelyDetached() throws Exception {
        doTtestBlockingReceiveCancelledWhenReceiverClosedOrDetached(false);
    }

    public void doTtestBlockingReceiveCancelledWhenReceiverRemotelyClosedOrDetached(boolean close) throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.RECEIVER.getValue()).respond();
            peer.expectFlow().withLinkCredit(10);
            peer.remoteDetach().withClosed(close)
                               .withErrorCondition(AmqpError.RESOURCE_DELETED.toString(), "Address was manually deleted")
                               .afterDelay(10).queue();
            peer.expectDetach().withClosed(close);
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession();
            Receiver receiver = session.openReceiver("test-queue");
            receiver.openFuture().get();

            try {
                assertNull(receiver.receive());
            } catch (IllegalStateException ise) {
                // Can happen if receiver closed before the receive call gets executed.
            }

            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testCloseReceiverWithErrorCondition() throws Exception {
        doTestCloseOrDetachWithErrorCondition(true);
    }

    @Test
    public void testDetachReceiverWithErrorCondition() throws Exception {
        doTestCloseOrDetachWithErrorCondition(false);
    }

    public void doTestCloseOrDetachWithErrorCondition(boolean close) throws Exception {
        final String condition = "amqp:link:detach-forced";
        final String description = "something bad happened.";

        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.RECEIVER.getValue()).respond();
            peer.expectFlow();
            peer.expectDetach().withClosed(close).withError(condition, description).respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession();
            final Receiver receiver = session.openReceiver("test-queue");
            receiver.openFuture().get();

            if (close) {
                receiver.closeAsync(ErrorCondition.create(condition, description, null));
            } else {
                receiver.detachAsync(ErrorCondition.create(condition, description, null));
            }

            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testOpenReceiverWithLinCapabilities() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.RECEIVER.getValue())
                               .withSource().withCapabilities("queue").and()
                               .respond();
            peer.expectFlow();
            peer.expectDetach().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Receiver test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession().openFuture().get(10, TimeUnit.SECONDS);
            ReceiverOptions receiverOptions = new ReceiverOptions();
            receiverOptions.sourceOptions().capabilities("queue");
            Receiver receiver = session.openReceiver("test-queue", receiverOptions);

            receiver.openFuture().get();
            receiver.close();

            connection.closeAsync().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testReceiveMessageInSplitTransferFrames() throws Exception {
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
            Session session = connection.openSession();
            final Receiver receiver = session.openReceiver("test-queue");
            receiver.openFuture().get();

            final byte[] payload = createEncodedMessage(new AmqpValue<>("Hello World"));

            final byte[] slice1 = Arrays.copyOfRange(payload, 0, 2);
            final byte[] slice2 = Arrays.copyOfRange(payload, 2, 4);
            final byte[] slice3 = Arrays.copyOfRange(payload, 4, payload.length);

            peer.remoteTransfer().withHandle(0)
                                 .withDeliveryId(0)
                                 .withDeliveryTag(new byte[] { 1 })
                                 .withMore(true)
                                 .withMessageFormat(0)
                                 .withPayload(slice1).now();

            assertNull(receiver.tryReceive());

            peer.remoteTransfer().withHandle(0)
                                 .withMore(true)
                                 .withMessageFormat(0)
                                 .withPayload(slice2).now();

            assertNull(receiver.tryReceive());

            peer.remoteTransfer().withHandle(0)
                                 .withMore(false)
                                 .withMessageFormat(0)
                                 .withPayload(slice3).now();

            peer.expectDisposition().withSettled(true).withState().accepted();
            peer.expectDetach().respond();
            peer.expectClose().respond();

            Delivery delivery = receiver.receive();
            assertNotNull(delivery);
            Message<?> received = delivery.message();
            assertNotNull(received);
            assertTrue(received.body() instanceof String);
            String value = (String) received.body();
            assertEquals("Hello World", value);

            delivery.accept();
            receiver.closeAsync();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testReceiverHandlesAbortedSplitFrameTransfer() throws Exception {
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

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession();
            final Receiver receiver = session.openReceiver("test-queue");

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            assertNull(receiver.receive(10, TimeUnit.MILLISECONDS));

            peer.expectDetach().respond();
            peer.expectClose().respond();
            peer.remoteTransfer().withHandle(0)
                                 .withMore(false)
                                 .withAborted(true)
                                 .withMessageFormat(0)
                                 .withPayload(payload).now();

            assertNull(receiver.receive(15, TimeUnit.MILLISECONDS));

            receiver.closeAsync();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testReceiverAddCreditOnAbortedTransferWhenNeeded() throws Exception {
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
            Session session = connection.openSession();
            ReceiverOptions options = new ReceiverOptions();
            options.creditWindow(1);
            final Receiver receiver = session.openReceiver("test-queue", options);
            receiver.openFuture().get();

            final byte[] payload = createEncodedMessage(new AmqpValue<>("Hello World"));

            peer.remoteTransfer().withHandle(0)
                                 .withDeliveryId(0)
                                 .withDeliveryTag(new byte[] { 1 })
                                 .withMore(true)
                                 .withMessageFormat(0)
                                 .withPayload(payload).now();

            assertNull(receiver.tryReceive());

            peer.expectFlow();
            peer.remoteTransfer().withHandle(0)
                                 .withDeliveryId(1)
                                 .withMessageFormat(0)
                                 .withMore(false)
                                 .withPayload(payload).queue();
            peer.expectDisposition().withSettled(true).withState().accepted();
            peer.expectFlow();
            peer.expectDetach().respond();
            peer.expectClose().respond();

            // Send final aborted transfer to complete first transfer and allow next to commence.
            peer.remoteTransfer().withHandle(0)
                                 .withDeliveryId(0)
                                 .withMore(false)
                                 .withAborted(true)
                                 .withMessageFormat(0)
                                 .withPayload(payload).now();

            Delivery delivery = receiver.receive();
            assertNotNull(delivery);
            Message<?> received = delivery.message();
            assertNotNull(received);
            assertTrue(received.body() instanceof String);
            String value = (String) received.body();
            assertEquals("Hello World", value);

            receiver.closeAsync();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testReceiverHandlesAbortedSplitFrameTransferAndReplenishesCredit() throws Exception {
        final byte[] payload = createEncodedMessage(new AmqpValue<>("Hello World"));

        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.RECEIVER.getValue()).respond();
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
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession();
            ReceiverOptions options = new ReceiverOptions();
            options.creditWindow(1);
            final Receiver receiver = session.openReceiver("test-queue", options);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            assertNull(receiver.receive(10, TimeUnit.MILLISECONDS));

            // Credit window is one and next transfer signals aborted so receiver should
            // top-up the credit window to allow more transfers to arrive.
            peer.expectFlow().withLinkCredit(1);
            peer.expectDetach().respond();
            peer.expectClose().respond();

            // Abort the delivery which should result in a credit top-up.
            peer.remoteTransfer().withHandle(0)
                                 .withMore(false)
                                 .withAborted(true)
                                 .withMessageFormat(0)
                                 .withPayload(payload).now();

            assertNull(receiver.receive(15, TimeUnit.MILLISECONDS));

            receiver.closeAsync();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testReceiveCallFailsWhenReceiverPreviouslyClosed() throws Exception {
        doTestReceiveCallFailsWhenReceiverDetachedOrClosed(true);
    }

    @Test
    public void testReceiveCallFailsWhenReceiverPreviouslyDetached() throws Exception {
        doTestReceiveCallFailsWhenReceiverDetachedOrClosed(false);
    }

    private void doTestReceiveCallFailsWhenReceiverDetachedOrClosed(boolean close) throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofReceiver().respond();
            peer.expectFlow().withLinkCredit(10);
            peer.expectDetach().withClosed(close).respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession();
            Receiver receiver = session.openReceiver("test-queue").openFuture().get();

            if (close) {
                receiver.closeAsync();
            } else {
                receiver.detachAsync();
            }

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            try {
                receiver.receive();
                fail("Receive call should fail when link closed or detached.");
            } catch (ClientIllegalStateException cliEx) {
                LOG.debug("Receiver threw error on receive call", cliEx);
            }

            peer.expectClose().respond();

            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testReceiveBlockedForMessageFailsWhenConnectionRemotelyClosed() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofReceiver().respond();
            peer.expectFlow().withLinkCredit(10);
            peer.remoteClose().withErrorCondition(AmqpError.RESOURCE_DELETED.toString(), "Connection was deleted").afterDelay(25).queue();
            peer.expectClose();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Receiver test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession();
            Receiver receiver = session.openReceiver("test-queue");
            receiver.openFuture().get();

            try {
                receiver.receive();
                fail("Receive should have failed when Connection remotely closed.");
            } catch (ClientConnectionRemotelyClosedException cliEx) {
                // Expected
            }

            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testTimedReceiveBlockedForMessageFailsWhenConnectionRemotelyClosed() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofReceiver().respond();
            peer.expectFlow().withLinkCredit(10);
            peer.remoteClose().withErrorCondition(AmqpError.RESOURCE_DELETED.toString(), "Connection was deleted").afterDelay(25).queue();
            peer.expectClose();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Receiver test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession();
            Receiver receiver = session.openReceiver("test-queue");
            receiver.openFuture().get();

            try {
                receiver.receive(10, TimeUnit.SECONDS);
                fail("Receive should have failed when Connection remotely closed.");
            } catch (ClientConnectionRemotelyClosedException cliEx) {
                // Expected send to throw indicating that the remote closed the connection
            }

            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testReceiveTimedCallFailsWhenReceiverClosed() throws Exception {
        doTestReceiveTimedCallFailsWhenReceiverDetachedOrClosed(true);
    }

    @Test
    public void testReceiveTimedCallFailsWhenReceiverDetached() throws Exception {
        doTestReceiveTimedCallFailsWhenReceiverDetachedOrClosed(false);
    }

    private void doTestReceiveTimedCallFailsWhenReceiverDetachedOrClosed(boolean close) throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofReceiver().respond();
            peer.expectFlow().withLinkCredit(10);
            peer.expectDetach().withClosed(close).respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession();
            Receiver receiver = session.openReceiver("test-queue").openFuture().get();

            if (close) {
                receiver.closeAsync();
            } else {
                receiver.detachAsync();
            }

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            try {
                receiver.receive(60, TimeUnit.SECONDS);
                fail("Receive call should fail when link closed or detached.");
            } catch (ClientIllegalStateException cliEx) {
                LOG.debug("Receiver threw error on receive call", cliEx);
            }

            peer.expectClose().respond();

            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testDrainFutureSignalsFailureWhenReceiverClosed() throws Exception {
        doTestDrainFutureSignalsFailureWhenReceiverClosedOrDetached(true);
    }

    @Test
    public void testDrainFutureSignalsFailureWhenReceiverDetached() throws Exception {
        doTestDrainFutureSignalsFailureWhenReceiverClosedOrDetached(false);
    }

    private void doTestDrainFutureSignalsFailureWhenReceiverClosedOrDetached(boolean close) throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofReceiver().respond();
            peer.expectFlow().withLinkCredit(10);
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession();
            Receiver receiver = session.openReceiver("test-queue").openFuture().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectFlow().withDrain(true).withLinkCredit(10);
            peer.execute(() -> {
                if (close) {
                    receiver.closeAsync();
                } else {
                    receiver.detachAsync();
                }
            }).queue();
            peer.expectDetach().withClosed(close).respond();
            peer.expectClose().respond();

            try {
                receiver.drain().get(10, TimeUnit.SECONDS);
                fail("Drain call should fail when link closed or detached.");
            } catch (ExecutionException cliEx) {
                LOG.debug("Receiver threw error on drain call", cliEx);
                assertTrue(cliEx.getCause() instanceof ClientException);
            }

            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testDrainFutureSignalsFailureWhenReceiverRemotelyClosed() throws Exception {
        doTestDrainFutureSignalsFailureWhenReceiverRemotelyClosedOrDetached(true);
    }

    @Test
    public void testDrainFutureSignalsFailureWhenReceiverRemotelyDetached() throws Exception {
        doTestDrainFutureSignalsFailureWhenReceiverRemotelyClosedOrDetached(false);
    }

    private void doTestDrainFutureSignalsFailureWhenReceiverRemotelyClosedOrDetached(boolean close) throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofReceiver().respond();
            peer.expectFlow().withLinkCredit(10);
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession();
            Receiver receiver = session.openReceiver("test-queue").openFuture().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectFlow().withDrain(true).withLinkCredit(10);
            peer.remoteDetach().withClosed(close)
                               .withErrorCondition(AmqpError.RESOURCE_DELETED.toString(), "Address was manually deleted").queue();
            peer.expectDetach().withClosed(close);
            peer.expectClose().respond();

            try {
                receiver.drain().get(10, TimeUnit.SECONDS);
                fail("Drain call should fail when link closed or detached.");
            } catch (ExecutionException cliEx) {
                LOG.debug("Receiver threw error on drain call", cliEx);
                assertTrue(cliEx.getCause() instanceof ClientException);
            }

            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testDrainFutureSignalsFailureWhenSessionRemotelyClosed() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofReceiver().respond();
            peer.expectFlow();
            peer.expectFlow().withDrain(true);
            peer.remoteEnd().withErrorCondition(AmqpError.RESOURCE_DELETED.toString(), "Session was closed").afterDelay(5).queue();
            peer.expectEnd();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession();
            Receiver receiver = session.openReceiver("test-queue").openFuture().get();

            try {
                receiver.drain().get(10, TimeUnit.SECONDS);
                fail("Drain call should fail when session closed by remote.");
            } catch (ExecutionException cliEx) {
                LOG.debug("Receiver threw error on drain call", cliEx);
                assertTrue(cliEx.getCause() instanceof ClientException);
            }

            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testDrainFutureSignalsFailureWhenConnectionDrops() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofReceiver().respond();
            peer.expectFlow();
            peer.expectFlow().withDrain(true);
            peer.dropAfterLastHandler();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession();
            Receiver receiver = session.openReceiver("test-queue").openFuture().get();

            try {
                receiver.drain().get(10, TimeUnit.SECONDS);
                fail("Drain call should fail when the connection drops.");
            } catch (ExecutionException cliEx) {
                LOG.debug("Receiver threw error on drain call", cliEx);
                assertTrue(cliEx.getCause() instanceof ClientException);
            }

            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testBlockedReceiveThrowsConnectionRemotelyClosedError() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofReceiver().withSource().withAddress("test").and().respond();
            peer.expectFlow();
            peer.dropAfterLastHandler(25);
            peer.start();

            URI remoteURI = peer.getServerURI();

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession();
            Receiver receiver = session.openReceiver("test");

            try {
                receiver.receive();
                fail("Receive should fail with remotely closed error after remote drops");
            } catch (ClientConnectionRemotelyClosedException cliEx) {
            }

            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testDeliveryRefusesRawStreamAfterMessage() throws Exception {
        final byte[] payload = createEncodedMessage(new AmqpValue<>("Hello World"));

        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.RECEIVER.getValue()).respond();
            peer.expectFlow().withLinkCredit(10);
            peer.remoteTransfer().withHandle(0)
                                 .withDeliveryId(0)
                                 .withDeliveryTag(new byte[] { 1 })
                                 .withMore(false)
                                 .withSettled(true)
                                 .withMessageFormat(0)
                                 .withPayload(payload).queue();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession();
            Receiver receiver = session.openReceiver("test-queue");

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectDetach().respond();
            peer.expectClose().respond();

            Delivery delivery = receiver.receive(10, TimeUnit.MILLISECONDS);
            assertNotNull(delivery);

            Message<String> message = delivery.message();
            assertNotNull(message);

            try {
                delivery.rawInputStream();
                fail("Should not be able to use the inputstream once message is requested");
            } catch (ClientIllegalStateException cliEx) {
                // Expected
            }

            receiver.closeAsync();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testDeliveryRefusesRawStreamAfterAnnotations() throws Exception {
        final byte[] payload = createEncodedMessage(new AmqpValue<>("Hello World"));

        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.RECEIVER.getValue()).respond();
            peer.expectFlow().withLinkCredit(10);
            peer.remoteTransfer().withHandle(0)
                                 .withDeliveryId(0)
                                 .withDeliveryTag(new byte[] { 1 })
                                 .withMore(false)
                                 .withSettled(true)
                                 .withMessageFormat(0)
                                 .withPayload(payload).queue();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession();
            Receiver receiver = session.openReceiver("test-queue");

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectDetach().respond();
            peer.expectClose().respond();

            Delivery delivery = receiver.receive(10, TimeUnit.MILLISECONDS);
            assertNotNull(delivery);
            assertNull(delivery.annotations());

            try {
                delivery.rawInputStream();
                fail("Should not be able to use the inputstream once message is requested");
            } catch (ClientIllegalStateException cliEx) {
                // Expected
            }

            receiver.closeAsync();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testDeliveryRefusesMessageDecodeOnceRawInputStreamIsRequested() throws Exception {
        final byte[] payload = createEncodedMessage(new AmqpValue<>("Hello World"));

        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.RECEIVER.getValue()).respond();
            peer.expectFlow().withLinkCredit(10);
            peer.remoteTransfer().withHandle(0)
                                 .withDeliveryId(0)
                                 .withDeliveryTag(new byte[] { 1 })
                                 .withMore(false)
                                 .withSettled(true)
                                 .withMessageFormat(0)
                                 .withPayload(payload).queue();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession();
            Receiver receiver = session.openReceiver("test-queue");

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectDetach().respond();
            peer.expectClose().respond();

            Delivery delivery = receiver.receive(10, TimeUnit.MILLISECONDS);
            assertNotNull(delivery);
            InputStream stream = delivery.rawInputStream();
            assertNotNull(stream);

            assertEquals(payload.length, stream.available());
            byte[] bytesRead = new byte[payload.length];
            assertEquals(payload.length, stream.read(bytesRead));
            assertArrayEquals(payload, bytesRead);

            try {
                delivery.message();
                fail("Should not be able to use the message API once raw stream is requested");
            } catch (ClientIllegalStateException cliEx) {
                // Expected
            }

            try {
                delivery.annotations();
                fail("Should not be able to use the annotations API once raw stream is requested");
            } catch (ClientIllegalStateException cliEx) {
                // Expected
            }

            receiver.closeAsync();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testReceiveDeliveryWithMultipleDataSections() throws Exception {
        final Data section1 = new Data(new byte[] { 0, 1, 2, 3 });
        final Data section2 = new Data(new byte[] { 0, 1, 2, 3 });
        final Data section3 = new Data(new byte[] { 0, 1, 2, 3 });

        final byte[] payload = createEncodedMessage(section1, section2, section3);

        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.RECEIVER.getValue()).respond();
            peer.expectFlow().withLinkCredit(10);
            peer.remoteTransfer().withHandle(0)
                                 .withDeliveryId(0)
                                 .withDeliveryTag(new byte[] { 1 })
                                 .withMore(false)
                                 .withSettled(true)
                                 .withMessageFormat(0)
                                 .withPayload(payload).queue();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession();
            Receiver receiver = session.openReceiver("test-queue");

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectDetach().respond();
            peer.expectClose().respond();

            Delivery delivery = receiver.receive(10, TimeUnit.MILLISECONDS);
            assertNotNull(delivery);

            AdvancedMessage<?> message = delivery.message().toAdvancedMessage();
            assertNotNull(message);

            assertEquals(3, message.bodySections().size());
            List<Section<?>> section = new ArrayList<>(message.bodySections());
            assertEquals(section1, section.get(0));
            assertEquals(section2, section.get(1));
            assertEquals(section3, section.get(2));

            receiver.closeAsync();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSessionWindowExpandedAsIncomingFramesArrive() throws Exception {
        final byte[] payload1 = new byte[255];
        final byte[] payload2 = new byte[255];

        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().withMaxFrameSize(1024).respond();
            peer.expectBegin().withIncomingWindow(1).respond();
            peer.expectAttach().withRole(Role.RECEIVER.getValue()).respond();
            peer.expectFlow().withIncomingWindow(1).withLinkCredit(10);
            peer.remoteTransfer().withHandle(0)
                                 .withDeliveryId(0)
                                 .withDeliveryTag(new byte[] { 1 })
                                 .withMore(true)
                                 .withSettled(true)
                                 .withMessageFormat(0)
                                 .withPayload(payload1).queue();
            peer.expectFlow().withIncomingWindow(1).withLinkCredit(10);
            peer.remoteTransfer().withHandle(0)
                                 .withDeliveryId(0)
                                 .withDeliveryTag(new byte[] { 1 })
                                 .withMore(false)
                                 .withSettled(true)
                                 .withMessageFormat(0)
                                 .withPayload(payload2).queue();
            peer.expectFlow().withIncomingWindow(1).withLinkCredit(9);
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            ConnectionOptions options = new ConnectionOptions().maxFrameSize(1024);
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), options);
            SessionOptions sessionOpts = new SessionOptions().incomingCapacity(1024);
            Session session = connection.openSession(sessionOpts);
            Receiver receiver = session.openReceiver("test-queue");

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectDetach().respond();
            peer.expectClose().respond();

            Delivery delivery = receiver.receive(10, TimeUnit.MILLISECONDS);
            assertNotNull(delivery);

            InputStream stream = delivery.rawInputStream();
            assertNotNull(stream);

            assertEquals(payload1.length + payload2.length, stream.available());
            byte[] bytesRead = new byte[payload1.length + payload2.length];
            assertEquals(payload1.length + payload2.length, stream.read(bytesRead));

            receiver.closeAsync();
            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testCannotReceiveFromStreamDeliveredBeforeConnectionDrop() throws Exception {
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
            peer.dropAfterLastHandler();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            final StreamReceiver receiver = connection.openStreamReceiver("test-queue");
            final StreamDelivery delivery = receiver.receive();

            peer.waitForScriptToComplete();

            assertNotNull(delivery);
            assertTrue(delivery.completed());
            assertFalse(delivery.aborted());

            assertEquals(-1, delivery.rawInputStream().read());

            connection.closeAsync().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }
}
