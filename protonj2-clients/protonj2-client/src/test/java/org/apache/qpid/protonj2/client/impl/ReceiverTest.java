package org.apache.qpid.protonj2.client.impl;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

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
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.exceptions.ClientIllegalStateException;
import org.apache.qpid.protonj2.client.exceptions.ClientOperationTimedOutException;
import org.apache.qpid.protonj2.client.exceptions.ClientSecurityException;
import org.apache.qpid.protonj2.client.test.ImperativeClientTestCase;
import org.apache.qpid.protonj2.test.driver.netty.NettyTestPeer;
import org.apache.qpid.protonj2.types.messaging.AmqpValue;
import org.apache.qpid.protonj2.types.transport.AmqpError;
import org.apache.qpid.protonj2.types.transport.ReceiverSettleMode;
import org.apache.qpid.protonj2.types.transport.Role;
import org.apache.qpid.protonj2.types.transport.SenderSettleMode;
import org.hamcrest.Matcher;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReceiverTest extends ImperativeClientTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(ReceiverTest.class);

    @Test(timeout = 30000)
    public void testCreateReceiverAndClose() throws Exception {
        doTestCreateReceiverAndCloseOrDetachLink(true);
    }

    @Test(timeout = 30000)
    public void testCreateReceiverAndDetach() throws Exception {
        doTestCreateReceiverAndCloseOrDetachLink(false);
    }

    private void doTestCreateReceiverAndCloseOrDetachLink(boolean close) throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofReceiver().respond();  // TODO match other options
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
                receiver.close().get(10, TimeUnit.SECONDS);
            } else {
                receiver.detach().get(10, TimeUnit.SECONDS);
            }

            connection.close().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 30000)
    public void testReceiverOpenRejectedByRemote() throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
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
                assertTrue(exe.getCause() instanceof ClientSecurityException);
            }

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            // Should not result in any close being sent now, already closed.
            receiver.close().get();

            peer.expectClose().respond();
            connection.close().get();

            peer.waitForScriptToComplete(1, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 30000)
    public void testOpenReceiverTimesOutWhenNoAttachResponseReceivedTimeout() throws Exception {
        doTestOpenReceiverTimesOutWhenNoAttachResponseReceived(true);
    }

    @Test(timeout = 30000)
    public void testOpenReceiverTimesOutWhenNoAttachResponseReceivedNoTimeout() throws Exception {
        doTestOpenReceiverTimesOutWhenNoAttachResponseReceived(false);
    }

    private void doTestOpenReceiverTimesOutWhenNoAttachResponseReceived(boolean timeout) throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
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
            connection.openFuture().get(10, TimeUnit.SECONDS);

            Session session = connection.openSession();
            session.openFuture().get(10, TimeUnit.SECONDS);

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

            connection.close().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 30000)
    public void testCloseReceiverTimesOutWhenNoCloseResponseReceivedTimeout() throws Exception {
        doTestCloseOrDetachReceiverTimesOutWhenNoCloseResponseReceived(true, true);
    }

    @Test(timeout = 30000)
    public void testCloseReceiverTimesOutWhenNoCloseResponseReceivedNoTimeout() throws Exception {
        doTestCloseOrDetachReceiverTimesOutWhenNoCloseResponseReceived(true, false);
    }

    @Test(timeout = 30000)
    public void testDetachReceiverTimesOutWhenNoCloseResponseReceivedTimeout() throws Exception {
        doTestCloseOrDetachReceiverTimesOutWhenNoCloseResponseReceived(false, true);
    }

    @Test(timeout = 30000)
    public void testDetachReceiverTimesOutWhenNoCloseResponseReceivedNoTimeout() throws Exception {
        doTestCloseOrDetachReceiverTimesOutWhenNoCloseResponseReceived(false, false);
    }

    private void doTestCloseOrDetachReceiverTimesOutWhenNoCloseResponseReceived(boolean close, boolean timeout) throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
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
                        receiver.close().get(10, TimeUnit.SECONDS);
                    } else {
                        receiver.close().get();
                    }
                } else {
                    if (timeout) {
                        receiver.detach().get(10, TimeUnit.SECONDS);
                    } else {
                        receiver.detach().get();
                    }
                }

                fail("Should not complete the close or detach future without an error");
            } catch (ExecutionException exe) {
                Throwable cause = exe.getCause();
                assertTrue(cause instanceof ClientOperationTimedOutException);
            }

            connection.close().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 20000)
    public void testReceiverDrainAllOutstanding() throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
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

            Future<Receiver> draining = receiver.drain();
            draining.get(5, TimeUnit.SECONDS);

            // Close things down
            peer.expectClose().respond();
            connection.close().get(5, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(1, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 20000)
    public void testAddCreditFailsWhileDrainPending() throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
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
            Receiver receiver = session.openReceiver("test-queue", new ReceiverOptions().creditWindow(0));

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            // Add some credit, verify not draining
            int credit = 7;
            Matcher<Boolean> drainMatcher = anyOf(equalTo(false), nullValue());
            peer.expectFlow().withDrain(drainMatcher).withLinkCredit(credit).withDeliveryCount(0);

            receiver.addCredit(credit);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            // Drain all the credit
            peer.expectFlow().withDrain(true).withLinkCredit(credit).withDeliveryCount(0);
            peer.expectClose().respond();

            Future<Receiver> draining = receiver.drain();
            assertFalse(draining.isDone());

            try {
                receiver.addCredit(1);
                fail("Should not allow add credit when drain is pending");
            } catch (ClientIllegalStateException ise) {
                // Expected
            }

            connection.close().get();

            peer.waitForScriptToComplete(1, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 20000)
    public void testAddCreditFailsWhenCreditWindowEnabled() throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
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
            peer.expectClose();

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
    @Test(timeout = 30000)
    public void testCreateDynamicReceiver() throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
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

            receiver.close().get(10, TimeUnit.SECONDS);

            connection.close().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 30000)
    public void testDynamicReceiverAddressWaitsForRemoteAttach() throws Exception {
        tryReadDynamicReceiverAddress(true);
    }

    @Test(timeout = 30000)
    public void testDynamicReceiverAddressFailsAfterOpenTimeout() throws Exception {
        tryReadDynamicReceiverAddress(false);
    }

    private void tryReadDynamicReceiverAddress(boolean attachResponse) throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
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

            if (attachResponse) {
                receiver.close().get();
            } else {
                try {
                    receiver.close().get();
                    fail("Should fail close to indicate remote misbehaving when connection not closed");
                } catch (ExecutionException ex) {
                    LOG.debug("Caught expected exception from close call", ex);
                }
            }

            peer.expectClose().respond();
            connection.close().get();

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

        try (NettyTestPeer peer = new NettyTestPeer()) {
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

            receiver.close().get(10, TimeUnit.SECONDS);

            connection.close().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 30000)
    public void testReceiverGetSourceWaitsForRemoteAttach() throws Exception {
        tryReadReceiverSource(true);
    }

    @Test(timeout = 30000)
    public void testReceiverGetSourceFailsAfterOpenTimeout() throws Exception {
        tryReadReceiverSource(false);
    }

    private void tryReadReceiverSource(boolean attachResponse) throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
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
                assertNotNull("Remote should have responded with a Source value", receiver.source());
                assertEquals("test-receiver", receiver.source().address());
            } else {
                try {
                    receiver.source();
                    fail("Should failed to get remote source due to no attach response");
                } catch (ClientException ex) {
                    LOG.debug("Caught expected exception from blocking call", ex);
                }
            }

            if (attachResponse) {
                receiver.close().get();
            } else {
                try {
                    receiver.close().get();
                    fail("Should fail close to indicate remote misbehaving when connection not closed");
                } catch (ExecutionException ex) {
                    LOG.debug("Caught expected exception from close call", ex);
                }
            }

            peer.expectClose().respond();
            connection.close().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 30000)
    public void testReceiverGetTargetWaitsForRemoteAttach() throws Exception {
        tryReadReceiverTarget(true);
    }

    @Test(timeout = 30000)
    public void testReceiverGetTargetFailsAfterOpenTimeout() throws Exception {
        tryReadReceiverTarget(false);
    }

    private void tryReadReceiverTarget(boolean attachResponse) throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
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
                assertNotNull("Remote should have responded with a Target value", receiver.target());
            } else {
                try {
                    receiver.target();
                    fail("Should failed to get remote source due to no attach response");
                } catch (ClientException ex) {
                    LOG.debug("Caught expected exception from blocking call", ex);
                }
            }

            if (attachResponse) {
                receiver.close().get();
            } else {
                try {
                    receiver.close().get();
                    fail("Should fail close to indicate remote misbehaving when connection not closed");
                } catch (ExecutionException ex) {
                    LOG.debug("Caught expected exception from close call", ex);
                }
            }

            peer.expectClose().respond();
            connection.close().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 30000)
    public void testReceiverGetRemotePropertiesWaitsForRemoteAttach() throws Exception {
        tryReadReceiverRemoteProperties(true);
    }

    @Test(timeout = 30000)
    public void testReceiverGetRemotePropertiesFailsAfterOpenTimeout() throws Exception {
        tryReadReceiverRemoteProperties(false);
    }

    private void tryReadReceiverRemoteProperties(boolean attachResponse) throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
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
                assertNotNull("Remote should have responded with a remote properties value", receiver.properties());
                assertEquals(expectedProperties, receiver.properties());
            } else {
                try {
                    receiver.properties();
                    fail("Should failed to get remote state due to no attach response");
                } catch (ClientException ex) {
                    LOG.debug("Caught expected exception from blocking call", ex);
                }
            }

            if (attachResponse) {
                receiver.close().get();
            } else {
                try {
                    receiver.close().get();
                    fail("Should fail close to indicate remote misbehaving when connection not closed");
                } catch (ExecutionException ex) {
                    LOG.debug("Caught expected exception from close call", ex);
                }
            }

            peer.expectClose().respond();
            connection.close().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 30000)
    public void testReceiverGetRemoteOfferedCapabilitiesWaitsForRemoteAttach() throws Exception {
        tryReadReceiverRemoteOfferedCapabilities(true);
    }

    @Test(timeout = 30000)
    public void testReceiverGetRemoteOfferedCapabilitiesFailsAfterOpenTimeout() throws Exception {
        tryReadReceiverRemoteOfferedCapabilities(false);
    }

    private void tryReadReceiverRemoteOfferedCapabilities(boolean attachResponse) throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
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
                assertNotNull("Remote should have responded with a remote offered Capabilities value", receiver.offeredCapabilities());
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

            if (attachResponse) {
                receiver.close().get();
            } else {
                try {
                    receiver.close().get();
                    fail("Should fail close to indicate remote misbehaving when connection not closed");
                } catch (ExecutionException ex) {
                    LOG.debug("Caught expected exception from close call", ex);
                }
            }

            peer.expectClose().respond();
            connection.close().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 30000)
    public void testReceiverGetRemoteDesiredCapabilitiesWaitsForRemoteAttach() throws Exception {
        tryReadReceiverRemoteDesiredCapabilities(true);
    }

    @Test(timeout = 30000)
    public void testReceiverGetRemoteDesiredCapabilitiesFailsAfterOpenTimeout() throws Exception {
        tryReadReceiverRemoteDesiredCapabilities(false);
    }

    private void tryReadReceiverRemoteDesiredCapabilities(boolean attachResponse) throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
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
                assertNotNull("Remote should have responded with a remote desired Capabilities value", receiver.desiredCapabilities());
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

            if (attachResponse) {
                receiver.close().get();
            } else {
                try {
                    receiver.close().get();
                    fail("Should fail close to indicate remote misbehaving when connection not closed");
                } catch (ExecutionException ex) {
                    LOG.debug("Caught expected exception from close call", ex);
                }
            }

            peer.expectClose().respond();
            connection.close().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 30000)
    public void testBlockingReceiveCancelledWhenReceiverClosed() throws Exception {
        doTtestBlockingReceiveCancelledWhenReceiverClosedOrDetached(true);
    }

    @Test(timeout = 30000)
    public void testBlockingReceiveCancelledWhenReceiverDetached() throws Exception {
        doTtestBlockingReceiveCancelledWhenReceiverClosedOrDetached(false);
    }

    public void doTtestBlockingReceiveCancelledWhenReceiverClosedOrDetached(boolean close) throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
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
                    receiver.close();
                } else {
                    receiver.detach();
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

            connection.close().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 30000)
    public void testBlockingReceiveCancelledWhenReceiverRemotelyClosed() throws Exception {
        doTtestBlockingReceiveCancelledWhenReceiverClosedOrDetached(true);
    }

    @Test(timeout = 30000)
    public void testBlockingReceiveCancelledWhenReceiverRemotelyDetached() throws Exception {
        doTtestBlockingReceiveCancelledWhenReceiverClosedOrDetached(false);
    }

    public void doTtestBlockingReceiveCancelledWhenReceiverRemotelyClosedOrDetached(boolean close) throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
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

            connection.close().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 30000)
    public void testCloseReceiverWithErrorCondition() throws Exception {
        doTestCloseOrDetachWithErrorCondition(true);
    }

    @Test(timeout = 30000)
    public void testDetachReceiverWithErrorCondition() throws Exception {
        doTestCloseOrDetachWithErrorCondition(false);
    }

    public void doTestCloseOrDetachWithErrorCondition(boolean close) throws Exception {
        final String condition = "amqp:link:detach-forced";
        final String description = "something bad happened.";

        try (NettyTestPeer peer = new NettyTestPeer()) {
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
                receiver.close(ErrorCondition.create(condition, description, null));
            } else {
                receiver.detach(ErrorCondition.create(condition, description, null));
            }

            connection.close().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 20_000)
    public void testReceiveMessageInSplitTransferFrames() throws Exception {
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
            final Receiver receiver = session.openReceiver("test-queue");
            receiver.openFuture().get();

            final byte[] payload = createEncodedMessage(new AmqpValue("Hello World"));

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
            receiver.close();
            connection.close().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 20_000)
    public void testReceiverHandlesAbortedSplitFrameTransfer() throws Exception {
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
            final Receiver receiver = session.openReceiver("test-queue");
            receiver.openFuture().get();

            final byte[] payload = createEncodedMessage(new AmqpValue("Hello World"));

            peer.remoteTransfer().withHandle(0)
                                 .withDeliveryId(0)
                                 .withDeliveryTag(new byte[] { 1 })
                                 .withMore(true)
                                 .withMessageFormat(0)
                                 .withPayload(payload).now();

            assertNull(receiver.tryReceive());

            peer.remoteTransfer().withHandle(0)
                                 .withMore(false)
                                 .withAborted(true)
                                 .withMessageFormat(0)
                                 .withPayload(payload).now();

            peer.expectDetach().respond();
            peer.expectClose().respond();

            assertNull(receiver.tryReceive());

            receiver.close();
            connection.close().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 20_000)
    public void testReceiverAddCreditOnAbortedTransferWhenNeeded() throws Exception {
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
            ReceiverOptions options = new ReceiverOptions();
            options.creditWindow(1);
            final Receiver receiver = session.openReceiver("test-queue", options);
            receiver.openFuture().get();

            final byte[] payload = createEncodedMessage(new AmqpValue("Hello World"));

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

            receiver.close();
            connection.close().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 30000)
    public void testReceiveCallFailsWhenReceiverPreviouslyClosed() throws Exception {
        doTestReceiveCallFailsWhenReceiverDetachedOrClosed(true);
    }

    @Test(timeout = 30000)
    public void testReceiveCallFailsWhenReceiverPreviouslyDetached() throws Exception {
        doTestReceiveCallFailsWhenReceiverDetachedOrClosed(false);
    }

    private void doTestReceiveCallFailsWhenReceiverDetachedOrClosed(boolean close) throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
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
                receiver.close();
            } else {
                receiver.detach();
            }

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            try {
                receiver.receive();
                fail("Receive call should fail when link closed or detached.");
            } catch (ClientIllegalStateException cliEx) {
                LOG.debug("Receiver threw error on receive call", cliEx);
            }

            peer.expectClose().respond();

            connection.close().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 30000)
    public void testReceiveTimedCallFailsWhenReceiverClosed() throws Exception {
        doTestReceiveTimedCallFailsWhenReceiverDetachedOrClosed(true);
    }

    @Test(timeout = 30000)
    public void testReceiveTimedCallFailsWhenReceiverDetached() throws Exception {
        doTestReceiveTimedCallFailsWhenReceiverDetachedOrClosed(false);
    }

    private void doTestReceiveTimedCallFailsWhenReceiverDetachedOrClosed(boolean close) throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
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
                receiver.close();
            } else {
                receiver.detach();
            }

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            try {
                receiver.receive(60_000);
                fail("Receive call should fail when link closed or detached.");
            } catch (ClientIllegalStateException cliEx) {
                LOG.debug("Receiver threw error on receive call", cliEx);
            }

            peer.expectClose().respond();

            connection.close().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 30000)
    public void testDrainFutureSignalsFailureWhenReceiverClosed() throws Exception {
        doTestDrainFutureSignalsFailureWhenReceiverClosedOrDetached(true);
    }

    @Test(timeout = 30000)
    public void testDrainFutureSignalsFailureWhenReceiverDetached() throws Exception {
        doTestDrainFutureSignalsFailureWhenReceiverClosedOrDetached(false);
    }

    private void doTestDrainFutureSignalsFailureWhenReceiverClosedOrDetached(boolean close) throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
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
                    receiver.close();
                } else {
                    receiver.detach();
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

            connection.close().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 30000)
    public void testDrainFutureSignalsFailureWhenReceiverRemotelyClosed() throws Exception {
        doTestDrainFutureSignalsFailureWhenReceiverRemotelyClosedOrDetached(true);
    }

    @Test(timeout = 30000)
    public void testDrainFutureSignalsFailureWhenReceiverRemotelyDetached() throws Exception {
        doTestDrainFutureSignalsFailureWhenReceiverRemotelyClosedOrDetached(false);
    }

    private void doTestDrainFutureSignalsFailureWhenReceiverRemotelyClosedOrDetached(boolean close) throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
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

            connection.close().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 30000)
    public void testDrainFutureSignalsFailureWhenConnectionDrops() throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
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
                fail("Drain call should fail when link closed or detached.");
            } catch (ExecutionException cliEx) {
                LOG.debug("Receiver threw error on drain call", cliEx);
                assertTrue(cliEx.getCause() instanceof ClientException);
            }

            connection.close().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }
}
