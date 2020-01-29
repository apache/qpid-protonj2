package org.messaginghub.amqperative;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.proton4j.amqp.driver.netty.NettyTestPeer;
import org.apache.qpid.proton4j.amqp.messaging.Accepted;
import org.apache.qpid.proton4j.amqp.messaging.Target;
import org.apache.qpid.proton4j.amqp.transport.AmqpError;
import org.apache.qpid.proton4j.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton4j.amqp.transport.Role;
import org.apache.qpid.proton4j.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.messaginghub.amqperative.impl.ClientException;
import org.messaginghub.amqperative.impl.exceptions.ClientOperationTimedOutException;
import org.messaginghub.amqperative.impl.exceptions.ClientSecurityException;
import org.messaginghub.amqperative.impl.exceptions.ClientSendTimedOutException;
import org.messaginghub.amqperative.impl.exceptions.ClientUnsupportedOperationException;
import org.messaginghub.amqperative.test.AMQPerativeTestCase;
import org.messaginghub.amqperative.util.AmqperativeTestRunner;
import org.messaginghub.amqperative.util.Repeat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(AmqperativeTestRunner.class)
public class SenderTest extends AMQPerativeTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(SenderTest.class);

    @Test(timeout = 20000)
    public void testCreateSenderAndClose() throws Exception {
        doTestCreateSenderAndCloseOrDeatch(true);
    }

    @Test(timeout = 20000)
    public void testCreateSenderAndDetach() throws Exception {
        doTestCreateSenderAndCloseOrDeatch(false);
    }

    private void doTestCreateSenderAndCloseOrDeatch(boolean close) throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.SENDER).respond();  // TODO match other options
            peer.expectDetach().withClosed(close).respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Sender test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());

            connection.openFuture().get(10, TimeUnit.SECONDS);

            Session session = connection.openSession();
            session.openFuture().get(10, TimeUnit.SECONDS);

            Sender sender = session.openSender("test-queue");
            sender.openFuture().get(10, TimeUnit.SECONDS);

            if (close) {
                sender.close().get(10, TimeUnit.SECONDS);
            } else {
                sender.detach().get(10, TimeUnit.SECONDS);
            }

            connection.close().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Repeat(repetitions = 1)
    @Test(timeout = 30000)
    public void testSenderOpenRejectedByRemote() throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().respond().withTarget((Target) null);
            peer.remoteDetach().withErrorCondition(AmqpError.UNAUTHORIZED_ACCESS, "Cannot read from this address").queue();
            peer.expectDetach();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Sender test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());

            connection.openFuture().get(10, TimeUnit.SECONDS);

            Session session = connection.openSession();
            session.openFuture().get(10, TimeUnit.SECONDS);

            Sender sender = session.openSender("test-queue");
            try {
                sender.openFuture().get(10, TimeUnit.SECONDS);
                fail("Open of sender should fail due to remote indicating pending close.");
            } catch (ExecutionException exe) {
                assertNotNull(exe.getCause());
                assertTrue(exe.getCause() instanceof ClientSecurityException);
            }

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            // Should not result in any close being sent now, already closed.
            sender.close().get(10, TimeUnit.SECONDS);

            peer.expectClose().respond();
            connection.close().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(1, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 30000)
    public void testOpenSenderTimesOutWhenNoAttachResponseReceivedTimeout() throws Exception {
        doTestOpenSenderTimesOutWhenNoAttachResponseReceived(true);
    }

    @Test(timeout = 30000)
    public void testOpenSenderTimesOutWhenNoAttachResponseReceivedNoTimeout() throws Exception {
        doTestOpenSenderTimesOutWhenNoAttachResponseReceived(false);
    }

    private void doTestOpenSenderTimesOutWhenNoAttachResponseReceived(boolean timeout) throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.SENDER);
            peer.expectDetach();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Sender test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            connection.openFuture().get(10, TimeUnit.SECONDS);

            Session session = connection.openSession();
            session.openFuture().get(10, TimeUnit.SECONDS);

            Sender sender = session.openSender("test-queue", new SenderOptions().openTimeout(10));

            try {
                if (timeout) {
                    sender.openFuture().get(10, TimeUnit.SECONDS);
                } else {
                    sender.openFuture().get();
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
    public void testCloseSenderTimesOutWhenNoCloseResponseReceivedTimeout() throws Exception {
        doTestCloseOrDetachSenderTimesOutWhenNoCloseResponseReceived(true, true);
    }

    @Test(timeout = 30000)
    public void testCloseSenderTimesOutWhenNoCloseResponseReceivedNoTimeout() throws Exception {
        doTestCloseOrDetachSenderTimesOutWhenNoCloseResponseReceived(true, false);
    }

    @Test(timeout = 30000)
    public void testDetachSenderTimesOutWhenNoCloseResponseReceivedTimeout() throws Exception {
        doTestCloseOrDetachSenderTimesOutWhenNoCloseResponseReceived(false, true);
    }

    @Test(timeout = 30000)
    public void testDetachSenderTimesOutWhenNoCloseResponseReceivedNoTimeout() throws Exception {
        doTestCloseOrDetachSenderTimesOutWhenNoCloseResponseReceived(false, false);
    }

    private void doTestCloseOrDetachSenderTimesOutWhenNoCloseResponseReceived(boolean close, boolean timeout) throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.SENDER).respond();
            peer.expectDetach();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Sender test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            ConnectionOptions options = new ConnectionOptions();
            options.closeTimeout(10);
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), options);

            connection.openFuture().get(10, TimeUnit.SECONDS);

            Session session = connection.openSession();
            session.openFuture().get(10, TimeUnit.SECONDS);

            Sender sender = session.openSender("test-queue");
            sender.openFuture().get(10, TimeUnit.SECONDS);

            try {
                if (close) {
                    if (timeout) {
                        sender.close().get(10, TimeUnit.SECONDS);
                    } else {
                        sender.close().get();
                    }
                } else {
                    if (timeout) {
                        sender.detach().get(10, TimeUnit.SECONDS);
                    } else {
                        sender.detach().get();
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

    @Test(timeout = 30000)
    public void testSendTimesOutWhenNoCreditIssued() throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.SENDER).respond();
            peer.expectDetach().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Sender test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            ConnectionOptions options = new ConnectionOptions();
            options.sendTimeout(1);
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), options);

            connection.openFuture().get(10, TimeUnit.SECONDS);

            Session session = connection.openSession();
            session.openFuture().get(10, TimeUnit.SECONDS);

            Sender sender = session.openSender("test-queue");
            sender.openFuture().get(10, TimeUnit.SECONDS);

            Message<String> message = Message.create("Hello World");
            try {
                sender.send(message);
                fail("Should throw a send timed out exception");
            } catch (ClientSendTimedOutException ex) {
                // Expected error, ignore
            }

            sender.close().get(10, TimeUnit.SECONDS);

            connection.close().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 30000)
    public void testSendCompletesWhenCreditEventuallyOffered() throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.SENDER).respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Sender test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            ConnectionOptions options = new ConnectionOptions();
            options.sendTimeout(200);
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), options);

            connection.openFuture().get(10, TimeUnit.SECONDS);

            Session session = connection.openSession();
            session.openFuture().get(10, TimeUnit.SECONDS);

            Sender sender = session.openSender("test-queue");
            sender.openFuture().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            // Expect a transfer but only after the flow which is delayed to allow the
            // client time to block on credit.
            peer.expectTransfer().withPayload(notNullValue(ProtonBuffer.class));
            peer.remoteFlow().withDeliveryCount(0)
                             .withLinkCredit(1)
                             .withIncomingWindow(1024)
                             .withOutgoingWindow(10)
                             .withNextIncomingId(0)
                             .withNextOutgoingId(1).later(30);
            peer.expectDetach().respond();
            peer.expectClose().respond();

            Message<String> message = Message.create("Hello World");
            try {
                LOG.debug("Attempting send with sender: {}", sender);
                sender.send(message);
            } catch (ClientSendTimedOutException ex) {
                fail("Should not throw a send timed out exception");
            }

            sender.close().get(10, TimeUnit.SECONDS);

            connection.close().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 30000)
    public void testSendWhenCreditIsAvailable() throws Exception {
        doTestSendWhenCreditIsAvailable(false);
    }

    @Test(timeout = 30000)
    public void testTrySendWhenCreditIsAvailable() throws Exception {
        doTestSendWhenCreditIsAvailable(true);
    }

    private void doTestSendWhenCreditIsAvailable(boolean trySend) throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.SENDER).respond();
            peer.remoteFlow().withDeliveryCount(0)
                             .withLinkCredit(10)
                             .withIncomingWindow(1024)
                             .withOutgoingWindow(10)
                             .withNextIncomingId(0)
                             .withNextOutgoingId(1).queue();
            peer.expectAttach().withRole(Role.RECEIVER).respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Sender test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());

            connection.openFuture().get(10, TimeUnit.SECONDS);

            Session session = connection.openSession();
            session.openFuture().get(10, TimeUnit.SECONDS);

            Sender sender = session.openSender("test-queue");
            sender.openFuture().get(10, TimeUnit.SECONDS);

            // This ensures that the flow to sender is processed before we try-send
            Receiver receiver = session.openReceiver("test-queue", new ReceiverOptions().creditWindow(0));
            receiver.openFuture().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectTransfer().withPayload(notNullValue(ProtonBuffer.class));
            peer.expectDetach().respond();
            peer.expectClose().respond();

            Message<String> message = Message.create("Hello World");

            final Tracker tracker;
            if (trySend) {
                tracker = sender.trySend(message);
            } else {
                tracker = sender.send(message);
            }
            assertNotNull(tracker);

            sender.close().get(10, TimeUnit.SECONDS);

            connection.close().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 30000)
    public void testTrySendWhenNoCreditAvailable() throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.SENDER).respond();
            peer.expectDetach().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Sender test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            ConnectionOptions options = new ConnectionOptions();
            options.sendTimeout(1);
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), options);

            connection.openFuture().get(10, TimeUnit.SECONDS);

            Session session = connection.openSession();
            session.openFuture().get(10, TimeUnit.SECONDS);

            Sender sender = session.openSender("test-queue");
            sender.openFuture().get(10, TimeUnit.SECONDS);

            Message<String> message = Message.create("Hello World");
            assertNull(sender.trySend(message));

            sender.close().get(10, TimeUnit.SECONDS);
            connection.close().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 30000)
    public void testCreateSenderWithQoSOfAtMostOnce() throws Exception {
        doTestCreateSenderWithConfiguredQoS(DeliveryMode.AT_MOST_ONCE);
    }

    @Test(timeout = 30000)
    public void testCreateSenderWithQoSOfAtLeastOnce() throws Exception {
        doTestCreateSenderWithConfiguredQoS(DeliveryMode.AT_LEAST_ONCE);
    }

    private void doTestCreateSenderWithConfiguredQoS(DeliveryMode qos) throws Exception {
        SenderSettleMode sndMode = qos == DeliveryMode.AT_MOST_ONCE ? SenderSettleMode.SETTLED : SenderSettleMode.UNSETTLED;

        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.SENDER)
                               .withSndSettleMode(sndMode)
                               .withRcvSettleMode(ReceiverSettleMode.FIRST)
                               .respond()
                               .withSndSettleMode(sndMode)
                               .withRcvSettleMode(ReceiverSettleMode.FIRST);
            peer.expectDetach().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Sender test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            connection.openFuture().get(10, TimeUnit.SECONDS);

            Session session = connection.openSession();
            session.openFuture().get(10, TimeUnit.SECONDS);

            SenderOptions options = new SenderOptions().deliveryMode(qos);
            Sender sender = session.openSender("test-qos", options);
            sender.openFuture().get(10, TimeUnit.SECONDS);

            assertEquals("test-qos", sender.address());

            sender.close().get(10, TimeUnit.SECONDS);

            connection.close().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 30000)
    public void testSendAutoSettlesOnceRemoteSettles() throws Exception {
        doTestSentMessageGetsAutoSettledAfterRemtoeSettles(false);
    }

    @Test(timeout = 30000)
    public void testTrySendAutoSettlesOnceRemoteSettles() throws Exception {
        doTestSentMessageGetsAutoSettledAfterRemtoeSettles(true);
    }

    private void doTestSentMessageGetsAutoSettledAfterRemtoeSettles(boolean trySend) throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.SENDER).respond();
            peer.remoteFlow().withDeliveryCount(0)
                             .withLinkCredit(10)
                             .withIncomingWindow(1024)
                             .withOutgoingWindow(10)
                             .withNextIncomingId(0)
                             .withNextOutgoingId(1).queue();
            peer.expectAttach().withRole(Role.RECEIVER).respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Sender test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());

            connection.openFuture().get(10, TimeUnit.SECONDS);

            Session session = connection.openSession();
            session.openFuture().get(10, TimeUnit.SECONDS);

            Sender sender = session.openSender("test-queue");
            sender.openFuture().get(10, TimeUnit.SECONDS);

            // This ensures that the flow to sender is processed before we try-send
            Receiver receiver = session.openReceiver("test-queue", new ReceiverOptions().creditWindow(0));
            receiver.openFuture().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectTransfer().withPayload(notNullValue(ProtonBuffer.class))
                                 .respond()
                                 .withSettled(true).withState(Accepted.getInstance());
            peer.expectDisposition().withSettled(true);
            peer.expectDetach().respond();
            peer.expectClose().respond();

            Message<String> message = Message.create("Hello World");

            final Tracker tracker;
            if (trySend) {
                tracker = sender.trySend(message);
            } else {
                tracker = sender.send(message);
            }

            assertNotNull(tracker);
            assertNotNull(tracker.acknowledgeFuture().get(5, TimeUnit.SECONDS));
            assertEquals(tracker.remoteState().getType(), DeliveryState.Type.ACCEPTED);

            sender.close().get(10, TimeUnit.SECONDS);

            connection.close().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 30000)
    public void testSendDoesNotAutoSettlesOnceRemoteSettlesIfAutoSettleOff() throws Exception {
        doTestSentMessageNotAutoSettledAfterRemtoeSettles(false);
    }

    @Test(timeout = 30000)
    public void testTrySendDoesNotAutoSettlesOnceRemoteSettlesIfAutoSettleOff() throws Exception {
        doTestSentMessageNotAutoSettledAfterRemtoeSettles(true);
    }

    private void doTestSentMessageNotAutoSettledAfterRemtoeSettles(boolean trySend) throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.SENDER).respond();
            peer.remoteFlow().withDeliveryCount(0)
                             .withLinkCredit(10)
                             .withIncomingWindow(1024)
                             .withOutgoingWindow(10)
                             .withNextIncomingId(0)
                             .withNextOutgoingId(1).queue();
            peer.expectAttach().withRole(Role.RECEIVER).respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Sender test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());

            connection.openFuture().get(10, TimeUnit.SECONDS);

            Session session = connection.openSession();
            session.openFuture().get(10, TimeUnit.SECONDS);

            Sender sender = session.openSender("test-queue", new SenderOptions().autoSettle(false));
            sender.openFuture().get(10, TimeUnit.SECONDS);

            // This ensures that the flow to sender is processed before we try-send
            Receiver receiver = session.openReceiver("test-queue", new ReceiverOptions().creditWindow(0));
            receiver.openFuture().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectTransfer().withPayload(notNullValue(ProtonBuffer.class))
                                 .respond()
                                 .withSettled(true).withState(Accepted.getInstance());
            peer.expectDetach().respond();
            peer.expectClose().respond();

            Message<String> message = Message.create("Hello World");

            final Tracker tracker;
            if (trySend) {
                tracker = sender.trySend(message);
            } else {
                tracker = sender.send(message);
            }

            assertNotNull(tracker);
            assertNotNull(tracker.acknowledgeFuture().get(5, TimeUnit.SECONDS));
            assertEquals(tracker.remoteState().getType(), DeliveryState.Type.ACCEPTED);
            assertNull(tracker.state());
            assertFalse(tracker.settled());

            sender.close().get(10, TimeUnit.SECONDS);

            connection.close().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 30000)
    public void testSenderSendingSettledCompletesTrackerAcknowledgeFuture() throws Exception {
        doTestSenderSendingSettledCompletesTrackerAcknowledgeFuture(false);
    }

    @Test(timeout = 30000)
    public void testSenderTrySendingSettledCompletesTrackerAcknowledgeFuture() throws Exception {
        doTestSenderSendingSettledCompletesTrackerAcknowledgeFuture(true);
    }

    private void doTestSenderSendingSettledCompletesTrackerAcknowledgeFuture(boolean trySend) throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.SENDER)
                               .withSndSettleMode(SenderSettleMode.SETTLED)
                               .withRcvSettleMode(ReceiverSettleMode.FIRST)
                               .respond()
                               .withSndSettleMode(SenderSettleMode.SETTLED)
                               .withRcvSettleMode(ReceiverSettleMode.FIRST);
            peer.remoteFlow().withLinkCredit(10).queue();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Sender test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort()).openFuture().get();

            Session session = connection.openSession().openFuture().get();

            SenderOptions options = new SenderOptions().deliveryMode(DeliveryMode.AT_MOST_ONCE);
            Sender sender = session.openSender("test-qos", options).openFuture().get();
            assertEquals("test-qos", sender.address());

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectTransfer().withPayload(notNullValue(ProtonBuffer.class));
            peer.expectDetach().respond();
            peer.expectClose().respond();

            final Message<String> message = Message.create("Hello World");
            final Tracker tracker;
            if (trySend) {
                // TODO: This can return null if the flow isn't processed in time
                tracker = sender.trySend(message);
            } else {
                tracker = sender.send(message);
            }

            assertNotNull(tracker);
            assertNotNull(tracker.acknowledgeFuture().isDone());
            assertNotNull(tracker.acknowledgeFuture().get().settled());

            sender.close().get(10, TimeUnit.SECONDS);

            connection.close().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 30000)
    public void testSenderIncrementsTransferTagOnEachSend() throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.SENDER).respond();
            peer.remoteFlow().withLinkCredit(10).queue();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Sender test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort()).openFuture().get();

            Session session = connection.openSession().openFuture().get();
            SenderOptions options = new SenderOptions().deliveryMode(DeliveryMode.AT_MOST_ONCE);
            Sender sender = session.openSender("test-tags", options).openFuture().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectTransfer().withPayload(notNullValue(ProtonBuffer.class))
                                 .withDeliveryTag(new byte[] {0});
            peer.expectTransfer().withPayload(notNullValue(ProtonBuffer.class))
                                 .withDeliveryTag(new byte[] {1});
            peer.expectTransfer().withPayload(notNullValue(ProtonBuffer.class))
                                 .withDeliveryTag(new byte[] {2});
            peer.expectDetach().respond();
            peer.expectClose().respond();

            final Message<String> message = Message.create("Hello World");
            final Tracker tracker1 = sender.send(message);
            final Tracker tracker2 = sender.send(message);
            final Tracker tracker3 = sender.send(message);

            assertNotNull(tracker1);
            assertNotNull(tracker1.acknowledgeFuture().get().settled());
            assertNotNull(tracker2);
            assertNotNull(tracker2.acknowledgeFuture().get().settled());
            assertNotNull(tracker3);
            assertNotNull(tracker3.acknowledgeFuture().get().settled());

            sender.close().get(10, TimeUnit.SECONDS);

            connection.close().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Repeat(repetitions = 1)
    @Test(timeout = 30000)
    public void testCreateAnonymousSenderFromWhenRemoteDoesNotOfferSupportForIt() throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectClose();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession().openFuture().get();

            try {
                session.openAnonymousSender();
                fail("Should not be able to open an anonymous sender when remote does not offer anonymous relay");
            } catch (ClientUnsupportedOperationException unsupported) {
                LOG.info("Caught expected error: ", unsupported);
            }

            connection.close();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Repeat(repetitions = 1)
    @Test(timeout = 30000)
    public void testAnonymousSenderOpenHeldUntilConnectionOpenedAndSupportConfirmed() throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen();
            peer.expectBegin();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession();
            Sender sender = session.openAnonymousSender();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            // This should happen after we inject the held open and attach
            peer.expectAttach().withRole(Role.SENDER).withTarget().withAddress(Matchers.nullValue()).and().respond();
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

    @Test(timeout = 30000)
    public void testSenderGetRemotePropertiesWaitsForRemoteAttach() throws Exception {
        tryReadSenderRemoteProperties(true);
    }

    @Test(timeout = 30000)
    public void testSenderGetRemotePropertiesFailsAfterOpenTimeout() throws Exception {
        tryReadSenderRemoteProperties(false);
    }

    private void tryReadSenderRemoteProperties(boolean attachResponse) throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.SENDER);
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Connect test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            ConnectionOptions options = new ConnectionOptions().openTimeout(75);
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), options);
            connection.openFuture().get();

            Session session = connection.openSession();
            session.openFuture().get();

            Sender sender = session.openSender("test-sender");

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
                assertNotNull("Remote should have responded with a remote properties value", sender.properties());
                assertEquals(expectedProperties, sender.properties());
            } else {
                try {
                    sender.properties();
                    fail("Should failed to get remote state due to no attach response");
                } catch (ClientException ex) {
                    LOG.debug("Caught expected exception from blocking call", ex);
                }
            }

            if (attachResponse) {
                sender.close().get();
            } else {
                try {
                    sender.close().get();
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
            peer.expectAttach().withRole(Role.SENDER);
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Connect test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            ConnectionOptions options = new ConnectionOptions().openTimeout(75);
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), options);
            connection.openFuture().get();

            Session session = connection.openSession();
            session.openFuture().get();

            Sender sender = session.openSender("test-sender");

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            if (attachResponse) {
                peer.expectDetach().respond();
                peer.respondToLastAttach().withOfferedCapabilities("QUEUE").later(10);
            } else {
                peer.expectDetach();
            }

            if (attachResponse) {
                assertNotNull("Remote should have responded with a remote offered Capabilities value", sender.offeredCapabilities());
                assertEquals(1, sender.offeredCapabilities().length);
                assertEquals("QUEUE", sender.offeredCapabilities()[0]);
            } else {
                try {
                    sender.offeredCapabilities();
                    fail("Should failed to get remote state due to no attach response");
                } catch (ClientException ex) {
                    LOG.debug("Caught expected exception from blocking call", ex);
                }
            }

            if (attachResponse) {
                sender.close().get();
            } else {
                try {
                    sender.close().get();
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
            peer.expectAttach().withRole(Role.SENDER);
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Connect test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            ConnectionOptions options = new ConnectionOptions().openTimeout(75);
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), options);
            connection.openFuture().get();

            Session session = connection.openSession();
            session.openFuture().get();

            Sender sender = session.openSender("test-sender");

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            if (attachResponse) {
                peer.expectDetach().respond();
                peer.respondToLastAttach().withDesiredCapabilities("Error-Free").later(10);
            } else {
                peer.expectDetach();
            }

            if (attachResponse) {
                assertNotNull("Remote should have responded with a remote desired Capabilities value", sender.desiredCapabilities());
                assertEquals(1, sender.desiredCapabilities().length);
                assertEquals("Error-Free", sender.desiredCapabilities()[0]);
            } else {
                try {
                    sender.desiredCapabilities();
                    fail("Should failed to get remote state due to no attach response");
                } catch (ClientException ex) {
                    LOG.debug("Caught expected exception from blocking call", ex);
                }
            }

            if (attachResponse) {
                sender.close().get();
            } else {
                try {
                    sender.close().get();
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
    public void testCloseSenderWithErrorCondition() throws Exception {
        doTestCloseOrDetachWithErrorCondition(true);
    }

    @Test(timeout = 30000)
    public void testDetachSenderWithErrorCondition() throws Exception {
        doTestCloseOrDetachWithErrorCondition(false);
    }

    public void doTestCloseOrDetachWithErrorCondition(boolean close) throws Exception {
        final String condition = "amqp:link:detach-forced";
        final String description = "something bad happened.";

        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.SENDER).respond();
            peer.expectDetach().withClosed(close).withError(condition, description).respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession();
            Sender sender = session.openSender("test-sender");
            sender.openFuture().get();

            if (close) {
                sender.close(ErrorCondition.create(condition, description, null));
            } else {
                sender.detach(ErrorCondition.create(condition, description, null));
            }

            connection.close().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }
}
