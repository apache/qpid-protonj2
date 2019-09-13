package org.messaginghub.amqperative;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.net.URI;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.proton4j.amqp.driver.netty.NettyTestPeer;
import org.apache.qpid.proton4j.amqp.messaging.Target;
import org.apache.qpid.proton4j.amqp.transport.AmqpError;
import org.apache.qpid.proton4j.amqp.transport.ErrorCondition;
import org.apache.qpid.proton4j.amqp.transport.Role;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.junit.Test;
import org.messaginghub.amqperative.impl.exceptions.ClientSendTimedOutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SenderTest {

    private static final Logger LOG = LoggerFactory.getLogger(SenderTest.class);

    @Test(timeout = 60000)
    public void testCreateSenderAndClose() throws Exception {
        doTestCreateSenderAndCloseOrDeatch(true);
    }

    @Test(timeout = 60000)
    public void testCreateSenderAndDetach() throws Exception {
        doTestCreateSenderAndCloseOrDeatch(false);
    }

    private void doTestCreateSenderAndCloseOrDeatch(boolean close) throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectAMQPHeader().respondWithAMQPHeader();
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

    @Test(timeout = 60000)
    public void testSenderOpenRejectedByRemote() throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectAMQPHeader().respondWithAMQPHeader();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().respond().withTarget((Target) null);
            peer.remoteDetach().withErrorCondition(new ErrorCondition(AmqpError.UNAUTHORIZED_ACCESS, "Cannot read from this address")).queue();
            peer.expectDetach();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Connect test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());

            connection.openFuture().get(10, TimeUnit.SECONDS);

            Session session = connection.openSession();
            session.openFuture().get(10, TimeUnit.SECONDS);

            Sender sender = session.openSender("test-queue");
            sender.openFuture().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            // Should not result in any close being sent now, already closed.
            sender.close().get(10, TimeUnit.SECONDS);

            peer.expectClose().respond();
            connection.close().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(1, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 60000)
    public void testSendTimesOutWhenNoCreditIssued() throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectAMQPHeader().respondWithAMQPHeader();
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
            options.setSendTimeout(1);
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

    @Test(timeout = 60000)
    public void testSendCompletesWhenCreditEventuallyOffered() throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectAMQPHeader().respondWithAMQPHeader();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.SENDER).respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Sender test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            ConnectionOptions options = new ConnectionOptions();
            options.setSendTimeout(200);
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
                fail("Should throw a send timed out exception");
            }

            sender.close().get(10, TimeUnit.SECONDS);

            connection.close().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }
    @Test(timeout = 60000)
    public void testSendWhenCreditIsAvailable() throws Exception {
        doTestSendWhenCreditIsAvailable(false);
    }

    @Test(timeout = 60000)
    public void testTrySendWhenCreditIsAvailable() throws Exception {
        doTestSendWhenCreditIsAvailable(true);
    }

    private void doTestSendWhenCreditIsAvailable(boolean trySend) throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectAMQPHeader().respondWithAMQPHeader();
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
            Receiver receiver = session.openReceiver("test-queue");
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

    @Test(timeout = 60000)
    public void testTrySendWhenNoCreditAvailable() throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectAMQPHeader().respondWithAMQPHeader();
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
            options.setSendTimeout(1);
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
}
