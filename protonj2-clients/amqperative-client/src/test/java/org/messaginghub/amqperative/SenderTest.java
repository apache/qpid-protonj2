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
import org.messaginghub.amqperative.client.exceptions.ClientSendTimedOutException;
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

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectTransfer().withPayload(notNullValue(ProtonBuffer.class));
            peer.expectDetach().respond();
            peer.expectClose().respond();

            Message<String> message = Message.create("Hello World");

            // TODO - Race on try send and credit could use some way to ensure there is credit
            //        before a send call is made.

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
