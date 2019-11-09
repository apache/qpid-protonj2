package org.messaginghub.amqperative;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.URI;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.proton4j.amqp.driver.matchers.messaging.SourceMatcher;
import org.apache.qpid.proton4j.amqp.driver.netty.NettyTestPeer;
import org.apache.qpid.proton4j.amqp.messaging.Source;
import org.apache.qpid.proton4j.amqp.transport.AmqpError;
import org.apache.qpid.proton4j.amqp.transport.ErrorCondition;
import org.apache.qpid.proton4j.amqp.transport.Role;
import org.hamcrest.Matcher;
import org.junit.Test;
import org.messaginghub.amqperative.impl.exceptions.ClientOperationTimedOutException;
import org.messaginghub.amqperative.impl.exceptions.ClientSecurityException;
import org.messaginghub.amqperative.test.AMQPerativeTestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReceiverTest extends AMQPerativeTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(ReceiverTest.class);

    @Test(timeout = 60000)
    public void testCreateReceiverAndClose() throws Exception {
        doTestCreateReceiverAndCloseOrDetachLink(true);
    }

    @Test(timeout = 60000)
    public void testCreateReceiverAndDetach() throws Exception {
        doTestCreateReceiverAndCloseOrDetachLink(false);
    }

    private void doTestCreateReceiverAndCloseOrDetachLink(boolean close) throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.RECEIVER).respond();  // TODO match other options
            peer.expectDetach().withClosed(close).respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Connect test started, peer listening on: {}", remoteURI);

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

            LOG.info("Receiver test completed normally");
        }
    }

    @Test(timeout = 60000)
    public void testReceiverOpenRejectedByRemote() throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().respond().withSource((Source) null);
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

            Receiver receiver = session.openReceiver("test-queue");
            try {
                receiver.openFuture().get(10, TimeUnit.SECONDS);
                fail("Open of receiver should fail due to remote indicating pending close.");
            } catch (ExecutionException exe) {
                assertNotNull(exe.getCause());
                assertTrue(exe.getCause() instanceof ClientSecurityException);
            }

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            // Should not result in any close being sent now, already closed.
            receiver.close().get(10, TimeUnit.SECONDS);

            peer.expectClose().respond();
            connection.close().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(1, TimeUnit.SECONDS);
            LOG.info("Receiver test completed normally");
        }
    }

    @Test(timeout = 60000)
    public void testCloseReceiverTimesOutWhenNoCloseResponseReceived() throws Exception {
        doTestCloseOrDetachReceiverTimesOutWhenNoCloseResponseReceived(true);
    }

    @Test(timeout = 60000)
    public void testDetachReceiverTimesOutWhenNoCloseResponseReceived() throws Exception {
        doTestCloseOrDetachReceiverTimesOutWhenNoCloseResponseReceived(false);
    }

    private void doTestCloseOrDetachReceiverTimesOutWhenNoCloseResponseReceived(boolean close) throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.RECEIVER).respond();
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
                    receiver.close().get(10, TimeUnit.SECONDS);
                } else {
                    receiver.detach().get(10, TimeUnit.SECONDS);
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
        int creditWindow = 10;
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.RECEIVER).respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Connect test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());

            connection.openFuture().get(5, TimeUnit.SECONDS);

            Session session = connection.openSession();
            session.openFuture().get(5, TimeUnit.SECONDS);

            Receiver receiver = session.openReceiver("test-queue");
            receiver.openFuture().get(5, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            // Add some credit, verify not draining
            Matcher<Boolean> drainMatcher = anyOf(equalTo(false), nullValue());
            peer.expectFlow().withDrain(drainMatcher).withLinkCredit(creditWindow).withDeliveryCount(0);

            receiver.addCredit(creditWindow);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            // Drain all the credit
            peer.expectFlow().withDrain(true).withLinkCredit(creditWindow).withDeliveryCount(0)
                    .respond().withDrain(true).withLinkCredit(0).withDeliveryCount(creditWindow);

            Future<Receiver> draining = receiver.drain();
            draining.get(5, TimeUnit.SECONDS);

            // Close things down
            peer.expectClose().respond();
            connection.close().get(5, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(1, TimeUnit.SECONDS);
            LOG.info("Receiver test completed normally");
        }
    }

    @Test(timeout = 60000)
    public void testCreateDynamicReceiver() throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.RECEIVER)
                               .withSource(new SourceMatcher().withDynamic(true).withAddress(nullValue()))
                               .respond();
            peer.expectDetach().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Connect test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            connection.openFuture().get(10, TimeUnit.SECONDS);

            Session session = connection.openSession();
            session.openFuture().get(10, TimeUnit.SECONDS);

            Receiver receiver = session.openDynamicReceiver();
            receiver.openFuture().get(10, TimeUnit.SECONDS);

            assertNotNull("Remote should have assigned the address for the dynamic receiver", receiver.address());

            receiver.close().get(10, TimeUnit.SECONDS);

            connection.close().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            LOG.info("Receiver test completed normally");
        }
    }
}
