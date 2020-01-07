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
package org.messaginghub.amqperative;

import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.qpid.proton4j.amqp.driver.matchers.messaging.SourceMatcher;
import org.apache.qpid.proton4j.amqp.driver.netty.NettyTestPeer;
import org.apache.qpid.proton4j.amqp.transport.AMQPHeader;
import org.apache.qpid.proton4j.amqp.transport.AmqpError;
import org.apache.qpid.proton4j.amqp.transport.ConnectionError;
import org.apache.qpid.proton4j.amqp.transport.ErrorCondition;
import org.apache.qpid.proton4j.amqp.transport.Role;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.hamcrest.Matchers;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.messaginghub.amqperative.impl.ClientConstants;
import org.messaginghub.amqperative.impl.ClientException;
import org.messaginghub.amqperative.impl.exceptions.ClientConnectionRemotelyClosedException;
import org.messaginghub.amqperative.impl.exceptions.ClientUnsupportedOperationException;
import org.messaginghub.amqperative.test.AMQPerativeTestCase;
import org.messaginghub.amqperative.util.AmqperativeTestRunner;
import org.messaginghub.amqperative.util.Repeat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for the Connection class
 */
@RunWith(AmqperativeTestRunner.class)
public class ConnectionTest extends AMQPerativeTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectionTest.class);

    @Test(timeout = 10000)
    public void testCreateConnectionToNonSaslPeer() throws Exception {
        doConnectionWithUnexpectedHeaderTestImpl(AMQPHeader.getAMQPHeader().getBuffer());;
    }

    @Test(timeout = 10000)
    public void testCreateConnectionToNonAmqpPeer() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.wrap(new byte[] { 'N', 'O', 'T', '-', 'A', 'M', 'Q', 'P' });
        doConnectionWithUnexpectedHeaderTestImpl(buffer);
    }

    private void doConnectionWithUnexpectedHeaderTestImpl(ProtonBuffer responseHeader) throws Exception, IOException {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLHeader().respondWithBytes(responseHeader);
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Connect test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            ConnectionOptions options = new ConnectionOptions();
            options.user("guest");
            options.password("guest");
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), options);

            try {
                connection.openFuture().get(10, TimeUnit.SECONDS);
            } catch (ExecutionException ex) {}

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 60000)
    public void testCreateConnectionString() throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Connect test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());

            connection.openFuture().get(10, TimeUnit.SECONDS);
            connection.close().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 60000)
    public void testCreateConnectionStringWithDefaultTcpPort() throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Connect test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            ConnectionOptions options = new ConnectionOptions();
            options.transportOptions().defaultTcpPort(remoteURI.getPort());
            Connection connection = container.connect(remoteURI.getHost(), options);

            connection.openFuture().get(10, TimeUnit.SECONDS);
            connection.close().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 60000)
    public void testConnectionCloseGetsResponseWithErrorDoesNotThrowTimedGet() throws Exception {
        doTestConnectionCloseGetsResponseWithErrorDoesNotThrow(true);
    }

    @Test(timeout = 60000)
    public void testConnectionCloseGetsResponseWithErrorDoesNotThrowUntimedGet() throws Exception {
        doTestConnectionCloseGetsResponseWithErrorDoesNotThrow(false);
    }

    protected void doTestConnectionCloseGetsResponseWithErrorDoesNotThrow(boolean tiemout) throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectClose().respond().
                               withErrorCondition(new ErrorCondition(ConnectionError.CONNECTION_FORCED, "Not accepting connections"));
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Connect test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());

            if (tiemout) {
                connection.openFuture().get(10, TimeUnit.SECONDS);
                // Should close normally and not throw error as we initiated the close.
                connection.close().get(10, TimeUnit.SECONDS);
            } else {
                connection.openFuture().get();
                // Should close normally and not throw error as we initiated the close.
                connection.close().get();
            }

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 60000)
    public void testConnectionRemoteClosedAfterOpened() throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.remoteClose().withErrorCondition(
                    new ErrorCondition(ConnectionError.CONNECTION_FORCED, "Not accepting connections")).queue();
            peer.expectClose();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Connect test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());

            connection.openFuture().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            connection.close().get(10, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 60000)
    public void testConnectionOpenFutureWaitCancelledOnConnectionDropWithTimeout() throws Exception {
        doTestConnectionOpenFutureWaitCancelledOnConnectionDrop(true);
    }

    @Test(timeout = 60000)
    public void testConnectionOpenFutureWaitCancelledOnConnectionDropNoTimeout() throws Exception {
        doTestConnectionOpenFutureWaitCancelledOnConnectionDrop(false);
    }

    protected void doTestConnectionOpenFutureWaitCancelledOnConnectionDrop(boolean timeout) throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Connect test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());

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
                    connection.close().get(10, TimeUnit.SECONDS);
                } else {
                    connection.close().get();
                }
            } catch (Throwable error) {
                LOG.info("connection close failed with error: ", error);
                fail("Close should ignore connect error and complete without error.");
            }
        }
    }

    @Test(timeout = 20000)
    public void testRemotelyCloseConnectionDuringSessionCreation() throws Exception {
        final String BREAD_CRUMB = "ErrorMessageBreadCrumb";

        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin();
            peer.remoteClose().withErrorCondition(new ErrorCondition(AmqpError.NOT_ALLOWED, BREAD_CRUMB)).queue();
            peer.expectClose();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            connection.openFuture().get(10, TimeUnit.SECONDS);

            Session session = connection.openSession();

            try {
                session.openFuture().get(5, TimeUnit.SECONDS);
                fail("Open should throw error when waiting for remote open and connection remotely closed.");
            } catch (ExecutionException error) {
                LOG.info("Session open failed with error: ", error);
                assertNotNull("Expected exception to have a message", error.getMessage());
                assertTrue("Expected breadcrumb to be present in message", error.getMessage().contains(BREAD_CRUMB));
                assertNotNull("Execution error should convery the cause", error.getCause());
                assertTrue(error.getCause() instanceof ClientConnectionRemotelyClosedException);
            }

            session.close().get(5, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            connection.close().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 60000)
    public void testConnectionOpenTimeoutWhenNoRemoteOpenArrivesTimeout() throws Exception {
        doTestConnectionOpenTimeoutWhenNoRemoteOpenArrives(true);
    }

    @Test(timeout = 60000)
    public void testConnectionOpenTimeoutWhenNoRemoteOpenArrivesNoTimeout() throws Exception {
        doTestConnectionOpenTimeoutWhenNoRemoteOpenArrives(false);
    }

    private void doTestConnectionOpenTimeoutWhenNoRemoteOpenArrives(boolean timeout) throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen();
            peer.expectClose();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            ConnectionOptions options = new ConnectionOptions();
            options.openTimeout(75);

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

    @Test(timeout = 60000)
    public void testConnectionCloseTimeoutWhenNoRemoteCloseArrivesTimeout() throws Exception {
        doTestConnectionCloseTimeoutWhenNoRemoteCloseArrives(true);
    }

    @Test(timeout = 60000)
    public void testConnectionCloseTimeoutWhenNoRemoteCloseArrivesNoTimeout() throws Exception {
        doTestConnectionCloseTimeoutWhenNoRemoteCloseArrives(false);
    }

    private void doTestConnectionCloseTimeoutWhenNoRemoteCloseArrives(boolean timeout) throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectClose();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            ConnectionOptions options = new ConnectionOptions();
            options.closeTimeout(75);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), options);

            connection.openFuture().get(10, TimeUnit.SECONDS);

            // Shouldn't throw from close, nothing to be done anyway.
            try {
                if (timeout) {
                    connection.close().get(10, TimeUnit.SECONDS);
                } else {
                    connection.close().get();
                }
            } catch (Throwable error) {
                LOG.info("connection close failed with error: ", error);
                fail("Close should ignore lack of close response and complete without error.");
            }

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Repeat(repetitions = 1)
    @Test(timeout = 60000)
    public void testCreateDefaultSenderFailsOnConnectionWithoutSupportForAnonymousRelay() throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectClose();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Connect test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());

            try {
                connection.openFuture().get();
            } catch (ExecutionException ex) {}

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

    @Test(timeout = 60000)
    public void testCreateDefaultSenderOnConnectionWithSupportForAnonymousRelay() throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().withDesiredCapabilities(ClientConstants.ANONYMOUS_RELAY)
                             .respond()
                             .withOfferedCapabilities(ClientConstants.ANONYMOUS_RELAY);
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.SENDER).respond();
            peer.expectClose();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Connect test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            connection.openFuture().get(10, TimeUnit.SECONDS);

            Sender defaultSender = connection.defaultSender().openFuture().get(5, TimeUnit.SECONDS);
            assertNotNull(defaultSender);

            connection.close();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 60000)
    public void testConnectionRecreatesAnonymousRelaySenderAfterRemoteCloseOfSender() throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().withDesiredCapabilities(ClientConstants.ANONYMOUS_RELAY)
                             .respond()
                             .withOfferedCapabilities(ClientConstants.ANONYMOUS_RELAY);
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.SENDER).respond();
            peer.remoteDetach().queue();
            peer.expectDetach();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Connect test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            connection.openFuture().get(10, TimeUnit.SECONDS);

            Sender defaultSender = connection.defaultSender().openFuture().get(5, TimeUnit.SECONDS);
            assertNotNull(defaultSender);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectAttach().withRole(Role.SENDER).respond();
            peer.expectClose();

            defaultSender = connection.defaultSender().openFuture().get(5, TimeUnit.SECONDS);
            assertNotNull(defaultSender);

            connection.close();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
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

            Receiver receiver = connection.openDynamicReceiver();
            receiver.openFuture().get(10, TimeUnit.SECONDS);

            assertNotNull("Remote should have assigned the address for the dynamic receiver", receiver.address());

            receiver.close().get(10, TimeUnit.SECONDS);

            connection.close().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Repeat(repetitions = 1)
    @Test(timeout = 60000)
    public void testConnectionSenderOpenHeldUntilConnectionOpenedAndRelaySupportConfirmed() throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen();
            peer.expectBegin();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Sender sender = connection.defaultSender();

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

    @Ignore("Skipped for now, needs server, and proton changes")//TODO
    @Test
    public void testSendAndReceiveMessage() throws InterruptedException, ExecutionException, TimeoutException, ClientException {
        Client container = Client.create();
        System.out.println("Created container");

        Connection conn = container.connect("localhost", 5672);
        System.out.println("Connection creation started (or already failed), waiting.");

        int timeout = 300;
        conn.openFuture().get(timeout, TimeUnit.SECONDS);
        System.out.println("Open completed successfully");

        ReceiverOptions receiverOptions = new ReceiverOptions().creditWindow(10);

        Receiver receiver = conn.openReceiver("queue", receiverOptions);
        receiver.openFuture().get(timeout, TimeUnit.SECONDS);

        Sender sender = conn.openSender("queue");

        sender.openFuture().get(timeout, TimeUnit.SECONDS);
        System.out.println("Sender created successfully");

        Thread.sleep(200);//TODO: remove, hack to allow sender to become sendable first.

        int count = 100;
        for (int i = 1; i <= count; i++) {
            //TODO: This fails if a prev message wasn't locally settled yet (which I'm deliberately not doing here,
            //      instead tweaked proton current() method to use !isPartial() rather than isSettled(..but that causes test failures))
            Tracker tracker = sender.send(Message.create("myBasicTextMessage" + i));
            System.out.println("Sent message " + i);

            Delivery delivery = receiver.receive(1000);

            if (delivery == null) {
                throw new IllegalStateException("Expected delivery but did not get one");
            }

            System.out.println("Got message body: " + delivery.message().body());

            delivery.accept();

            Thread.sleep(20); //TODO: remove, hack to give time for settlement propagation (when send+receive done end to end via dispatch router)
            System.out.println("Settled: " + tracker.remotelySettled());
            //TODO: should locally settle sent delivery..if sender not set to 'auto settle' when peer does.
        }

        Future<Connection> closing = conn.close();
        System.out.println("Close started, waiting.");

        closing.get(3, TimeUnit.SECONDS);
        System.out.println("Close completed");
    }
}
