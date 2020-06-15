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

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.URI;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.proton4j.amqp.driver.netty.NettyTestPeer;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.types.Binary;
import org.apache.qpid.proton4j.types.messaging.Accepted;
import org.apache.qpid.proton4j.types.messaging.AmqpValue;
import org.apache.qpid.proton4j.types.messaging.Modified;
import org.apache.qpid.proton4j.types.messaging.Rejected;
import org.apache.qpid.proton4j.types.messaging.Released;
import org.apache.qpid.proton4j.types.transactions.TransactionalState;
import org.apache.qpid.proton4j.types.transport.Role;
import org.junit.Test;
import org.messaginghub.amqperative.exceptions.ClientIllegalStateException;
import org.messaginghub.amqperative.exceptions.ClientOperationTimedOutException;
import org.messaginghub.amqperative.exceptions.ClientTransactionRolledBackException;
import org.messaginghub.amqperative.test.AMQPerativeTestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionsTest extends AMQPerativeTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionsTest.class);

    @Test(timeout = 20_000)
    public void testCoordinatorLinkSupportedOutcomes() throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectCoordinatorAttach().withSource().withOutcomes(Accepted.DESCRIPTOR_SYMBOL,
                                                                     Rejected.DESCRIPTOR_SYMBOL,
                                                                     Released.DESCRIPTOR_SYMBOL,
                                                                     Modified.DESCRIPTOR_SYMBOL).and().respond();
            peer.remoteFlow().withLinkCredit(2).queue();
            peer.expectDeclare().accept();
            peer.expectDischarge().accept();
            peer.expectEnd().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Sender test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession().openFuture().get();

            session.begin();
            session.commit();

            session.close();
            connection.close().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 20_000)
    public void testTimedOutExceptionOnBeginWithNoResponse() throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectCoordinatorAttach().respond();
            peer.remoteFlow().withLinkCredit(2).queue();
            peer.expectDeclare();
            peer.expectEnd().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Sender test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            ConnectionOptions options = new ConnectionOptions().requestTimeout(50);
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), options);
            Session session = connection.openSession().openFuture().get();

            try {
                session.begin();
                fail("Begin should have timoued out after no response.");
            } catch (ClientOperationTimedOutException expected) {
                // Expect this to time out.
            }

            session.close();
            connection.close().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    /**
     * Create a transaction and then close the Session which result in the remote rolling back
     * the transaction by default so the client doesn't manually roll it back itself.
     *
     * @throws Exception
     */
    @Test(timeout = 20_000)
    public void testBeginTransactionAndClose() throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectCoordinatorAttach().respond();
            peer.remoteFlow().withLinkCredit(2).queue();
            peer.expectDeclare().accept();
            peer.expectEnd().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Sender test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession().openFuture().get();

            session.begin();

            session.close();
            connection.close().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 20_000)
    public void testBeginAndCommitTransaction() throws Exception {
        final byte[] txnId = new byte[] { 0, 1, 2, 3 };

        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectCoordinatorAttach().respond();
            peer.remoteFlow().withLinkCredit(2).queue();
            peer.expectDeclare().accept(txnId);
            peer.expectDischarge().withFail(false).withTxnId(txnId).accept();
            peer.expectEnd().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Sender test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession().openFuture().get();

            session.begin();
            session.commit();

            session.close();
            connection.close().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 20_000)
    public void testBeginAndCommitTransactions() throws Exception {
        final byte[] txnId1 = new byte[] { 0, 1, 2, 3 };
        final byte[] txnId2 = new byte[] { 1, 1, 2, 3 };
        final byte[] txnId3 = new byte[] { 2, 1, 2, 3 };
        final byte[] txnId4 = new byte[] { 3, 1, 2, 3 };
        final byte[] txnId5 = new byte[] { 4, 1, 2, 3 };

        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectCoordinatorAttach().respond();
            peer.remoteFlow().withLinkCredit(10).queue();
            peer.expectDeclare().accept(txnId1);
            peer.expectDischarge().withFail(false).withTxnId(txnId1).accept();
            peer.expectDeclare().accept(txnId2);
            peer.expectDischarge().withFail(false).withTxnId(txnId2).accept();
            peer.expectDeclare().accept(txnId3);
            peer.expectDischarge().withFail(false).withTxnId(txnId3).accept();
            peer.expectDeclare().accept(txnId4);
            peer.expectDischarge().withFail(false).withTxnId(txnId4).accept();
            peer.expectDeclare().accept(txnId5);
            peer.expectDischarge().withFail(false).withTxnId(txnId5).accept();
            peer.expectEnd().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Sender test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession().openFuture().get();

            for (int i = 0; i < 5; ++i) {
                LOG.info("Transaction declare and discharge cycle: {}", i);
                session.begin();
                session.commit();
            }

            session.close();
            connection.close().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 20_000)
    public void testCannotBeginSecondTransactionWhileFirstIsActive() throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectCoordinatorAttach().respond();
            peer.remoteFlow().withLinkCredit(2).queue();
            peer.expectDeclare().accept();
            peer.expectEnd().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Sender test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession().openFuture().get();

            session.begin();

            try {
                session.begin();
                fail("Should not be allowed to begin another transaction");
            } catch (ClientIllegalStateException cliEx) {
                // Expected
            }

            session.close();
            connection.close().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 20_000)
    public void testSendMessageInsideOfTransaction() throws Exception {
        final byte[] txnId = new byte[] { 0, 1, 2, 3 };

        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.SENDER).respond();
            peer.remoteFlow().withLinkCredit(1).queue();
            peer.expectCoordinatorAttach().respond();
            peer.remoteFlow().withLinkCredit(2).queue();
            peer.expectDeclare().accept(txnId);
            peer.expectTransfer().withHandle(0)
                                 .withPayload(notNullValue(ProtonBuffer.class))
                                 .withState(new TransactionalState().setTxnId(new Binary(txnId)))
                                 .respond()
                                 .withState(new TransactionalState().setTxnId(new Binary(txnId)).setOutcome(Accepted.getInstance()))
                                 .withSettled(true);
            peer.expectDischarge().withFail(false).withTxnId(txnId).accept();
            peer.expectEnd().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Sender test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession().openFuture().get();
            Sender sender = session.openSender("address").openFuture().get();

            session.begin();

            final Tracker tracker = sender.send(Message.create("test-message"));

            assertNotNull(tracker);
            assertNotNull(tracker.acknowledgeFuture().get());
            assertEquals(tracker.remoteState().getType(), DeliveryState.Type.TRANSACTIONAL);
            assertNotNull(tracker.state());
            assertEquals(tracker.state().getType(), DeliveryState.Type.TRANSACTIONAL);
            assertTrue(tracker.settled());

            session.commit();

            session.close();
            connection.close().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 20_000)
    public void testReceiveMessageInsideOfTransaction() throws Exception {
        final byte[] txnId = new byte[] { 0, 1, 2, 3 };

        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.RECEIVER).respond();
            peer.expectFlow();
            peer.start();

            final URI remoteURI = peer.getServerURI();
            final ProtonBuffer payload = createEncodedMessage(new AmqpValue("Hello World"));

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession();
            Receiver receiver = session.openReceiver("test-queue").openFuture().get();

            peer.expectCoordinatorAttach().respond();
            peer.remoteFlow().withLinkCredit(2).queue();
            peer.expectDeclare().accept(txnId);
            peer.remoteTransfer().withHandle(0)
                                 .withDeliveryId(0)
                                 .withDeliveryTag(new byte[] { 1 })
                                 .withMore(false)
                                 .withMessageFormat(0)
                                 .withPayload(payload).queue();
            peer.expectDisposition().withSettled(true)
                                    .withState(new TransactionalState().setOutcome(Accepted.getInstance()).setTxnId(new Binary(txnId)));
            peer.expectDischarge().withFail(false).withTxnId(txnId).accept();
            peer.expectDetach().respond();
            peer.expectClose().respond();

            session.begin();

            Delivery delivery = receiver.receive(100);
            assertNotNull(delivery);
            Message<?> received = delivery.message();
            assertNotNull(received);
            assertTrue(received.body() instanceof String);
            String value = (String) received.body();
            assertEquals("Hello World", value);

            session.commit();
            receiver.close();
            connection.close().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 20_000)
    public void testTransactionCommitFailWithEmptyRejectedDisposition() throws Exception {
        final byte[] txnId = new byte[] { 0, 1, 2, 3 };

        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.SENDER).respond();
            peer.remoteFlow().withLinkCredit(1).queue();
            peer.expectCoordinatorAttach().respond();
            peer.remoteFlow().withLinkCredit(2).queue();
            peer.expectDeclare().accept(txnId);
            peer.expectTransfer().withHandle(0)
                                 .withPayload(notNullValue(ProtonBuffer.class))
                                 .withState(new TransactionalState().setTxnId(new Binary(txnId)))
                                 .respond()
                                 .withState(new TransactionalState().setTxnId(new Binary(txnId)).setOutcome(Accepted.getInstance()))
                                 .withSettled(true);
            peer.expectDischarge().withFail(false).withTxnId(txnId).reject();
            peer.expectEnd().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Sender test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession().openFuture().get();
            Sender sender = session.openSender("address").openFuture().get();

            session.begin();

            final Tracker tracker = sender.send(Message.create("test-message"));
            assertNotNull(tracker.acknowledgeFuture().get());
            assertEquals(tracker.remoteState().getType(), DeliveryState.Type.TRANSACTIONAL);

            try {
                session.commit();
                fail("Commit should fail with Rollback exception");
            } catch (ClientTransactionRolledBackException cliRbEx) {
                // Expected roll back due to discharge rejection
            }

            session.close();
            connection.close().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }
}
