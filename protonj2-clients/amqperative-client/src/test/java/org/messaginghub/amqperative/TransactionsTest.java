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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
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
import org.apache.qpid.proton4j.types.transactions.TransactionErrors;
import org.apache.qpid.proton4j.types.transactions.TransactionalState;
import org.apache.qpid.proton4j.types.transport.AmqpError;
import org.apache.qpid.proton4j.types.transport.Role;
import org.junit.Test;
import org.messaginghub.amqperative.exceptions.ClientException;
import org.messaginghub.amqperative.exceptions.ClientIllegalStateException;
import org.messaginghub.amqperative.exceptions.ClientOperationTimedOutException;
import org.messaginghub.amqperative.exceptions.ClientTransactionNotActiveException;
import org.messaginghub.amqperative.exceptions.ClientTransactionRolledBackException;
import org.messaginghub.amqperative.test.AMQPerativeTestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionsTest extends AMQPerativeTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionsTest.class);

    @Test(timeout = 20_000)
    public void testCoordinatorLinkSupportedOutcomes() throws Exception {
        final byte[] txnId = new byte[] { 0, 1, 2, 3 };

        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectCoordinatorAttach().withSource().withOutcomes(Accepted.DESCRIPTOR_SYMBOL,
                                                                     Rejected.DESCRIPTOR_SYMBOL,
                                                                     Released.DESCRIPTOR_SYMBOL,
                                                                     Modified.DESCRIPTOR_SYMBOL).and().respond();
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

            try {
                session.commit();
                fail("Commit should have failed due to no active transaction.");
            } catch (ClientIllegalStateException expected) {
                // Expect this to time out.
            }

            try {
                session.rollback();
                fail("Rollback should have failed due to no active transaction.");
            } catch (ClientIllegalStateException expected) {
                // Expect this to time out.
            }

            session.close();
            connection.close().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 20_000)
    public void testTimedOutExceptionOnBeginWithDelayedResponseDischargesTransaction() throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectCoordinatorAttach().respond();
            peer.remoteFlow().withLinkCredit(2).queue();
            peer.expectDeclare().accept().afterDelay(50);
            peer.expectDischarge().accept();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Sender test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            SessionOptions options = new SessionOptions().requestTimeout(25);
            Session session = connection.openSession(options).openFuture().get();

            try {
                session.begin();
                fail("Begin should have timoued out after no response.");
            } catch (ClientOperationTimedOutException expected) {
                LOG.info("Transaction begin timed out as expected");
            }

            // TODO: Handling of request timeout is disjoint from the operation making
            //       proper cleanup of current state not possible on the timeouet which
            //       means we are in an inconsistent state until the declared actually
            //       does arrive which isn't what we really want.  Need to revisit the
            //       request handling in connection.

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectEnd().respond();
            peer.expectClose().respond();

            try {
                session.commit();
                fail("Commit should have failed due to no active transaction.");
            } catch (ClientIllegalStateException expected) {
                // Expect this to time out.
            }

            try {
                session.rollback();
                fail("Rollback should have failed due to no active transaction.");
            } catch (ClientIllegalStateException expected) {
                // Expect this to time out.
            }

            session.close();
            connection.close().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 20_000)
    public void testExceptionOnBeginWhenCoordinatorLinkRefused() throws Exception {
        final String errorMessage = "CoordinatorLinkRefusal-breadcrumb";

        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectCoordinatorAttach().reject(true, AmqpError.NOT_IMPLEMENTED, errorMessage);
            peer.expectDetach();
            peer.expectEnd().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Sender test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession().openFuture().get();

            try {
                session.begin();
                fail("Begin should have failed after link closed.");
            } catch (ClientException expected) {
                // Expect this to time out.
                String message = expected.getMessage();
                assertTrue(message.contains(errorMessage));
            }

            try {
                session.commit();
                fail("Commit should have failed due to no active transaction.");
            } catch (ClientTransactionNotActiveException expected) {
                // Expect this as the begin failed on coordinator rejected
            }

            try {
                session.rollback();
                fail("Rollback should have failed due to no active transaction.");
            } catch (ClientTransactionNotActiveException expected) {
                // Expect this as the begin failed on coordinator rejected
            }

            session.close();
            connection.close().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 20_000)
    public void testExceptionOnBeginWhenCoordinatorLinkClosedAfterDeclare() throws Exception {
        final String errorMessage = "CoordinatorLinkClosed-breadcrumb";

        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectCoordinatorAttach().respond();
            peer.remoteFlow().withLinkCredit(2).queue();
            peer.expectDeclare();
            peer.remoteDetach().withClosed(true)
                               .withErrorCondition(AmqpError.NOT_IMPLEMENTED, errorMessage).queue();
            peer.expectDetach();
            peer.expectEnd().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Sender test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession().openFuture().get();

            try {
                session.begin();
                fail("Begin should have failed after link closed.");
            } catch (ClientException expected) {
                // Expect this to time out.
                String message = expected.getMessage();
                assertTrue(message.contains(errorMessage));
            }

            try {
                session.commit();
                fail("Commit should have failed due to no active transaction.");
            } catch (ClientTransactionNotActiveException expected) {
                // Expect this as the begin failed on coordinator close
            }

            try {
                session.rollback();
                fail("Rollback should have failed due to no active transaction.");
            } catch (ClientTransactionNotActiveException expected) {
                // Expect this as the begin failed on coordinator close
            }

            session.close();
            connection.close().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 20_000)
    public void testExceptionOnBeginWhenCoordinatorLinkClosedAfterDeclareAllowsNewTransactionDeclaration() throws Exception {
        final String errorMessage = "CoordinatorLinkClosed-breadcrumb";

        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectCoordinatorAttach().respond();
            peer.remoteFlow().withLinkCredit(2).queue();
            peer.expectDeclare();
            peer.remoteDetach().withClosed(true)
                               .withErrorCondition(AmqpError.NOT_IMPLEMENTED, errorMessage).queue();
            peer.expectDetach();
            peer.expectCoordinatorAttach().respond();
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

            try {
                session.begin();
                fail("Begin should have failed after link closed.");
            } catch (ClientException expected) {
                // Expect this to time out.
                String message = expected.getMessage();
                assertTrue(message.contains(errorMessage));
            }

            // Try again and expect to return to normal state now.
            session.begin();
            session.commit();

            session.close();
            connection.close().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 20_000)
    public void testExceptionOnCommitWhenCoordinatorLinkClosedAfterDischargeSent() throws Exception {
        final String errorMessage = "CoordinatorLinkClosed-breadcrumb";

        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectCoordinatorAttach().respond();
            peer.remoteFlow().withLinkCredit(2).queue();
            peer.expectDeclare().accept();
            peer.expectDischarge();
            peer.remoteDetach().withClosed(true)
                               .withErrorCondition(AmqpError.RESOURCE_DELETED, errorMessage).queue();
            peer.expectDetach();
            peer.expectCoordinatorAttach().respond();
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

            try {
                session.commit();
                fail("Commit should have failed after link closed.");
            } catch (ClientTransactionRolledBackException expected) {
                // Expect this to time out.
                String message = expected.getMessage();
                assertTrue(message.contains(errorMessage));
            }

            session.begin();
            session.rollback();

            session.close();
            connection.close().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 20_000)
    public void testExceptionOnCommitWhenCoordinatorRejectsDischarge() throws Exception {
        final String errorMessage = "Transaction aborted due to timeout";
        final byte[] txnId1 = new byte[] { 0, 1, 2, 3 };
        final byte[] txnId2 = new byte[] { 1, 1, 2, 3 };

        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectCoordinatorAttach().respond();
            peer.remoteFlow().withLinkCredit(4).queue();
            peer.expectDeclare().accept(txnId1);
            peer.expectDischarge().withFail(false)
                                  .withTxnId(txnId1)
                                  .reject(TransactionErrors.TRANSACTION_TIMEOUT, "Transaction aborted due to timeout");
            peer.expectDeclare().accept(txnId2);
            peer.expectDischarge().withFail(true).withTxnId(txnId2).accept();
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
                session.commit();
                fail("Commit should have failed after link closed.");
            } catch (ClientTransactionRolledBackException expected) {
                // Expect this to time out.
                String message = expected.getMessage();
                assertTrue(message.contains(errorMessage));
            }

            session.begin();
            session.rollback();

            session.close();
            connection.close().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 20_000)
    public void testExceptionOnRollbackWhenCoordinatorRejectsDischarge() throws Exception {
        final String errorMessage = "Transaction aborted due to timeout";
        final byte[] txnId1 = new byte[] { 0, 1, 2, 3 };
        final byte[] txnId2 = new byte[] { 1, 1, 2, 3 };

        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectCoordinatorAttach().respond();
            peer.remoteFlow().withLinkCredit(4).queue();
            peer.expectDeclare().accept(txnId1);
            peer.expectDischarge().withFail(true)
                                  .withTxnId(txnId1)
                                  .reject(TransactionErrors.TRANSACTION_TIMEOUT, "Transaction aborted due to timeout");
            peer.expectDeclare().accept(txnId2);
            peer.expectDischarge().withFail(false).withTxnId(txnId2).accept();
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
                session.rollback();
                fail("Commit should have failed after link closed.");
            } catch (ClientTransactionRolledBackException expected) {
                // Expect this to time out.
                String message = expected.getMessage();
                assertTrue(message.contains(errorMessage));
            }

            session.begin();
            session.commit();

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
    public void testBeginAndRollbackTransaction() throws Exception {
        final byte[] txnId = new byte[] { 0, 1, 2, 3 };

        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectCoordinatorAttach().respond();
            peer.remoteFlow().withLinkCredit(2).queue();
            peer.expectDeclare().accept(txnId);
            peer.expectDischarge().withFail(true).withTxnId(txnId).accept();
            peer.expectEnd().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Sender test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession().openFuture().get();

            session.begin();
            session.rollback();

            session.close();
            connection.close().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 20_000)
    public void testTransactionDeclaredDispositionWithoutTxnId() throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectCoordinatorAttach().respond();
            peer.remoteFlow().withLinkCredit(1).queue();
            peer.expectDeclare().accept(null);
            peer.expectClose().withError(AmqpError.DECODE_ERROR, "The txn-id field cannot be omitted").respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Sender test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession().openFuture().get();

            try {
                session.begin();
                fail("Should not complete transaction begin due to client connection failure on decode issue.");
            } catch (ClientException ex) {
                // expected to fail
            }

            connection.close().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToCompleteIgnoreErrors();
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
    public void testSendMessagesInsideOfUniqueTransactions() throws Exception {
        final byte[] txnId1 = new byte[] { 0, 1, 2, 3 };
        final byte[] txnId2 = new byte[] { 1, 1, 2, 3 };
        final byte[] txnId3 = new byte[] { 2, 1, 2, 3 };
        final byte[] txnId4 = new byte[] { 3, 1, 2, 3 };

        final byte[][] txns = new byte[][] { txnId1, txnId2, txnId3, txnId4 };

        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.SENDER).respond();
            peer.remoteFlow().withLinkCredit(txns.length).queue();
            peer.expectCoordinatorAttach().respond();
            peer.remoteFlow().withLinkCredit(txns.length * 2).queue();
            for (int i = 0; i < txns.length; ++i) {
                peer.expectDeclare().accept(txns[i]);
                peer.expectTransfer().withHandle(0)
                                     .withPayload(notNullValue(ProtonBuffer.class))
                                     .withState(new TransactionalState().setTxnId(new Binary(txns[i])))
                                     .respond()
                                     .withState(new TransactionalState().setTxnId(new Binary(txns[i])).setOutcome(Accepted.getInstance()))
                                     .withSettled(true);
                peer.expectDischarge().withFail(false).withTxnId(txns[i]).accept();
            }
            peer.expectEnd().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Sender test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession().openFuture().get();
            Sender sender = session.openSender("address").openFuture().get();

            for (int i = 0; i < txns.length; ++i) {
                session.begin();

                final Tracker tracker = sender.send(Message.create("test-message-" + i));

                assertNotNull(tracker);
                assertNotNull(tracker.acknowledgeFuture().get());
                assertEquals(tracker.remoteState().getType(), DeliveryState.Type.TRANSACTIONAL);
                assertNotNull(tracker.state());
                assertEquals(tracker.state().getType(), DeliveryState.Type.TRANSACTIONAL);
                assertTrue(tracker.settled());

                session.commit();
            }

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
    public void testReceiveMessageInsideOfTransactionNoAutoSettleSenderSettles() throws Exception {
        doTestReceiveMessageInsideOfTransactionNoAutoSettle(true);
    }

    @Test(timeout = 20_000)
    public void testReceiveMessageInsideOfTransactionNoAutoSettleSenderDoesNotSettle() throws Exception {
        doTestReceiveMessageInsideOfTransactionNoAutoSettle(false);
    }

    private void doTestReceiveMessageInsideOfTransactionNoAutoSettle(boolean settle) throws Exception {
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
            ReceiverOptions options = new ReceiverOptions().autoAccept(false).autoSettle(false);
            Receiver receiver = session.openReceiver("test-queue", options).openFuture().get();

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
            assertFalse(delivery.settled());
            assertNull(delivery.state());

            Message<?> received = delivery.message();
            assertNotNull(received);
            assertTrue(received.body() instanceof String);
            String value = (String) received.body();
            assertEquals("Hello World", value);

            // Manual Accept within the transaction, settlement is ignored.
            delivery.disposition(DeliveryState.accepted(), settle);

            session.commit();
            receiver.close();
            connection.close().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 20_000)
    public void testReceiveMessageInsideOfTransactionButAcceptAndSettleOutside() throws Exception {
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
            ReceiverOptions options = new ReceiverOptions().autoAccept(false).autoSettle(false);
            Receiver receiver = session.openReceiver("test-queue", options).openFuture().get();

            peer.expectCoordinatorAttach().respond();
            peer.remoteFlow().withLinkCredit(2).queue();
            peer.expectDeclare().accept(txnId);
            peer.remoteTransfer().withHandle(0)
                                 .withDeliveryId(0)
                                 .withDeliveryTag(new byte[] { 1 })
                                 .withMore(false)
                                 .withMessageFormat(0)
                                 .withPayload(payload).queue();
            peer.expectDischarge().withFail(false).withTxnId(txnId).accept();
            peer.expectDisposition().withSettled(true).withState(Accepted.getInstance());

            session.begin();

            Delivery delivery = receiver.receive(100);
            assertNotNull(delivery);
            assertFalse(delivery.settled());
            assertNull(delivery.state());

            Message<?> received = delivery.message();
            assertNotNull(received);
            assertTrue(received.body() instanceof String);
            String value = (String) received.body();
            assertEquals("Hello World", value);

            session.commit();

            // Manual Accept outside the transaction and no auto settle or accept
            // so no transactional enlistment.
            delivery.disposition(DeliveryState.accepted(), true);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectDetach().respond();
            peer.expectClose().respond();

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
