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
package org.apache.qpid.proton4j.engine.impl;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.driver.ProtonTestPeer;
import org.apache.qpid.proton4j.amqp.messaging.Accepted;
import org.apache.qpid.proton4j.amqp.messaging.Modified;
import org.apache.qpid.proton4j.amqp.messaging.Rejected;
import org.apache.qpid.proton4j.amqp.messaging.Released;
import org.apache.qpid.proton4j.amqp.messaging.Source;
import org.apache.qpid.proton4j.amqp.messaging.Target;
import org.apache.qpid.proton4j.amqp.transactions.TransactionalState;
import org.apache.qpid.proton4j.amqp.transport.AmqpError;
import org.apache.qpid.proton4j.amqp.transport.DeliveryState;
import org.apache.qpid.proton4j.amqp.transport.ErrorCondition;
import org.apache.qpid.proton4j.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton4j.amqp.transport.Role;
import org.apache.qpid.proton4j.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.engine.Connection;
import org.apache.qpid.proton4j.engine.Engine;
import org.apache.qpid.proton4j.engine.EngineFactory;
import org.apache.qpid.proton4j.engine.OutgoingDelivery;
import org.apache.qpid.proton4j.engine.Sender;
import org.apache.qpid.proton4j.engine.Session;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Test the {@link ProtonSender}
 */
public class ProtonSenderTest extends ProtonEngineTestSupport {

    @Test
    public void testSenderEmitsOpenAndCloseEvents() throws Exception {
        doTestSenderEmitsEvents(false);
    }

    @Test
    public void testSenderEmitsOpenAndDetachEvents() throws Exception {
        doTestSenderEmitsEvents(true);
    }

    private void doTestSenderEmitsEvents(boolean detach) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        final AtomicBoolean senderLocalOpen = new AtomicBoolean();
        final AtomicBoolean senderLocalClose = new AtomicBoolean();
        final AtomicBoolean senderLocalDetach = new AtomicBoolean();
        final AtomicBoolean senderRemoteOpen = new AtomicBoolean();
        final AtomicBoolean senderRemoteClose = new AtomicBoolean();
        final AtomicBoolean senderRemoteDetach = new AtomicBoolean();

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.expectDetach().respond();
        peer.expectEnd().respond();

        Connection connection = engine.start();

        connection.open();
        Session session = connection.session();
        session.open();

        Sender sender = session.sender("test");
        sender.localOpenHandler(result -> senderLocalOpen.set(true))
              .localCloseHandler(result -> senderLocalClose.set(true))
              .localDetachHandler(result -> senderLocalDetach.set(true))
              .openHandler(result -> senderRemoteOpen.set(true))
              .detachHandler(result -> senderRemoteDetach.set(true))
              .closeHandler(result -> senderRemoteClose.set(true));

        sender.open();

        if (detach) {
            sender.detach();
        } else {
            sender.close();
        }

        assertTrue("Sender should have reported local open", senderLocalOpen.get());
        assertTrue("Sender should have reported remote open", senderRemoteOpen.get());

        if (detach) {
            assertFalse("Sender should not have reported local close", senderLocalClose.get());
            assertTrue("Sender should have reported local detach", senderLocalDetach.get());
            assertFalse("Sender should not have reported remote close", senderRemoteClose.get());
            assertTrue("Sender should have reported remote close", senderRemoteDetach.get());
        } else {
            assertTrue("Sender should have reported local close", senderLocalClose.get());
            assertFalse("Sender should not have reported local detach", senderLocalDetach.get());
            assertTrue("Sender should have reported remote close", senderRemoteClose.get());
            assertFalse("Sender should not have reported remote close", senderRemoteDetach.get());
        }

        session.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSenderOpenWithNoSenderOrReceiverSettleModes() throws Exception {
        doTestOpenSenderWithConfiguredSenderAndReceiverSettlementModes(null, null);
    }

    @Test
    public void testSenderOpenWithSettledAndFirst() throws Exception {
        doTestOpenSenderWithConfiguredSenderAndReceiverSettlementModes(SenderSettleMode.SETTLED, ReceiverSettleMode.FIRST);
    }

    @Test
    public void testSenderOpenWithUnsettledAndSecond() throws Exception {
        doTestOpenSenderWithConfiguredSenderAndReceiverSettlementModes(SenderSettleMode.UNSETTLED, ReceiverSettleMode.SECOND);
    }

    private void doTestOpenSenderWithConfiguredSenderAndReceiverSettlementModes(SenderSettleMode senderMode, ReceiverSettleMode receiverMode) {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().withSndSettleMode(senderMode)
                           .withRcvSettleMode(receiverMode)
                           .respond()
                           .withSndSettleMode(senderMode)
                           .withRcvSettleMode(receiverMode);

        Connection connection = engine.start();

        connection.open();
        Session session = connection.session();
        session.open();

        Sender sender = session.sender("sender");
        sender.setSenderSettleMode(senderMode);
        sender.setReceiverSettleMode(receiverMode);
        sender.open();

        peer.waitForScriptToComplete();
        peer.expectDetach().respond();

        if (senderMode != null) {
            assertEquals(senderMode, sender.getSenderSettleMode());
        } else {
            assertEquals(SenderSettleMode.MIXED, sender.getSenderSettleMode());
        }
        if (receiverMode != null) {
            assertEquals(receiverMode, sender.getReceiverSettleMode());
        } else {
            assertEquals(ReceiverSettleMode.FIRST, sender.getReceiverSettleMode());
        }

        sender.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSenderOpenAndCloseAreIdempotent() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().withRole(Role.SENDER).respond();
        peer.expectDetach().respond();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Sender sender = session.sender("test");
        sender.open();

        // Should not emit another attach frame
        sender.open();

        sender.close();

        // Should not emit another detach frame
        sender.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test(timeout = 60000)
    public void testCreateSenderAndClose() throws Exception {
        doTestCreateSenderAndCloseOrDetachLink(true);
    }

    @Test(timeout = 60000)
    public void testCreateSenderAndDetach() throws Exception {
        doTestCreateSenderAndCloseOrDetachLink(false);
    }

    private void doTestCreateSenderAndCloseOrDetachLink(boolean close) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.expectAttach().withRole(Role.SENDER).respond();
        peer.expectDetach().withClosed(close).respond();
        peer.expectClose().respond();

        Connection connection = engine.start();
        connection.open();
        Session session = connection.session();
        session.open();

        Sender sender = session.sender("test");
        sender.open();

        if (close) {
            sender.close();
        } else {
            sender.detach();
        }

        connection.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testEngineEmitsAttachAfterLocalSenderOpened() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.expectDetach().respond();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Sender sender = session.sender("test");
        sender.open();
        sender.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testOpenBeginAttachBeforeRemoteResponds() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen();
        peer.expectBegin();
        peer.expectAttach();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Sender sender = session.sender("test");
        sender.open();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSenderFireOpenedEventAfterRemoteAttachArrives() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.expectDetach().respond();

        final AtomicBoolean senderRemotelyOpened = new AtomicBoolean();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Sender sender = session.sender("test");
        sender.openHandler(result -> {
            senderRemotelyOpened.set(true);
        });
        sender.open();

        assertTrue("Sender remote opened event did not fire", senderRemotelyOpened.get());

        sender.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSenderFireOpenedEventAfterRemoteAttachArrivesWithNullTarget() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond().withTarget((Target) null);
        peer.expectDetach().respond();

        final AtomicBoolean senderRemotelyOpened = new AtomicBoolean();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Sender sender = session.sender("test");
        sender.setSource(new Source());
        sender.setTarget(new Target());
        sender.openHandler(result -> {
            senderRemotelyOpened.set(true);
        });
        sender.open();

        assertTrue("Sender remote opened event did not fire", senderRemotelyOpened.get());
        assertNull(sender.getRemoteTarget());

        sender.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testOpenAndCloseMultipleSenders() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().withHandle(0).respond();
        peer.expectAttach().withHandle(1).respond();
        peer.expectDetach().withHandle(1).respond();
        peer.expectDetach().withHandle(0).respond();

        Connection connection = engine.start();

        connection.open();
        Session session = connection.session();
        session.open();

        Sender sender1 = session.sender("sender-1");
        sender1.open();
        Sender sender2 = session.sender("sender-2");
        sender2.open();

        // Close in reverse order
        sender2.close();
        sender1.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSenderFireClosedEventAfterRemoteDetachArrives() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.expectDetach().respond();

        final AtomicBoolean senderRemotelyOpened = new AtomicBoolean();
        final AtomicBoolean senderRemotelyClosed = new AtomicBoolean();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Sender sender = session.sender("test");
        sender.openHandler(result -> {
            senderRemotelyOpened.set(true);
        });
        sender.closeHandler(result -> {
            senderRemotelyClosed.set(true);
        });
        sender.open();

        assertTrue("Sender remote opened event did not fire", senderRemotelyOpened.get());

        sender.close();

        assertTrue("Sender remote closed event did not fire", senderRemotelyClosed.get());

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSenderFireClosedEventAfterRemoteDetachArrivesBeforeLocalClose() throws Exception {
        doTestSenderFireEventAfterRemoteDetachArrivesBeforeLocalClose(true);
    }

    @Test
    public void testSenderFireDetachEventAfterRemoteDetachArrivesBeforeLocalClose() throws Exception {
        doTestSenderFireEventAfterRemoteDetachArrivesBeforeLocalClose(false);
    }

    private void doTestSenderFireEventAfterRemoteDetachArrivesBeforeLocalClose(boolean close) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.remoteDetach().withClosed(close).queue();

        final AtomicBoolean senderRemotelyOpened = new AtomicBoolean();
        final AtomicBoolean senderRemotelyClosed = new AtomicBoolean();
        final AtomicBoolean senderRemotelyDetached = new AtomicBoolean();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Sender sender = session.sender("test");
        sender.openHandler(result -> senderRemotelyOpened.set(true));
        sender.closeHandler(result -> senderRemotelyClosed.set(true));
        sender.detachHandler(result -> senderRemotelyDetached.set(true));
        sender.open();

        peer.waitForScriptToComplete();

        assertTrue("Sender remote opened event did not fire", senderRemotelyOpened.get());
        if (close) {
            assertTrue("Sender remote closed event did not fire", senderRemotelyClosed.get());
            assertFalse("Sender remote detached event fired", senderRemotelyDetached.get());
        } else {
            assertFalse("Sender remote closed event fired", senderRemotelyClosed.get());
            assertTrue("Sender remote closed event did not fire", senderRemotelyDetached.get());
        }

        peer.expectDetach().withClosed(close);
        if (close) {
            sender.close();
        } else {
            sender.detach();
        }

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testRemotelyCloseSenderAndOpenNewSenderImmediatelyAfterWithNewLinkName() throws Exception {
        doTestRemotelyTerminateLinkAndThenCreateNewLink(true, false);
    }

    @Test
    public void testRemotelyDetachSenderAndOpenNewSenderImmediatelyAfterWithNewLinkName() throws Exception {
        doTestRemotelyTerminateLinkAndThenCreateNewLink(false, false);
    }

    @Ignore("Issue with untracking links by name currently")  // TODO - No Free in current API when to stop tracking ?
    @Test
    public void testRemotelyCloseSenderAndOpenNewSenderImmediatelyAfterWithSameLinkName() throws Exception {
        doTestRemotelyTerminateLinkAndThenCreateNewLink(true, true);
    }

    @Ignore("Issue with untracking links by name currently")  // TODO - No Free in current API when to stop tracking ?
    @Test
    public void testRemotelyDetachSenderAndOpenNewSenderImmediatelyAfterWithSameLinkName() throws Exception {
        doTestRemotelyTerminateLinkAndThenCreateNewLink(false, true);
    }

    private void doTestRemotelyTerminateLinkAndThenCreateNewLink(boolean close, boolean sameLinkName) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        String firstLinkName = "test-link-1";
        String secondLinkName = sameLinkName ? firstLinkName : "test-link-2";

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().withHandle(0).withRole(Role.SENDER).respond();
        peer.remoteDetach().withClosed(close).queue();

        final AtomicBoolean senderRemotelyOpened = new AtomicBoolean();
        final AtomicBoolean senderRemotelyClosed = new AtomicBoolean();
        final AtomicBoolean senderRemotelyDetached = new AtomicBoolean();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Sender sender = session.sender(firstLinkName);
        sender.openHandler(result -> senderRemotelyOpened.set(true));
        sender.closeHandler(result -> senderRemotelyClosed.set(true));
        sender.detachHandler(result -> senderRemotelyDetached.set(true));
        sender.open();

        peer.waitForScriptToComplete();

        assertTrue("Sender remote opened event did not fire", senderRemotelyOpened.get());
        if (close) {
            assertTrue("Sender remote closed event did not fire", senderRemotelyClosed.get());
            assertFalse("Sender remote detached event fired", senderRemotelyDetached.get());
        } else {
            assertFalse("Sender remote closed event fired", senderRemotelyClosed.get());
            assertTrue("Sender remote closed event did not fire", senderRemotelyDetached.get());
        }

        peer.expectDetach().withClosed(close);
        if (close) {
            sender.close();
        } else {
            sender.detach();
        }

        peer.waitForScriptToComplete();
        peer.expectAttach().withHandle(0).withRole(Role.SENDER).respond();
        peer.expectDetach().withClosed(close).respond();

        // Reset trackers
        senderRemotelyOpened.set(false);
        senderRemotelyClosed.set(false);
        senderRemotelyDetached.set(false);

        sender = session.sender(secondLinkName);
        sender.openHandler(result -> senderRemotelyOpened.set(true));
        sender.closeHandler(result -> senderRemotelyClosed.set(true));
        sender.detachHandler(result -> senderRemotelyDetached.set(true));
        sender.open();

        if (close) {
            sender.close();
        } else {
            sender.detach();
        }

        peer.waitForScriptToComplete();

        assertTrue("Sender remote opened event did not fire", senderRemotelyOpened.get());
        if (close) {
            assertTrue("Sender remote closed event did not fire", senderRemotelyClosed.get());
            assertFalse("Sender remote detached event fired", senderRemotelyDetached.get());
        } else {
            assertFalse("Sender remote closed event fired", senderRemotelyClosed.get());
            assertTrue("Sender remote closed event did not fire", senderRemotelyDetached.get());
        }

        assertNull(failure);
    }

    @Test
    public void testConnectionSignalsRemoteSenderOpen() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.remoteAttach().withName("receiver")
                           .withHandle(0)
                           .withRole(Role.RECEIVER)
                           .withInitialDeliveryCount(0)
                           .onChannel(0).queue();
        peer.expectAttach();
        peer.expectDetach().respond();

        final AtomicBoolean senderRemotelyOpened = new AtomicBoolean();
        final AtomicReference<Sender> sender = new AtomicReference<>();

        Connection connection = engine.start();

        connection.senderOpenHandler(result -> {
            senderRemotelyOpened.set(true);
            sender.set(result);
        });

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();

        assertTrue("Sender remote opened event did not fire", senderRemotelyOpened.get());

        sender.get().open();
        sender.get().close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testCannotOpenSenderAfterSessionClosed() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectEnd().respond();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();

        Sender sender = session.sender("test");

        session.close();

        try {
            sender.open();
            fail("Should not be able to open a link from a closed session.");
        } catch (IllegalStateException ise) {}

        sender.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testCannotOpenSenderAfterSessionRemotelyClosed() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.remoteEnd().queue();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        Sender sender = session.sender("test");
        session.open();

        try {
            sender.open();
            fail("Should not be able to open a link from a remotely closed session.");
        } catch (IllegalStateException ise) {}

        sender.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testGetCurrentDeliveryFromSender() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().withHandle(0).respond();
        peer.expectDetach().withHandle(0).respond();

        Connection connection = engine.start();

        connection.open();
        Session session = connection.session();
        session.open();

        Sender sender = session.sender("sender-1");

        sender.open();

        OutgoingDelivery delivery = sender.next();
        assertNotNull(delivery);

        assertFalse(delivery.isAborted());
        assertTrue(delivery.isPartial());
        assertFalse(delivery.isSettled());
        assertFalse(delivery.isRemotelySettled());

        // Always return same delivery until completed.
        assertSame(delivery, sender.current());

        sender.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSenderGetsCreditOnIncomingFlow() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().withRole(Role.SENDER).respond();
        peer.remoteFlow().withDeliveryCount(0)
                         .withLinkCredit(10)
                         .withIncomingWindow(1024)
                         .withOutgoingWindow(10)
                         .withNextIncomingId(0)
                         .withNextOutgoingId(1).queue();
        peer.expectDetach().withHandle(0).respond();

        Connection connection = engine.start();

        connection.open();
        Session session = connection.session();
        session.open();

        Sender sender = session.sender("sender-1");

        assertFalse(sender.isSendable());

        sender.open();

        assertEquals(10, sender.getCredit());
        assertTrue(sender.isSendable());

        sender.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSendSmallPayloadWhenCreditAvailable() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        final byte [] payloadBuffer = new byte[] {0, 1, 2, 3, 4};

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().withRole(Role.SENDER).respond();
        peer.remoteFlow().withDeliveryCount(0)     // TODO - Would be nice to automate filling in these
                         .withLinkCredit(10)       //        these bits using last session opened values
                         .withIncomingWindow(1024) //        plus some defaults or generated values.
                         .withOutgoingWindow(10)
                         .withNextIncomingId(0)
                         .withNextOutgoingId(1).queue();
        peer.expectTransfer().withHandle(0)
                             .withSettled(false)
                             .withState((DeliveryState) null)
                             .withDeliveryId(0)
                             .withDeliveryTag(new byte[] {0})
                             .withPayload(payloadBuffer);
        peer.expectDetach().withHandle(0).respond();

        Connection connection = engine.start();

        connection.open();
        Session session = connection.session();
        session.open();

        ProtonBuffer payload = ProtonByteBufferAllocator.DEFAULT.wrap(payloadBuffer);

        Sender sender = session.sender("sender-1");

        assertFalse(sender.isSendable());

        sender.sendableHandler(handler -> {
            handler.next().setTag(new byte[] {0}).writeBytes(payload);
        });

        sender.open();
        sender.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSenderSignalsDeliveryUpdatedOnSettled() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        ProtonBuffer payload = ProtonByteBufferAllocator.DEFAULT.wrap(new byte[] {0, 1, 2, 3, 4});

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().withRole(Role.SENDER).respond();
        peer.remoteFlow().withDeliveryCount(0)
                         .withLinkCredit(10)
                         .withIncomingWindow(1024)
                         .withOutgoingWindow(10)
                         .withNextIncomingId(0)
                         .withNextOutgoingId(1).queue();
        peer.expectTransfer().withHandle(0)
                             .withSettled(false)
                             .withState((DeliveryState) null)
                             .withDeliveryId(0)
                             .withDeliveryTag(new byte[] {0})
                             .withPayload(payload.copy());
        peer.remoteDisposition().withSettled(true)
                                .withRole(Role.RECEIVER)
                                .withState(Accepted.getInstance())
                                .withFirst(0).queue();
        peer.expectDetach().withHandle(0).respond();

        Connection connection = engine.start();

        connection.open();
        Session session = connection.session();
        session.open();

        Sender sender = session.sender("sender-1");

        final AtomicBoolean deliveryUpdatedAndSettled = new AtomicBoolean();
        final AtomicReference<OutgoingDelivery> updatedDelivery = new AtomicReference<>();
        sender.deliveryUpdatedHandler(delivery -> {
            if (delivery.isRemotelySettled()) {
                deliveryUpdatedAndSettled.set(true);
            }

            updatedDelivery.set(delivery);
        });

        assertFalse(sender.isSendable());

        sender.sendableHandler(handler -> {
            handler.next().setTag(new byte[] {0}).writeBytes(payload);
        });

        sender.open();

        assertTrue("Delivery should have been updated and state settled", deliveryUpdatedAndSettled.get());
        assertEquals(Accepted.getInstance(), updatedDelivery.get().getRemoteState());

        sender.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testOpenSenderBeforeOpenConnection() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        // Create the connection but don't open, then open a session and a sender and
        // the session begin and sender attach shouldn't go out until the connection
        // is opened locally.
        Connection connection = engine.start();
        Session session = connection.session();
        session.open();
        Sender sender = session.sender("sender");
        sender.open();

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.expectAttach().withHandle(0).withName("sender").withRole(Role.SENDER).respond();

        // Now open the connection, expect the Open, Begin, and Attach frames
        connection.open();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testOpenSenderBeforeOpenSession() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();

        // Create the connection and open it, then create a session and a sender
        // and observe that the sender doesn't send its attach until the session
        // is opened.
        Connection connection = engine.start();
        connection.open();
        Session session = connection.session();
        Sender sender = session.sender("sender");
        sender.open();

        peer.expectBegin().respond();
        peer.expectAttach().withHandle(0).withName("sender").withRole(Role.SENDER).respond();

        // Now open the session, expect the Begin, and Attach frames
        session.open();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSenderDetachAfterEndSent() {
        doTestSenderClosedOrDetachedAfterEndSent(false);
    }

    @Test
    public void testSenderCloseAfterEndSent() {
        doTestSenderClosedOrDetachedAfterEndSent(true);
    }

    public void doTestSenderClosedOrDetachedAfterEndSent(boolean close) {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.expectAttach().withHandle(0).withName("sender").withRole(Role.SENDER).respond();
        peer.expectEnd().respond();

        // Create the connection and open it, then create a session and a sender
        // and observe that the sender doesn't send its detach if the session has
        // already been closed.
        Connection connection = engine.start();
        connection.open();
        Session session = connection.session();
        session.open();
        Sender sender = session.sender("sender");
        sender.open();

        // Causes the End frame to be sent
        session.close();

        // The sender should not emit an end as the session was closed which implicitly
        // detached the link.
        if (close) {
            sender.close();
        } else {
            sender.detach();
        }

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSenderDetachAfterCloseSent() {
        doTestSenderClosedOrDetachedAfterCloseSent(false);
    }

    @Test
    public void testSenderCloseAfterCloseSent() {
        doTestSenderClosedOrDetachedAfterCloseSent(true);
    }

    public void doTestSenderClosedOrDetachedAfterCloseSent(boolean close) {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.expectAttach().withHandle(0).withName("sender").withRole(Role.SENDER).respond();
        peer.expectClose().respond();

        // Create the connection and open it, then create a session and a sender
        // and observe that the sender doesn't send its detach if the connection has
        // already been closed.
        Connection connection = engine.start();
        connection.open();
        Session session = connection.session();
        session.open();
        Sender sender = session.sender("sender");
        sender.open();

        // Cause an Close frame to be sent
        connection.close();

        // The sender should not emit an detach as the connection was closed which implicitly
        // detached the link.
        if (close) {
            sender.close();
        } else {
            sender.detach();
        }

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testNoDispositionSentAfterDeliverySettledForSender() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().withRole(Role.SENDER).respond();
        peer.remoteFlow().withDeliveryCount(0)
                         .withLinkCredit(10)
                         .withIncomingWindow(1024)
                         .withOutgoingWindow(10)
                         .withNextIncomingId(0)
                         .withNextOutgoingId(1).queue();
        peer.expectTransfer().withHandle(0)
                             .withSettled(false)
                             .withState((DeliveryState) null)
                             .withDeliveryId(0)
                             .withDeliveryTag(new byte[] {0});
        peer.expectDisposition().withFirst(0)
                                .withSettled(true)
                                .withState(Accepted.getInstance());
        peer.expectDetach().withHandle(0).respond();

        Connection connection = engine.start();

        connection.open();
        Session session = connection.session();
        session.open();

        ProtonBuffer payload = ProtonByteBufferAllocator.DEFAULT.wrap(new byte[] {0, 1, 2, 3, 4});

        Sender sender = session.sender("sender-1");

        final AtomicBoolean deliverySentAfterSenable = new AtomicBoolean();
        sender.sendableHandler(handler -> {
            handler.next().setTag(new byte[] {0}).writeBytes(payload);
            deliverySentAfterSenable.set(true);
        });

        sender.open();

        assertTrue("Delivery should have been sent after credit arrived", deliverySentAfterSenable.get());

        OutgoingDelivery delivery1 = sender.current();
        delivery1.disposition(Accepted.getInstance(), true);
        OutgoingDelivery delivery2 = sender.current();
        OutgoingDelivery delivery3 = sender.next();
        assertSame(delivery1, delivery2);
        assertNotSame(delivery2, delivery3);
        delivery3.disposition(Released.getInstance(), true);

        sender.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSenderCannotSendAfterConnectionClosed() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().withRole(Role.SENDER).respond();
        peer.remoteFlow().withDeliveryCount(0)
                         .withLinkCredit(10)
                         .withIncomingWindow(1024)
                         .withOutgoingWindow(10)
                         .withNextIncomingId(0)
                         .withNextOutgoingId(1).queue();
        peer.expectClose().respond();

        Connection connection = engine.start();

        connection.open();
        Session session = connection.session();
        session.open();

        Sender sender = session.sender("sender-1");

        assertFalse(sender.isSendable());

        OutgoingDelivery delivery = sender.next();
        assertNotNull(delivery);

        sender.open();

        assertEquals(10, sender.getCredit());
        assertTrue(sender.isSendable());

        connection.close();

        assertFalse(sender.isSendable());
        try {
            delivery.writeBytes(ProtonByteBufferAllocator.DEFAULT.wrap(new byte[] { 1 }));
            fail("Should not be able to write to delivery after connection closed.");
        } catch (IllegalStateException ise) {
            // Should not allow writes on past delivery instances after connection closed
        }

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSenderCannotSendAfterSessionClosed() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().withRole(Role.SENDER).respond();
        peer.remoteFlow().withDeliveryCount(0)
                         .withLinkCredit(10)
                         .withIncomingWindow(1024)
                         .withOutgoingWindow(10)
                         .withNextIncomingId(0)
                         .withNextOutgoingId(1).queue();
        peer.expectEnd().respond();
        peer.expectClose().respond();

        Connection connection = engine.start();

        connection.open();
        Session session = connection.session();
        session.open();

        Sender sender = session.sender("sender-1");

        assertFalse(sender.isSendable());

        OutgoingDelivery delivery = sender.next();
        assertNotNull(delivery);

        sender.open();

        assertEquals(10, sender.getCredit());
        assertTrue(sender.isSendable());

        session.close();

        assertFalse(sender.isSendable());
        try {
            delivery.writeBytes(ProtonByteBufferAllocator.DEFAULT.wrap(new byte[] { 1 }));
            fail("Should not be able to write to delivery after session closed.");
        } catch (IllegalStateException ise) {
            // Should not allow writes on past delivery instances after session closed
        }

        connection.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSendMultiFrameDeliveryAndSingleFrameDeliveryOnSingleSessionFromDifferentSenders() {
        doMultiplexMultiFrameDeliveryOnSingleSessionOutgoingTestImpl(false);
    }

    @Test
    public void testMultipleMultiFrameDeliveriesOnSingleSessionFromDifferentSenders() {
        doMultiplexMultiFrameDeliveryOnSingleSessionOutgoingTestImpl(true);
    }

    private void doMultiplexMultiFrameDeliveryOnSingleSessionOutgoingTestImpl(boolean bothDeliveriesMultiFrame) {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        int contentLength1 = 6000;
        int frameSizeLimit = 4000;
        int contentLength2 = 2000;
        if (bothDeliveriesMultiFrame) {
            contentLength2 = 6000;
        }

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().withMaxFrameSize(frameSizeLimit).respond().withContainerId("driver").withMaxFrameSize(frameSizeLimit);
        peer.expectBegin().respond();
        peer.expectAttach().withRole(Role.SENDER).respond();
        peer.expectAttach().withRole(Role.SENDER).respond();

        Connection connection = engine.start();
        connection.setMaxFrameSize(frameSizeLimit);
        connection.open();
        Session session = connection.session();
        session.open();

        String linkName1 = "Sender1";
        Sender sender1 = session.sender(linkName1);
        sender1.open();

        String linkName2 = "Sender2";
        Sender sender2 = session.sender(linkName2);
        sender2.open();

        final AtomicBoolean sender1MarkedSendable = new AtomicBoolean();
        sender1.sendableHandler(handler -> {
            sender1MarkedSendable.set(true);
        });

        final AtomicBoolean sender2MarkedSendable = new AtomicBoolean();
        sender2.sendableHandler(handler -> {
            sender2MarkedSendable.set(true);
        });

        peer.remoteFlow().withHandle(0)
                         .withDeliveryCount(0)
                         .withLinkCredit(10)
                         .withIncomingWindow(1024)
                         .withOutgoingWindow(10)
                         .withNextIncomingId(0)
                         .withNextOutgoingId(1).now();
        peer.remoteFlow().withHandle(1)
                         .withDeliveryCount(0)
                         .withLinkCredit(10)
                         .withIncomingWindow(1024)
                         .withOutgoingWindow(10)
                         .withNextIncomingId(0)
                         .withNextOutgoingId(1).now();

        assertTrue("Sender 1 should now be sendable", sender1MarkedSendable.get());
        assertTrue("Sender 2 should now be sendable", sender2MarkedSendable.get());

        // Frames are not multiplexed for large deliveries as we write the full
        // writable portion out when a write is called.

        peer.expectTransfer().withHandle(0)
                             .withSettled(true)
                             .withState(Accepted.getInstance())
                             .withDeliveryId(0)
                             .withMore(true)
                             .withDeliveryTag(new byte[] {1});
        peer.expectTransfer().withHandle(0)
                             .withSettled(true)
                             .withState(Accepted.getInstance())
                             .withDeliveryId(0)
                             .withMore(false)
                             .withDeliveryTag(new byte[] {1});
        peer.expectTransfer().withHandle(1)
                             .withSettled(true)
                             .withState(Accepted.getInstance())
                             .withDeliveryId(1)
                             .withMore(bothDeliveriesMultiFrame)
                             .withDeliveryTag(new byte[] {2});
        if (bothDeliveriesMultiFrame) {
            peer.expectTransfer().withHandle(1)
                                 .withSettled(true)
                                 .withState(Accepted.getInstance())
                                 .withDeliveryId(1)
                                 .withMore(false)
                                 .withDeliveryTag(new byte[] {2});
        }

        ProtonBuffer messageContent1 = createContentBuffer(contentLength1);
        OutgoingDelivery delivery1 = sender1.next();
        delivery1.setTag(new byte[] { 1 });
        delivery1.disposition(Accepted.getInstance(), true);
        delivery1.writeBytes(messageContent1);

        ProtonBuffer messageContent2 = createContentBuffer(contentLength2);
        OutgoingDelivery delivery2 = sender2.next();
        delivery2.setTag(new byte[] { 2 });
        delivery2.disposition(Accepted.getInstance(), true);
        delivery2.writeBytes(messageContent2);

        peer.expectClose().respond();
        connection.close();

        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test
    public void testMaxFrameSizeOfPeerHasEffect() {
        doMaxFrameSizeTestImpl(0, 0, 5700, 1);
        doMaxFrameSizeTestImpl(1024, 0, 5700, 6);
    }

    @Test
    public void testMaxFrameSizeOutgoingFrameSizeLimitHasEffect() {
        doMaxFrameSizeTestImpl(0, 512, 5700, 12);
        doMaxFrameSizeTestImpl(1024, 512, 5700, 12);
        doMaxFrameSizeTestImpl(1024, 2048, 5700, 6);
    }

    void doMaxFrameSizeTestImpl(int remoteMaxFrameSize, int outboundFrameSizeLimit, int contentLength, int expectedNumFrames) {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        if (outboundFrameSizeLimit == 0) {
            if (remoteMaxFrameSize == 0) {
                peer.expectOpen().respond();
            } else {
                peer.expectOpen().respond().withMaxFrameSize(remoteMaxFrameSize);
            }
        } else {
            if (remoteMaxFrameSize == 0) {
                peer.expectOpen().withMaxFrameSize(outboundFrameSizeLimit).respond();
            } else {
                peer.expectOpen().withMaxFrameSize(outboundFrameSizeLimit)
                                 .respond()
                                 .withMaxFrameSize(remoteMaxFrameSize);
            }
        }
        peer.expectBegin().respond();
        peer.expectAttach().withRole(Role.SENDER).respond();

        Connection connection = engine.start();
        if (outboundFrameSizeLimit != 0) {
            connection.setMaxFrameSize(outboundFrameSizeLimit);
        }
        connection.open();
        Session session = connection.session();
        session.open();

        String linkName = "mySender";
        Sender sender = session.sender(linkName);
        sender.open();

        final AtomicBoolean senderMarkedSendable = new AtomicBoolean();
        sender.sendableHandler(handler -> {
            senderMarkedSendable.set(true);
        });

        peer.remoteFlow().withHandle(0)
                         .withDeliveryCount(0)
                         .withLinkCredit(50)
                         .withIncomingWindow(65535)
                         .withOutgoingWindow(65535)
                         .withNextIncomingId(0)
                         .withNextOutgoingId(1).now();

        assertTrue("Sender should now be sendable", senderMarkedSendable.get());

        // This calculation isn't entirely precise, there is some added performative/frame overhead not
        // accounted for...but values are chosen to work, and verified here.
        final int frameCount;
        if(remoteMaxFrameSize == 0 && outboundFrameSizeLimit == 0) {
            frameCount = 1;
        } else if(remoteMaxFrameSize == 0 && outboundFrameSizeLimit != 0) {
            frameCount = (int) Math.ceil((double)contentLength / (double) outboundFrameSizeLimit);
        } else {
            int effectiveMaxFrameSize;
            if(outboundFrameSizeLimit != 0) {
                effectiveMaxFrameSize = Math.min(outboundFrameSizeLimit, remoteMaxFrameSize);
            } else {
                effectiveMaxFrameSize = remoteMaxFrameSize;
            }

            frameCount = (int) Math.ceil((double)contentLength / (double) effectiveMaxFrameSize);
        }

        assertEquals("Unexpected number of frames calculated", expectedNumFrames, frameCount);

        for (int i = 1; i <= expectedNumFrames; ++i) {
            peer.expectTransfer().withHandle(0)
                                 .withSettled(true)
                                 .withState(Accepted.getInstance())
                                 .withDeliveryId(0)
                                 .withMore(i != expectedNumFrames ? true : false)
                                 .withDeliveryTag(notNullValue())
                                 .withPayload(notNullValue(ProtonBuffer.class));
        }

        ProtonBuffer messageContent = createContentBuffer(contentLength);
        OutgoingDelivery delivery = sender.next();
        delivery.setTag(new byte[] { 1 });
        delivery.disposition(Accepted.getInstance(), true);
        delivery.writeBytes(messageContent);

        peer.expectClose().respond();
        connection.close();

        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test
    public void testAbortInProgressDelivery() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        ProtonBuffer payload = ProtonByteBufferAllocator.DEFAULT.wrap(new byte[] {0, 1, 2, 3, 4});

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().withRole(Role.SENDER).respond();
        peer.remoteFlow().withDeliveryCount(0)
                         .withLinkCredit(10)
                         .withIncomingWindow(1024)
                         .withOutgoingWindow(10)
                         .withNextIncomingId(0)
                         .withNextOutgoingId(1).queue();
        peer.expectTransfer().withHandle(0)
                             .withMore(true)
                             .withSettled(false)
                             .withState((DeliveryState) null)
                             .withDeliveryId(0)
                             .withDeliveryTag(new byte[] {0})
                             .withPayload(payload.copy());
        peer.expectTransfer().withHandle(0)
                             .withState((DeliveryState) null)
                             .withDeliveryId(0)
                             .withDeliveryTag(new byte[] {0})
                             .withAborted(true)
                             .withSettled(true)
                             .withMore(false)
                             .withPayload(nullValue(ProtonBuffer.class));
        peer.expectDetach().withHandle(0).respond();

        Connection connection = engine.start();

        connection.open();
        Session session = connection.session();
        session.open();

        Sender sender = session.sender("sender-1");
        sender.open();

        final AtomicBoolean senderMarkedSendable = new AtomicBoolean();
        sender.sendableHandler(handler -> {
            senderMarkedSendable.set(true);
        });

        OutgoingDelivery delivery = sender.next();
        assertNotNull(delivery);

        delivery.setTag(new byte[] {0});
        delivery.streamBytes(payload);
        delivery.abort();

        assertTrue(delivery.isAborted());
        assertFalse(delivery.isPartial());
        assertTrue(delivery.isSettled());

        sender.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testAbortAlreadyAbortedDelivery() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        ProtonBuffer payload = ProtonByteBufferAllocator.DEFAULT.wrap(new byte[] {0, 1, 2, 3, 4});

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().withRole(Role.SENDER).respond();
        peer.remoteFlow().withDeliveryCount(0)
                         .withLinkCredit(10)
                         .withIncomingWindow(1024)
                         .withOutgoingWindow(10)
                         .withNextIncomingId(0)
                         .withNextOutgoingId(1).queue();
        peer.expectTransfer().withHandle(0)
                             .withMore(true)
                             .withSettled(false)
                             .withState((DeliveryState) null)
                             .withDeliveryId(0)
                             .withDeliveryTag(new byte[] {0})
                             .withPayload(payload.copy());
        peer.expectTransfer().withHandle(0)
                             .withState((DeliveryState) null)
                             .withDeliveryId(0)
                             .withDeliveryTag(new byte[] {0})
                             .withAborted(true)
                             .withSettled(true)
                             .withMore(false)
                             .withPayload(nullValue(ProtonBuffer.class));
        peer.expectDetach().withHandle(0).respond();

        Connection connection = engine.start();

        connection.open();
        Session session = connection.session();
        session.open();

        Sender sender = session.sender("sender-1");
        sender.open();

        final AtomicBoolean senderMarkedSendable = new AtomicBoolean();
        sender.sendableHandler(handler -> {
            senderMarkedSendable.set(true);
        });

        OutgoingDelivery delivery = sender.next();
        assertNotNull(delivery);

        delivery.setTag(new byte[] {0});
        delivery.streamBytes(payload);
        delivery.abort();

        assertTrue(delivery.isAborted());
        assertFalse(delivery.isPartial());
        assertTrue(delivery.isSettled());

        // Second abort attempt should not error out or trigger additional frames
        delivery.abort();

        sender.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testAbortOnDeliveryThatHasNoWritesIsNoOp() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().withRole(Role.SENDER).respond();
        peer.remoteFlow().withDeliveryCount(0)
                         .withLinkCredit(10)
                         .withIncomingWindow(1024)
                         .withOutgoingWindow(10)
                         .withNextIncomingId(0)
                         .withNextOutgoingId(1).queue();
        peer.expectDetach().withHandle(0).respond();

        Connection connection = engine.start();

        connection.open();
        Session session = connection.session();
        session.open();

        Sender sender = session.sender("sender-1");
        sender.open();

        final AtomicBoolean senderMarkedSendable = new AtomicBoolean();
        sender.sendableHandler(handler -> {
            senderMarkedSendable.set(true);
        });

        OutgoingDelivery delivery = sender.next();
        assertNotNull(delivery);

        delivery.setTag(new byte[] {0});
        delivery.abort();

        assertSame(delivery, sender.current());
        assertFalse(delivery.isAborted());
        assertTrue(delivery.isPartial());
        assertFalse(delivery.isSettled());

        sender.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSettleTransferWithNullDisposition() throws Exception {
        doTestSettleTransferWithSpecifiedOutcome(null);
    }

    @Test
    public void testSettleTransferWithAcceptedDisposition() throws Exception {
        doTestSettleTransferWithSpecifiedOutcome(Accepted.getInstance());
    }

    @Test
    public void testSettleTransferWithReleasedDisposition() throws Exception {
        doTestSettleTransferWithSpecifiedOutcome(Released.getInstance());
    }

    @Test
    public void testSettleTransferWithRejectedDisposition() throws Exception {
        doTestSettleTransferWithSpecifiedOutcome(new Rejected());
    }

    @Test
    public void testSettleTransferWithRejectedWithErrorDisposition() throws Exception {
        doTestSettleTransferWithSpecifiedOutcome(new Rejected().setError(new ErrorCondition(AmqpError.DECODE_ERROR, "test")));
    }

    @Test
    public void testSettleTransferWithModifiedDisposition() throws Exception {
        // TODO - Matcher has an issue with false types mapped to null by the codec.
        // doTestSettleTransferWithSpecifiedOutcome(new Modified().setDeliveryFailed(true).setUndeliverableHere(false));
        doTestSettleTransferWithSpecifiedOutcome(new Modified().setDeliveryFailed(true).setUndeliverableHere(true));
    }

    @Test
    public void testSettleTransferWithTransactionalDisposition() throws Exception {
        doTestSettleTransferWithSpecifiedOutcome(new TransactionalState().setTxnId(new Binary(new byte[] {1})).setOutcome(Accepted.getInstance()));
    }

    private void doTestSettleTransferWithSpecifiedOutcome(DeliveryState state) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().withRole(Role.SENDER).respond();
        peer.remoteFlow().withDeliveryCount(0)
                         .withLinkCredit(10)
                         .withIncomingWindow(1024)
                         .withOutgoingWindow(10)
                         .withNextIncomingId(0)
                         .withNextOutgoingId(1).queue();
        peer.expectTransfer().withHandle(0)
                             .withSettled(false)
                             .withState((DeliveryState) null)
                             .withDeliveryId(0)
                             .withDeliveryTag(new byte[] {0});
        peer.expectDisposition().withFirst(0)
                                .withSettled(true)
                                .withState(state);
        peer.expectDetach().withHandle(0).respond();

        Connection connection = engine.start();

        connection.open();
        Session session = connection.session();
        session.open();

        ProtonBuffer payload = ProtonByteBufferAllocator.DEFAULT.wrap(new byte[] {0, 1, 2, 3, 4});

        Sender sender = session.sender("sender-1");

        final AtomicBoolean deliverySentAfterSenable = new AtomicBoolean();
        sender.sendableHandler(handler -> {
            handler.next().setTag(new byte[] {0}).writeBytes(payload);
            deliverySentAfterSenable.set(true);
        });

        sender.open();

        assertTrue("Delivery should have been sent after credit arrived", deliverySentAfterSenable.get());

        OutgoingDelivery delivery = sender.current();
        assertNotNull(delivery);
        delivery.disposition(state, true);

        sender.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testAttemptedSecondDispostionOnAlreadySettledDeliveryNull() throws Exception {
        doTestAttemptedSecondDispostionOnAlreadySettledDelivery(Accepted.getInstance(), null);
    }

    @Test
    public void testAttemptedSecondDispostionOnAlreadySettledDeliveryReleased() throws Exception {
        doTestAttemptedSecondDispostionOnAlreadySettledDelivery(Accepted.getInstance(), Released.getInstance());
    }

    @Test
    public void testAttemptedSecondDispostionOnAlreadySettledDeliveryModiified() throws Exception {
        doTestAttemptedSecondDispostionOnAlreadySettledDelivery(Released.getInstance(), new Modified().setDeliveryFailed(true));
    }

    @Test
    public void testAttemptedSecondDispostionOnAlreadySettledDeliveryRejected() throws Exception {
        doTestAttemptedSecondDispostionOnAlreadySettledDelivery(Released.getInstance(), new Rejected());
    }

    @Test
    public void testAttemptedSecondDispostionOnAlreadySettledDeliveryTransactional() throws Exception {
        doTestAttemptedSecondDispostionOnAlreadySettledDelivery(Released.getInstance(), new TransactionalState().setOutcome(Accepted.getInstance()));
    }

    private void doTestAttemptedSecondDispostionOnAlreadySettledDelivery(DeliveryState first, DeliveryState second) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().withRole(Role.SENDER).respond();
        peer.remoteFlow().withDeliveryCount(0)
                         .withLinkCredit(10)
                         .withIncomingWindow(1024)
                         .withOutgoingWindow(10)
                         .withNextIncomingId(0)
                         .withNextOutgoingId(1).queue();
        peer.expectTransfer().withHandle(0)
                             .withSettled(false)
                             .withState((DeliveryState) null)
                             .withDeliveryId(0)
                             .withDeliveryTag(new byte[] {0});
        peer.expectDisposition().withFirst(0)
                                .withSettled(true)
                                .withState(first);
        peer.expectDetach().withHandle(0).respond();

        Connection connection = engine.start();

        connection.open();
        Session session = connection.session();
        session.open();

        ProtonBuffer payload = ProtonByteBufferAllocator.DEFAULT.wrap(new byte[] {0, 1, 2, 3, 4});

        Sender sender = session.sender("sender-1");

        final AtomicBoolean deliverySentAfterSenable = new AtomicBoolean();
        sender.sendableHandler(handler -> {
            handler.next().setTag(new byte[] {0}).writeBytes(payload);
            deliverySentAfterSenable.set(true);
        });

        sender.open();

        assertTrue("Delivery should have been sent after credit arrived", deliverySentAfterSenable.get());

        OutgoingDelivery delivery = sender.current();
        assertNotNull(delivery);
        delivery.disposition(first, true);

        // A second attempt at the same outcome should result in no action.
        delivery.disposition(first, true);

        try {
            delivery.disposition(second, true);
            fail("Should not be able to update outcome on already setttled delivery");
        } catch (IllegalStateException ise) {
            // Expected
        }

        sender.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSettleSentDeliveryAfterRemoteSettles() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().withRole(Role.SENDER).respond();
        peer.remoteFlow().withDeliveryCount(0)
                         .withLinkCredit(10)
                         .withIncomingWindow(1024)
                         .withOutgoingWindow(10)
                         .withNextIncomingId(0)
                         .withNextOutgoingId(1).queue();
        peer.expectTransfer().withHandle(0)
                             .withSettled(false)
                             .withState((DeliveryState) null)
                             .withDeliveryId(0)
                             .withDeliveryTag(new byte[] {0})
                             .respond()
                                 .withSettled(true)
                                 .withState(Accepted.getInstance());
        peer.expectDisposition().withFirst(0)
                                .withSettled(true)
                                .withState(nullValue());
        peer.expectDetach().withHandle(0).respond();

        Connection connection = engine.start();

        connection.open();
        Session session = connection.session();
        session.open();

        ProtonBuffer payload = ProtonByteBufferAllocator.DEFAULT.wrap(new byte[] {0, 1, 2, 3, 4});

        Sender sender = session.sender("sender-1");

        final AtomicBoolean deliverySentAfterSenable = new AtomicBoolean();
        sender.sendableHandler(handler -> {
            handler.next().setTag(new byte[] {0}).writeBytes(payload);
            deliverySentAfterSenable.set(true);
        });

        sender.deliveryUpdatedHandler((delivery) -> {
            if (delivery.isRemotelySettled()) {
                delivery.settle();
            }
        });

        sender.open();

        assertTrue("Delivery should have been sent after credit arrived", deliverySentAfterSenable.get());

        OutgoingDelivery delivery = sender.current();
        assertNotNull(delivery);
        assertTrue(delivery.isRemotelySettled());
        assertSame(Accepted.getInstance(), delivery.getRemoteState());
        assertNull(delivery.getState());
        assertTrue(delivery.isSettled());

        sender.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSenderHandlesDeferredOpenAndBeginAttachResponses() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        final AtomicBoolean senderRemotelyOpened = new AtomicBoolean();

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen();
        peer.expectBegin();
        peer.expectAttach().withRole(Role.SENDER)
                           .withTarget().withDynamic(true).withAddress((String) null);

        Connection connection = engine.start();

        connection.open();
        Session session = connection.session();
        session.open();

        Sender sender = session.sender("sender-1");
        sender.setTarget(new Target().setDynamic(true).setAddress(null));
        sender.openHandler(result -> senderRemotelyOpened.set(true)).open();

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

        // This should happen after we inject the held open and attach
        peer.expectClose().respond();

        // Inject held responses to get the ball rolling again
        peer.remoteOpen().withOfferedCapabilities("ANONYMOUS_REALY").now();
        peer.respondToLastBegin().now();
        peer.respondToLastAttach().now();

        assertTrue("Sender remote opened event did not fire", senderRemotelyOpened.get());
        assertNotNull(sender.getRemoteTarget().getAddress());

        connection.close();

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
    }
}
