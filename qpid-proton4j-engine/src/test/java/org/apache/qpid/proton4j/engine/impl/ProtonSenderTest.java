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

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.is;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.DeliveryTag;
import org.apache.qpid.proton4j.amqp.Symbol;
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
import org.apache.qpid.proton4j.engine.Delivery;
import org.apache.qpid.proton4j.engine.DeliveryTagGenerator;
import org.apache.qpid.proton4j.engine.Engine;
import org.apache.qpid.proton4j.engine.EngineFactory;
import org.apache.qpid.proton4j.engine.OutgoingDelivery;
import org.apache.qpid.proton4j.engine.Sender;
import org.apache.qpid.proton4j.engine.Session;
import org.apache.qpid.proton4j.engine.exceptions.EngineFailedException;
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
    public void testSenderRoutesDetachEventToCloseHandlerIfNonSset() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        final AtomicBoolean senderLocalOpen = new AtomicBoolean();
        final AtomicBoolean senderLocalClose = new AtomicBoolean();
        final AtomicBoolean senderRemoteOpen = new AtomicBoolean();
        final AtomicBoolean senderRemoteClose = new AtomicBoolean();

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
              .openHandler(result -> senderRemoteOpen.set(true))
              .closeHandler(result -> senderRemoteClose.set(true));

        sender.open();
        sender.detach();

        assertTrue("Sender should have reported local open", senderLocalOpen.get());
        assertTrue("Sender should have reported remote open", senderRemoteOpen.get());
        assertTrue("Sender should have reported local detach", senderLocalClose.get());
        assertTrue("Sender should have reported remote detach", senderRemoteClose.get());

        session.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test(timeout = 30000)
    public void testEngineShutdownEventNeitherEndClosed() throws Exception {
        doTestEngineShutdownEvent(false, false);
    }

    @Test(timeout = 30000)
    public void testEngineShutdownEventLocallyClosed() throws Exception {
        doTestEngineShutdownEvent(true, false);
    }

    @Test(timeout = 30000)
    public void testEngineShutdownEventRemotelyClosed() throws Exception {
        doTestEngineShutdownEvent(false, true);
    }

    @Test(timeout = 30000)
    public void testEngineShutdownEventBothEndsClosed() throws Exception {
        doTestEngineShutdownEvent(true, true);
    }

    private void doTestEngineShutdownEvent(boolean locallyClosed, boolean remotelyClosed) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        final AtomicBoolean engineShutdown = new AtomicBoolean();

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.expectAttach().respond();

        Connection connection = engine.start();

        connection.open();

        Session session = connection.session();
        session.open();

        Sender sender = session.sender("test");
        sender.open();
        sender.engineShutdownHandler(result -> engineShutdown.set(true));

        if (locallyClosed) {
            if (remotelyClosed) {
                peer.expectDetach().respond();
            } else {
                peer.expectDetach();
            }

            sender.close();
        }

        if (remotelyClosed && !locallyClosed) {
            peer.remoteDetach();
        }

        engine.shutdown();

        if (locallyClosed && remotelyClosed) {
            assertFalse("Should not have reported engine shutdown", engineShutdown.get());
        } else {
            assertTrue("Should have reported engine shutdown", engineShutdown.get());
        }

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

        assertTrue(sender.isSender());
        assertFalse(sender.isReceiver());

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

    @Test
    public void testRemotelyCloseSenderAndOpenNewSenderImmediatelyAfterWithSameLinkName() throws Exception {
        doTestRemotelyTerminateLinkAndThenCreateNewLink(true, true);
    }

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
                             .withPayload(payloadBuffer);
        peer.expectDetach().withHandle(0).respond();

        Connection connection = engine.start();

        connection.open();
        Session session = connection.session();
        session.open();

        ProtonBuffer payload = ProtonByteBufferAllocator.DEFAULT.wrap(payloadBuffer);

        Sender sender = session.sender("sender-1");

        assertFalse(sender.isSendable());

        sender.creditStateUpdateHandler(handler -> {
            if (handler.isSendable()) {
                handler.next().setTag(new byte[] {0}).writeBytes(payload);
            }
        });

        sender.open();
        sender.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSenderSignalsDeliveryUpdatedOnSettledThenSettleFromLinkAPI() throws Exception {
        doTestSenderSignalsDeliveryUpdatedOnSettled(true);
    }

    @Test
    public void testSenderSignalsDeliveryUpdatedOnSettledThenSettleDelivery() throws Exception {
        doTestSenderSignalsDeliveryUpdatedOnSettled(false);
    }

    private void doTestSenderSignalsDeliveryUpdatedOnSettled(boolean settleFromLink) {

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
        sender.deliveryStateUpdatedHandler(delivery -> {
            if (delivery.isRemotelySettled()) {
                deliveryUpdatedAndSettled.set(true);
            }

            updatedDelivery.set(delivery);
        });

        assertFalse(sender.isSendable());

        sender.creditStateUpdateHandler(handler -> {
            if (handler.isSendable()) {
                handler.next().setTag(new byte[] {0}).writeBytes(payload);
            }
        });

        sender.open();

        assertTrue("Delivery should have been updated and state settled", deliveryUpdatedAndSettled.get());
        assertEquals(Accepted.getInstance(), updatedDelivery.get().getRemoteState());
        assertTrue(sender.hasUnsettled());
        assertFalse(sender.unsettled().isEmpty());

        if (settleFromLink) {
            sender.settle(delivery -> true);
        } else {
            updatedDelivery.get().settle();
        }

        assertFalse(sender.hasUnsettled());
        assertTrue(sender.unsettled().isEmpty());

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
        final AtomicReference<Delivery> sent = new AtomicReference<>();

        sender.creditStateUpdateHandler(handler -> {
            if (handler.isSendable()) {
                sent.set(handler.next().setTag(new byte[] {0}).writeBytes(payload));
                deliverySentAfterSenable.set(true);
            }
        });

        sender.open();

        assertTrue("Delivery should have been sent after credit arrived", deliverySentAfterSenable.get());

        assertNull(sender.current());

        sent.get().disposition(Accepted.getInstance(), true);

        OutgoingDelivery delivery2 = sender.next();
        assertNotSame(delivery2, sent.get());
        delivery2.disposition(Released.getInstance(), true);

        assertFalse(sender.hasUnsettled());

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

        try {
            sender.next();
            fail("Should not be able get next after connection closed");
        } catch (IllegalStateException ise) {
            // Should not allow next message after close of connection
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
        sender1.creditStateUpdateHandler(handler -> {
            sender1MarkedSendable.set(handler.isSendable());
        });

        final AtomicBoolean sender2MarkedSendable = new AtomicBoolean();
        sender2.creditStateUpdateHandler(handler -> {
            sender2MarkedSendable.set(handler.isSendable());
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
                             .withDeliveryTag(nullValue());
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
                                 .withDeliveryTag(nullValue());
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
        sender.creditStateUpdateHandler(handler -> {
            senderMarkedSendable.set(handler.isSendable());
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
        if (remoteMaxFrameSize == 0 && outboundFrameSizeLimit == 0) {
            frameCount = 1;
        } else if(remoteMaxFrameSize == 0 && outboundFrameSizeLimit != 0) {
            frameCount = (int) Math.ceil((double)contentLength / (double) outboundFrameSizeLimit);
        } else {
            int effectiveMaxFrameSize;
            if (outboundFrameSizeLimit != 0) {
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
                                 .withDeliveryTag(i == 1 ? notNullValue() : nullValue())
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
                             .withMore(anyOf(nullValue(), is(false)))
                             .withPayload(nullValue(ProtonBuffer.class));
        peer.expectDetach().withHandle(0).respond();

        Connection connection = engine.start();

        connection.open();
        Session session = connection.session();
        session.open();

        Sender sender = session.sender("sender-1");
        sender.open();

        final AtomicBoolean senderMarkedSendable = new AtomicBoolean();
        sender.creditStateUpdateHandler(handler -> {
            senderMarkedSendable.set(sender.isSendable());
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
                             .withMore(anyOf(nullValue(), is(false)))
                             .withPayload(nullValue(ProtonBuffer.class));
        peer.expectDetach().withHandle(0).respond();

        Connection connection = engine.start();

        connection.open();
        Session session = connection.session();
        session.open();

        Sender sender = session.sender("sender-1");
        sender.open();

        final AtomicBoolean senderMarkedSendable = new AtomicBoolean();
        sender.creditStateUpdateHandler(handler -> {
            senderMarkedSendable.set(sender.isSendable());
        });

        OutgoingDelivery delivery = sender.next();
        assertNotNull(delivery);

        delivery.setTag(new byte[] {0});
        delivery.streamBytes(payload);

        assertTrue(sender.hasUnsettled());

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
        sender.creditStateUpdateHandler(handler -> {
            senderMarkedSendable.set(sender.isSendable());
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
    public void testAbortOnDeliveryThatHasNoWritesIsNoOpThenSendUsingCurrent() throws Exception {
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

        ProtonBuffer payload = ProtonByteBufferAllocator.DEFAULT.wrap(new byte[] {0, 1, 2, 3, 4});

        Connection connection = engine.start();

        connection.open();
        Session session = connection.session();
        session.open();

        Sender sender = session.sender("sender-1");
        sender.open();

        final AtomicBoolean senderMarkedSendable = new AtomicBoolean();
        sender.creditStateUpdateHandler(handler -> {
            senderMarkedSendable.set(sender.isSendable());
        });

        OutgoingDelivery delivery = sender.next();
        assertNotNull(delivery);

        delivery.setTag(new byte[] {0});
        delivery.abort();

        assertSame(delivery, sender.current());
        try {
            sender.next();
            fail("Should not be able to next as current was not aborted since nothing was ever written.");
        } catch (IllegalStateException ise) {
            // Expected
        }
        assertFalse(delivery.isAborted());
        assertTrue(delivery.isPartial());
        assertFalse(delivery.isSettled());

        peer.expectTransfer().withHandle(0)
                             .withSettled(false)
                             .withState((DeliveryState) null)
                             .withDeliveryId(0)
                             .withDeliveryTag(new byte[] {1})
                             .withPayload(payload.duplicate());
        peer.expectDisposition().withFirst(0).withSettled(true).withState(Accepted.getInstance());
        peer.expectDetach().withHandle(0).respond();

        delivery = sender.current();
        delivery.setTag(new byte[] {1}).writeBytes(payload);
        delivery.disposition(Accepted.getInstance(), true);

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
        final AtomicReference<Delivery> sentDelivery = new AtomicReference<>();
        sender.creditStateUpdateHandler(handler -> {
            sentDelivery.set(handler.next().setTag(new byte[] {0}).writeBytes(payload));
            deliverySentAfterSenable.set(sender.isSendable());
        });

        sender.open();

        assertTrue("Delivery should have been sent after credit arrived", deliverySentAfterSenable.get());

        OutgoingDelivery delivery = sender.current();
        assertNull(delivery);
        sentDelivery.get().disposition(state, true);

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
        final AtomicReference<Delivery> sentDelivery = new AtomicReference<>();

        final AtomicBoolean deliverySentAfterSenable = new AtomicBoolean();
        sender.creditStateUpdateHandler(handler -> {
            sentDelivery.set(handler.next().setTag(new byte[] {0}).writeBytes(payload));
            deliverySentAfterSenable.set(sender.isSendable());
        });

        sender.open();

        assertTrue("Delivery should have been sent after credit arrived", deliverySentAfterSenable.get());

        OutgoingDelivery delivery = sender.current();
        assertNull(delivery);
        sentDelivery.get().disposition(first, true);

        // A second attempt at the same outcome should result in no action.
        sentDelivery.get().disposition(first, true);

        try {
            sentDelivery.get().disposition(second, true);
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
        peer.expectDetach().withHandle(0).respond();

        Connection connection = engine.start();

        connection.open();
        Session session = connection.session();
        session.open();

        ProtonBuffer payload = ProtonByteBufferAllocator.DEFAULT.wrap(new byte[] {0, 1, 2, 3, 4});

        Sender sender = session.sender("sender-1");

        final AtomicBoolean deliverySentAfterSenable = new AtomicBoolean();
        final AtomicReference<Delivery> sentDelivery = new AtomicReference<>();
        sender.creditStateUpdateHandler(handler -> {
            sentDelivery.set(handler.next().setTag(new byte[] {0}).writeBytes(payload));
            deliverySentAfterSenable.set(sender.isSendable());
        });

        sender.deliveryStateUpdatedHandler((delivery) -> {
            if (delivery.isRemotelySettled()) {
                delivery.settle();
            }
        });

        sender.open();

        assertTrue("Delivery should have been sent after credit arrived", deliverySentAfterSenable.get());

        assertNull(sender.current());

        assertTrue(sentDelivery.get().isRemotelySettled());
        assertSame(Accepted.getInstance(), sentDelivery.get().getRemoteState());
        assertNull(sentDelivery.get().getState());
        assertTrue(sentDelivery.get().isSettled());

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
        assertNotNull(sender.<Target>getRemoteTarget().getAddress());

        connection.close();

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
    }

    @Test
    public void testCloseAfterShutdownDoesNotThrowExceptionOpenAndBeginWrittenAndResponseAttachWrittenAndRsponse() throws Exception {
        testCloseAfterShutdownNoOutputAndNoException(true, true, true, true);
    }

    @Test
    public void testCloseAfterShutdownDoesNotThrowExceptionOpenAndBeginWrittenAndResponseAttachWrittenAndNoRsponse() throws Exception {
        testCloseAfterShutdownNoOutputAndNoException(true, true, true, false);
    }

    @Test
    public void testCloseAfterShutdownDoesNotThrowExceptionOpenWrittenAndResponseBeginWrittenAndNoRsponse() throws Exception {
        testCloseAfterShutdownNoOutputAndNoException(true, true, false, false);
    }

    @Test
    public void testCloseAfterShutdownDoesNotThrowExceptionOpenWrittenButNoResponse() throws Exception {
        testCloseAfterShutdownNoOutputAndNoException(true, false, false, false);
    }

    @Test
    public void testCloseAfterShutdownDoesNotThrowExceptionOpenNotWritten() throws Exception {
        testCloseAfterShutdownNoOutputAndNoException(false, false, false, false);
    }

    private void testCloseAfterShutdownNoOutputAndNoException(boolean respondToHeader, boolean respondToOpen, boolean respondToBegin, boolean respondToAttach) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        if (respondToHeader) {
            peer.expectAMQPHeader().respondWithAMQPHeader();
            if (respondToOpen) {
                peer.expectOpen().respond();
                if (respondToBegin) {
                    peer.expectBegin().respond();
                    if (respondToAttach) {
                        peer.expectAttach().respond();
                    } else {
                        peer.expectAttach();
                    }
                } else {
                    peer.expectBegin();
                    peer.expectAttach();
                }
            } else {
                peer.expectOpen();
                peer.expectBegin();
                peer.expectAttach();
            }
        } else {
            peer.expectAMQPHeader();
        }

        Connection connection = engine.start();
        connection.open();

        Session session = connection.session();
        session.open();

        Sender sender = session.sender("test");
        sender.open();

        engine.shutdown();

        // Should clean up and not throw as we knowingly shutdown engine operations.
        sender.close();
        session.close();
        connection.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testCloseAfterFailureThrowsEngineStateExceptionOpenAndBeginWrittenAndResponseAttachWrittenAndReponse() throws Exception {
        testCloseAfterEngineFailedThrowsAndNoOutputWritten(true, true, true, true);
    }

    @Test
    public void testCloseAfterFailureThrowsEngineStateExceptionOpenAndBeginWrittenAndResponseAttachWrittenAndNoResponse() throws Exception {
        testCloseAfterEngineFailedThrowsAndNoOutputWritten(true, true, true, false);
    }

    @Test
    public void testCloseAfterFailureThrowsEngineStateExceptionOpenWrittenAndResponseBeginWrittenAndNoResponse() throws Exception {
        testCloseAfterEngineFailedThrowsAndNoOutputWritten(true, true, true, false);
    }

    @Test
    public void testCloseAfterFailureThrowsEngineStateExceptionOpenWrittenButNoResponse() throws Exception {
        testCloseAfterEngineFailedThrowsAndNoOutputWritten(true, false, false, false);
    }

    @Test
    public void testCloseAfterFailureThrowsEngineStateExceptionOpenNotWritten() throws Exception {
        testCloseAfterEngineFailedThrowsAndNoOutputWritten(false, false, false, false);
    }

    private void testCloseAfterEngineFailedThrowsAndNoOutputWritten(boolean respondToHeader, boolean respondToOpen, boolean respondToBegin, boolean respondToAttach) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        if (respondToHeader) {
            peer.expectAMQPHeader().respondWithAMQPHeader();
            if (respondToOpen) {
                peer.expectOpen().respond();
                if (respondToBegin) {
                    peer.expectBegin().respond();
                    if (respondToAttach) {
                        peer.expectAttach().respond();
                    } else {
                        peer.expectAttach();
                    }
                } else {
                    peer.expectBegin();
                    peer.expectAttach();
                }
            } else {
                peer.expectOpen();
                peer.expectBegin();
                peer.expectAttach();
            }
        } else {
            peer.expectAMQPHeader();
        }

        Connection connection = engine.start();
        connection.open();

        Session session = connection.session();
        session.open();

        Sender sender = session.sender("test");
        sender.open();

        engine.engineFailed(new IOException());

        try {
            sender.close();
            fail("Should throw exception indicating engine is in a failed state.");
        } catch (EngineFailedException efe) {}

        try {
            session.close();
            fail("Should throw exception indicating engine is in a failed state.");
        } catch (EngineFailedException efe) {}

        try {
            connection.close();
            fail("Should throw exception indicating engine is in a failed state.");
        } catch (EngineFailedException efe) {}

        engine.shutdown();  // Explicit shutdown now allows local close to complete

        // Should clean up and not throw as we knowingly shutdown engine operations.
        sender.close();
        session.close();
        connection.close();

        peer.waitForScriptToComplete();

        assertNotNull(failure);
    }

    @Test(timeout = 30000)
    public void testCloseReceiverWithErrorCondition() throws Exception {
        doTestCloseOrDetachWithErrorCondition(true);
    }

    @Test(timeout = 30000)
    public void testDetachReceiverWithErrorCondition() throws Exception {
        doTestCloseOrDetachWithErrorCondition(false);
    }

    private void doTestCloseOrDetachWithErrorCondition(boolean close) throws Exception {
        final String condition = "amqp:link:detach-forced";
        final String description = "something bad happened.";

        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.expectDetach().withClosed(close)
                           .withError(new ErrorCondition(Symbol.valueOf(condition), description))
                           .respond();
        peer.expectClose();

        Connection connection = engine.start();

        connection.open();
        Session session = connection.session();
        session.open();

        Sender sender = session.sender("sender-1");
        sender.open();
        sender.setCondition(new ErrorCondition(Symbol.valueOf(condition), description));

        if (close) {
            sender.close();
        } else {
            sender.detach();
        }

        connection.close();

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
    }

    @Test(timeout = 20000)
    public void testSenderSignalsDrainedWhenCreditOutstanding() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.remoteFlow().withDeliveryCount(0).withLinkCredit(10).withDrain(true).queue();
        peer.expectFlow().withDeliveryCount(10).withLinkCredit(0).withDrain(true);
        peer.expectDetach().respond();
        peer.expectClose().respond();

        Connection connection = engine.start().open();
        Session session = connection.session().open();
        Sender sender = session.sender("sender-1");

        sender.creditStateUpdateHandler(link -> link.drained());
        sender.open();
        sender.close();

        connection.close();

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
    }

    @Test(timeout = 20000)
    public void testSenderOmitsFlowWhenDrainedCreditIsSatisfied() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        ProtonBuffer payload = ProtonByteBufferAllocator.DEFAULT.wrap(new byte[] {0, 1, 2, 3, 4});

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.remoteFlow().withDeliveryCount(0).withLinkCredit(1).withDrain(true).queue();

        peer.expectTransfer().withHandle(0)
                             .withSettled(false)
                             .withState((DeliveryState) null)
                             .withDeliveryId(0)
                             .withDeliveryTag(new byte[] {0})
                             .respond()
                             .withSettled(true)
                             .withState(Accepted.getInstance());

        Connection connection = engine.start().open();
        Session session = connection.session().open();
        Sender sender = session.sender("sender-1");

        final AtomicBoolean deliverySentAfterSenable = new AtomicBoolean();
        final AtomicReference<Delivery> sentDelivery = new AtomicReference<>();
        sender.creditStateUpdateHandler(link -> {
            if (link.isSendable()) {
                sentDelivery.set(link.next().setTag(new byte[] {0}).writeBytes(payload));
                deliverySentAfterSenable.set(true);
            }
        });

        sender.deliveryStateUpdatedHandler((delivery) -> {
            if (delivery.isRemotelySettled()) {
                delivery.settle();
            }
        });

        sender.open();

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        peer.expectDetach().respond();
        peer.expectClose().respond();

        // Should not send a flow as the send fulfilled the requested drain amount.
        sender.drained();

        sender.close();
        connection.close();

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
    }

    @Test
    public void testSenderAppliesDeliveryTagGeneratorToNextDelivery() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        ProtonBuffer payload = ProtonByteBufferAllocator.DEFAULT.wrap(new byte[] {0, 1, 2, 3, 4});

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.expectAttach().withRole(Role.SENDER).respond();
        peer.remoteFlow().withLinkCredit(10).queue();

        Connection connection = engine.start().open();
        Session session = connection.session().open();
        Sender sender = session.sender("sender-1");

        sender.setDeliveryTagGenerator(ProtonDeliveryTagGenerator.BUILTIN.SEQUENTIAL.createGenerator());
        sender.deliveryStateUpdatedHandler((delivery) -> {
            delivery.settle();
        });

        sender.open();

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        peer.expectTransfer().withPayload(notNullValue(ProtonBuffer.class))
                             .withDeliveryTag(new byte[] {0}).respond().withSettled(true).withState(Accepted.getInstance());
        peer.expectTransfer().withPayload(notNullValue(ProtonBuffer.class))
                             .withDeliveryTag(new byte[] {1}).respond().withSettled(true).withState(Accepted.getInstance());
        peer.expectTransfer().withPayload(notNullValue(ProtonBuffer.class))
                             .withDeliveryTag(new byte[] {2}).respond().withSettled(true).withState(Accepted.getInstance());

        OutgoingDelivery delivery1 = sender.next();
        delivery1.writeBytes(payload.duplicate());
        OutgoingDelivery delivery2 = sender.next();
        delivery2.writeBytes(payload.duplicate());
        OutgoingDelivery delivery3 = sender.next();
        delivery3.writeBytes(payload.duplicate());

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

        assertNotNull(delivery1);
        assertTrue(delivery1.isSettled());
        assertTrue(delivery1.isRemotelySettled());
        assertNotNull(delivery2);
        assertTrue(delivery2.isSettled());
        assertTrue(delivery2.isRemotelySettled());
        assertNotNull(delivery3);
        assertTrue(delivery3.isSettled());
        assertTrue(delivery3.isRemotelySettled());

        peer.expectDetach().respond();
        peer.expectClose().respond();

        sender.close();
        connection.close();

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
    }

    @Test(timeout = 20000)
    public void testSenderAppliedGeneratedDeliveryTagCanBeOverriden() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        final byte [] payloadBuffer = new byte[] {0, 1, 2, 3, 4};

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

        Connection connection = engine.start();

        connection.open();
        Session session = connection.session();
        session.open();

        ProtonBuffer payload = ProtonByteBufferAllocator.DEFAULT.wrap(payloadBuffer);

        Sender sender = session.sender("sender-1");

        assertFalse(sender.isSendable());

        final DeliveryTagGenerator generator = ProtonDeliveryTagGenerator.BUILTIN.POOLED.createGenerator();

        sender.setDeliveryTagGenerator(generator);
        sender.open();

        peer.waitForScriptToComplete();
        peer.expectTransfer().withHandle(0)
                             .withSettled(false)
                             .withState((DeliveryState) null)
                             .withDeliveryId(0)
                             .withDeliveryTag(new byte[] {127})
                             .withPayload(payloadBuffer);
        peer.expectDetach().withHandle(0).respond();

        OutgoingDelivery delivery = sender.next();

        DeliveryTag oldTag = delivery.getTag();

        delivery.setTag(new byte[] {127});

        // Pooled tag should be reused.
        assertSame(oldTag, generator.nextTag());

        delivery.writeBytes(payload);

        sender.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test(timeout = 20000)
    public void testSenderReleasesPooledDeliveryTagsAfterSettledByBoth() throws Exception {
        doTestSenderReleasesPooledDeliveryTags(false, true);
    }

    @Test(timeout = 20000)
    public void testSenderReleasesPooledDeliveryTagsAfterSettledAfterDispositionUpdate() throws Exception {
        doTestSenderReleasesPooledDeliveryTags(false, false);
    }

    @Test(timeout = 20000)
    public void testSenderReleasesPooledDeliveryTagsSenderSettlesFirst() throws Exception {
        doTestSenderReleasesPooledDeliveryTags(true, false);
    }

    private void doTestSenderReleasesPooledDeliveryTags(boolean sendSettled, boolean receiverSettles) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        ProtonBuffer payload = ProtonByteBufferAllocator.DEFAULT.wrap(new byte[] {0, 1, 2, 3, 4});

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.remoteFlow().withDeliveryCount(0).withLinkCredit(10).queue();

        Connection connection = engine.start().open();
        Session session = connection.session().open();
        Sender sender = session.sender("sender-1");

        sender.setDeliveryTagGenerator(ProtonDeliveryTagGenerator.BUILTIN.POOLED.createGenerator());

        sender.deliveryStateUpdatedHandler((delivery) -> {
            delivery.settle();
        });

        sender.open();

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

        final int toSend = sender.getCredit();
        final byte[] expectedTag = new byte[] {0};

        List<Delivery> sent = new ArrayList<>(toSend);

        for (int i = 0; i < toSend; ++i) {
            peer.expectTransfer().withHandle(0)
                                 .withSettled(sendSettled)
                                 .withState((DeliveryState) null)
                                 .withDeliveryId(i)
                                 .withDeliveryTag(expectedTag)
                                 .respond()
                                 .withSettled(receiverSettles)
                                 .withState(Accepted.getInstance());
            if (!sendSettled && !receiverSettles) {
                peer.expectDisposition().withFirst(i)
                                        .withSettled(true)
                                        .withState(nullValue());
            }
        }

        for (int i = 0; i < toSend; ++i) {
            OutgoingDelivery delivery = sender.next();

            if (sendSettled) {
                delivery.settle();
            }
            delivery.writeBytes(payload.duplicate());
        }

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

        sent.forEach(delivery -> assertEquals(delivery.getTag().tagBytes() , expectedTag));

        peer.expectDetach().respond();
        peer.expectClose().respond();

        // Should not send a flow as the send fulfilled the requested drain amount.
        sender.drained();

        sender.close();
        connection.close();

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
    }

    @Test
    public void testSenderHandlesDelayedDispositionsForSentTransfers() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        ProtonBuffer payload = ProtonByteBufferAllocator.DEFAULT.wrap(new byte[] {0, 1, 2, 3, 4});

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.expectAttach().withRole(Role.SENDER).respond();
        peer.remoteFlow().withLinkCredit(10).queue();

        Connection connection = engine.start().open();
        Session session = connection.session().open();
        Sender sender = session.sender("sender-1");

        sender.setDeliveryTagGenerator(ProtonDeliveryTagGenerator.BUILTIN.SEQUENTIAL.createGenerator());
        sender.deliveryStateUpdatedHandler((delivery) -> {
            delivery.settle();
        });

        sender.open();

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        peer.expectTransfer().withPayload(notNullValue(ProtonBuffer.class))
                             .withDeliveryTag(new byte[] {0});
        peer.expectTransfer().withPayload(notNullValue(ProtonBuffer.class))
                             .withDeliveryTag(new byte[] {1});
        peer.expectTransfer().withPayload(notNullValue(ProtonBuffer.class))
                             .withDeliveryTag(new byte[] {2});

        OutgoingDelivery delivery1 = sender.next();
        delivery1.writeBytes(payload.duplicate());
        OutgoingDelivery delivery2 = sender.next();
        delivery2.writeBytes(payload.duplicate());
        OutgoingDelivery delivery3 = sender.next();
        delivery3.writeBytes(payload.duplicate());

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

        assertNotNull(delivery1);
        assertNotNull(delivery2);
        assertNotNull(delivery3);

        peer.remoteDisposition().withRole(Role.RECEIVER).withFirst(0).withSettled(true).withState(Accepted.getInstance()).now();

        assertTrue(delivery1.isSettled());
        assertTrue(delivery1.isRemotelySettled());
        assertFalse(delivery2.isSettled());
        assertFalse(delivery2.isRemotelySettled());
        assertFalse(delivery3.isSettled());
        assertFalse(delivery3.isRemotelySettled());

        peer.remoteDisposition().withRole(Role.RECEIVER).withFirst(1).withSettled(true).withState(Accepted.getInstance()).now();

        assertTrue(delivery1.isSettled());
        assertTrue(delivery1.isRemotelySettled());
        assertTrue(delivery2.isSettled());
        assertTrue(delivery2.isRemotelySettled());
        assertFalse(delivery3.isSettled());
        assertFalse(delivery3.isRemotelySettled());

        peer.remoteDisposition().withRole(Role.RECEIVER).withFirst(2).withSettled(true).withState(Accepted.getInstance()).now();

        assertTrue(delivery1.isSettled());
        assertTrue(delivery1.isRemotelySettled());
        assertTrue(delivery2.isSettled());
        assertTrue(delivery2.isRemotelySettled());
        assertTrue(delivery3.isSettled());
        assertTrue(delivery3.isRemotelySettled());

        peer.expectDetach().respond();
        peer.expectClose().respond();

        sender.close();
        connection.close();

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
    }
}
