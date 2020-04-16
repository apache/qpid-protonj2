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
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedInteger;
import org.apache.qpid.proton4j.amqp.driver.ProtonTestPeer;
import org.apache.qpid.proton4j.amqp.messaging.Accepted;
import org.apache.qpid.proton4j.amqp.messaging.Data;
import org.apache.qpid.proton4j.amqp.messaging.Modified;
import org.apache.qpid.proton4j.amqp.messaging.Rejected;
import org.apache.qpid.proton4j.amqp.messaging.Released;
import org.apache.qpid.proton4j.amqp.messaging.Source;
import org.apache.qpid.proton4j.amqp.messaging.Target;
import org.apache.qpid.proton4j.amqp.transport.ErrorCondition;
import org.apache.qpid.proton4j.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton4j.amqp.transport.Role;
import org.apache.qpid.proton4j.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.engine.Connection;
import org.apache.qpid.proton4j.engine.Engine;
import org.apache.qpid.proton4j.engine.EngineFactory;
import org.apache.qpid.proton4j.engine.IncomingDelivery;
import org.apache.qpid.proton4j.engine.LinkState;
import org.apache.qpid.proton4j.engine.Receiver;
import org.apache.qpid.proton4j.engine.Session;
import org.apache.qpid.proton4j.engine.exceptions.EngineFailedException;
import org.hamcrest.Matcher;
import org.junit.Test;

/**
 * Test the {@link ProtonReceiver}
 */
public class ProtonReceiverTest extends ProtonEngineTestSupport {

    public static final Symbol[] SUPPORTED_OUTCOMES = new Symbol[] { Accepted.DESCRIPTOR_SYMBOL,
                                                                     Rejected.DESCRIPTOR_SYMBOL,
                                                                     Released.DESCRIPTOR_SYMBOL,
                                                                     Modified.DESCRIPTOR_SYMBOL };

    @Test
    public void testReceiverEmitsOpenAndCloseEvents() throws Exception {
        doTestReceiverEmitsEvents(false);
    }

    @Test
    public void testReceiverEmitsOpenAndDetachEvents() throws Exception {
        doTestReceiverEmitsEvents(true);
    }

    private void doTestReceiverEmitsEvents(boolean detach) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        final AtomicBoolean receiverLocalOpen = new AtomicBoolean();
        final AtomicBoolean receiverLocalClose = new AtomicBoolean();
        final AtomicBoolean receiverLocalDetach = new AtomicBoolean();
        final AtomicBoolean receiverRemoteOpen = new AtomicBoolean();
        final AtomicBoolean receiverRemoteClose = new AtomicBoolean();
        final AtomicBoolean receiverRemoteDetach = new AtomicBoolean();

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

        Receiver receiver = session.receiver("test");
        receiver.localOpenHandler(result -> receiverLocalOpen.set(true))
                .localCloseHandler(result -> receiverLocalClose.set(true))
                .localDetachHandler(result -> receiverLocalDetach.set(true))
                .openHandler(result -> receiverRemoteOpen.set(true))
                .detachHandler(result -> receiverRemoteDetach.set(true))
                .closeHandler(result -> receiverRemoteClose.set(true));

        receiver.open();

        if (detach) {
            receiver.detach();
        } else {
            receiver.close();
        }

        assertTrue("Receiver should have reported local open", receiverLocalOpen.get());
        assertTrue("Receiver should have reported remote open", receiverRemoteOpen.get());

        if (detach) {
            assertFalse("Receiver should not have reported local close", receiverLocalClose.get());
            assertTrue("Receiver should have reported local detach", receiverLocalDetach.get());
            assertFalse("Receiver should not have reported remote close", receiverRemoteClose.get());
            assertTrue("Receiver should have reported remote close", receiverRemoteDetach.get());
        } else {
            assertTrue("Receiver should have reported local close", receiverLocalClose.get());
            assertFalse("Receiver should not have reported local detach", receiverLocalDetach.get());
            assertTrue("Receiver should have reported remote close", receiverRemoteClose.get());
            assertFalse("Receiver should not have reported remote close", receiverRemoteDetach.get());
        }

        session.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testReceiverRoutesDetachEventToCloseHandlerIfNonSset() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        final AtomicBoolean receiverLocalOpen = new AtomicBoolean();
        final AtomicBoolean receiverLocalClose = new AtomicBoolean();
        final AtomicBoolean receiverRemoteOpen = new AtomicBoolean();
        final AtomicBoolean receiverRemoteClose = new AtomicBoolean();

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

        Receiver receiver = session.receiver("test");
        receiver.localOpenHandler(result -> receiverLocalOpen.set(true))
                .localCloseHandler(result -> receiverLocalClose.set(true))
                .openHandler(result -> receiverRemoteOpen.set(true))
                .closeHandler(result -> receiverRemoteClose.set(true));

        receiver.open();
        receiver.detach();

        assertTrue("Receiver should have reported local open", receiverLocalOpen.get());
        assertTrue("Receiver should have reported remote open", receiverRemoteOpen.get());
        assertTrue("Receiver should have reported local detach", receiverLocalClose.get());
        assertTrue("Receiver should have reported remote detach", receiverRemoteClose.get());

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

        Receiver receiver = session.receiver("test");
        receiver.open();
        receiver.engineShutdownHandler(result -> engineShutdown.set(true));

        if (locallyClosed) {
            if (remotelyClosed) {
                peer.expectDetach().respond();
            } else {
                peer.expectDetach();
            }

            receiver.close();
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
    public void testReceiverOpenWithNoSenderOrReceiverSettleModes() throws Exception {
        doTestOpenReceiverWithConfiguredSenderAndReceiverSettlementModes(null, null);
    }

    @Test
    public void testReceiverOpenWithSettledAndFirst() throws Exception {
        doTestOpenReceiverWithConfiguredSenderAndReceiverSettlementModes(SenderSettleMode.SETTLED, ReceiverSettleMode.FIRST);
    }

    @Test
    public void testReceiverOpenWithUnsettledAndSecond() throws Exception {
        doTestOpenReceiverWithConfiguredSenderAndReceiverSettlementModes(SenderSettleMode.UNSETTLED, ReceiverSettleMode.SECOND);
    }

    private void doTestOpenReceiverWithConfiguredSenderAndReceiverSettlementModes(SenderSettleMode senderMode, ReceiverSettleMode receiverMode) {
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

        Receiver receiver = session.receiver("test");
        receiver.setSenderSettleMode(senderMode);
        receiver.setReceiverSettleMode(receiverMode);
        receiver.open();

        peer.waitForScriptToComplete();
        peer.expectDetach().respond();

        if (senderMode != null) {
            assertEquals(senderMode, receiver.getSenderSettleMode());
        } else {
            assertEquals(SenderSettleMode.MIXED, receiver.getSenderSettleMode());
        }
        if (receiverMode != null) {
            assertEquals(receiverMode, receiver.getReceiverSettleMode());
        } else {
            assertEquals(ReceiverSettleMode.FIRST, receiver.getReceiverSettleMode());
        }

        receiver.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testCreateReceiverAndInspectRemoteEndpoint() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().withRole(Role.RECEIVER)
                           .withTarget(notNullValue())
                           .withTarget(notNullValue())
                           .respond();
        peer.expectDetach().respond();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();

        final Modified defaultOutcome = new Modified().setDeliveryFailed(true);
        final String sourceAddress = UUID.randomUUID().toString() + ":1";

        Source source = new Source();
        source.setAddress(sourceAddress);
        source.setOutcomes(SUPPORTED_OUTCOMES);
        source.setDefaultOutcome(defaultOutcome);

        Receiver receiver = session.receiver("test");
        receiver.setSource(source);
        receiver.setTarget(new Target());
        receiver.open();

        assertTrue(receiver.getRemoteState().equals(LinkState.ACTIVE));
        assertNotNull(receiver.getRemoteSource());
        assertNotNull(receiver.getRemoteTarget());
        assertArrayEquals(SUPPORTED_OUTCOMES, receiver.getRemoteSource().getOutcomes());
        assertTrue(receiver.getRemoteSource().getDefaultOutcome() instanceof Modified);
        assertEquals(sourceAddress, receiver.getRemoteSource().getAddress());

        receiver.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test(timeout = 60000)
    public void testCreateReceiverAndClose() throws Exception {
        doTestCreateReceiverAndCloseOrDetachLink(true);
    }

    @Test(timeout = 60000)
    public void testCreateReceiverAndDetach() throws Exception {
        doTestCreateReceiverAndCloseOrDetachLink(false);
    }

    private void doTestCreateReceiverAndCloseOrDetachLink(boolean close) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.expectAttach().withRole(Role.RECEIVER).respond();
        peer.expectDetach().withClosed(close).respond();
        peer.expectClose().respond();

        Connection connection = engine.start();
        connection.open();
        Session session = connection.session();
        session.open();

        Receiver receiver = session.receiver("test");
        receiver.open();

        assertTrue(receiver.isReceiver());
        assertFalse(receiver.isSender());

        if (close) {
            receiver.close();
        } else {
            receiver.detach();
        }

        connection.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testReceiverOpenAndCloseAreIdempotent() throws Exception {
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
        Receiver receiver = session.receiver("test");
        receiver.open();

        // Should not emit another attach frame
        receiver.open();

        receiver.close();

        // Should not emit another detach frame
        receiver.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testEngineEmitsAttachAfterLocalReceiverOpened() throws Exception {
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
        Receiver receiver = session.receiver("test");
        receiver.open();
        receiver.close();

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
        Receiver receiver = session.receiver("test");
        receiver.open();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testReceiverFireOpenedEventAfterRemoteAttachArrives() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.expectDetach().respond();

        final AtomicBoolean receiverRemotelyOpened = new AtomicBoolean();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("test");
        receiver.openHandler(result -> {
            receiverRemotelyOpened.set(true);
        });
        receiver.open();

        assertTrue("Receiver remote opened event did not fire", receiverRemotelyOpened.get());

        receiver.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testReceiverFireClosedEventAfterRemoteDetachArrives() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.expectDetach().respond();

        final AtomicBoolean receiverRemotelyOpened = new AtomicBoolean();
        final AtomicBoolean receiverRemotelyClosed = new AtomicBoolean();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("test");
        receiver.openHandler(result -> {
            receiverRemotelyOpened.set(true);
        });
        receiver.closeHandler(result -> {
            receiverRemotelyClosed.set(true);
        });
        receiver.open();

        assertTrue("Receiver remote opened event did not fire", receiverRemotelyOpened.get());

        receiver.close();

        assertTrue("Receiver remote closed event did not fire", receiverRemotelyClosed.get());

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testRemotelyCloseReceiverAndOpenNewReceiverImmediatelyAfterWithNewLinkName() throws Exception {
        doTestRemotelyTerminateLinkAndThenCreateNewLink(true, false);
    }

    @Test
    public void testRemotelyDetachReceiverAndOpenNewReceiverImmediatelyAfterWithNewLinkName() throws Exception {
        doTestRemotelyTerminateLinkAndThenCreateNewLink(false, false);
    }

    @Test
    public void testRemotelyCloseReceiverAndOpenNewReceiverImmediatelyAfterWithSameLinkName() throws Exception {
        doTestRemotelyTerminateLinkAndThenCreateNewLink(true, true);
    }

    @Test
    public void testRemotelyDetachReceiverAndOpenNewReceiverImmediatelyAfterWithSameLinkName() throws Exception {
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
        peer.expectAttach().withHandle(0).withRole(Role.RECEIVER).respond();
        peer.remoteDetach().withClosed(close).queue();

        final AtomicBoolean receiverRemotelyOpened = new AtomicBoolean();
        final AtomicBoolean receiverRemotelyClosed = new AtomicBoolean();
        final AtomicBoolean receiverRemotelyDetached = new AtomicBoolean();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver(firstLinkName);
        receiver.openHandler(result -> receiverRemotelyOpened.set(true));
        receiver.closeHandler(result -> receiverRemotelyClosed.set(true));
        receiver.detachHandler(result -> receiverRemotelyDetached.set(true));
        receiver.open();

        peer.waitForScriptToComplete();

        assertTrue("Receiver remote opened event did not fire", receiverRemotelyOpened.get());
        if (close) {
            assertTrue("Receiver remote closed event did not fire", receiverRemotelyClosed.get());
            assertFalse("Receiver remote detached event fired", receiverRemotelyDetached.get());
        } else {
            assertFalse("Receiver remote closed event fired", receiverRemotelyClosed.get());
            assertTrue("Receiver remote closed event did not fire", receiverRemotelyDetached.get());
        }

        peer.expectDetach().withClosed(close);
        if (close) {
            receiver.close();
        } else {
            receiver.detach();
        }

        peer.waitForScriptToComplete();
        peer.expectAttach().withHandle(0).withRole(Role.RECEIVER).respond();
        peer.expectDetach().withClosed(close).respond();

        // Reset trackers
        receiverRemotelyOpened.set(false);
        receiverRemotelyClosed.set(false);
        receiverRemotelyDetached.set(false);

        receiver = session.receiver(secondLinkName);
        receiver.openHandler(result -> receiverRemotelyOpened.set(true));
        receiver.closeHandler(result -> receiverRemotelyClosed.set(true));
        receiver.detachHandler(result -> receiverRemotelyDetached.set(true));
        receiver.open();

        if (close) {
            receiver.close();
        } else {
            receiver.detach();
        }

        peer.waitForScriptToComplete();

        assertTrue("Receiver remote opened event did not fire", receiverRemotelyOpened.get());
        if (close) {
            assertTrue("Receiver remote closed event did not fire", receiverRemotelyClosed.get());
            assertFalse("Receiver remote detached event fired", receiverRemotelyDetached.get());
        } else {
            assertFalse("Receiver remote closed event fired", receiverRemotelyClosed.get());
            assertTrue("Receiver remote closed event did not fire", receiverRemotelyDetached.get());
        }

        assertNull(failure);
    }

    @Test
    public void testReceiverFireOpenedEventAfterRemoteAttachArrivesWithNullTarget() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond().withSource((Source) null);
        peer.expectDetach().respond();

        final AtomicBoolean receiverRemotelyOpened = new AtomicBoolean();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("test");
        receiver.setSource(new Source());
        receiver.setTarget(new Target());
        receiver.openHandler(result -> {
            receiverRemotelyOpened.set(true);
        });
        receiver.open();

        assertTrue("Receiver remote opened event did not fire", receiverRemotelyOpened.get());
        assertNull(receiver.getRemoteSource());

        receiver.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testOpenAndCloseMultipleReceivers() throws Exception {
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

        Receiver receiver1 = session.receiver("receiver-1");
        receiver1.open();
        Receiver receiver2 = session.receiver("receiver-2");
        receiver2.open();

        // Close in reverse order
        receiver2.close();
        receiver1.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testConnectionSignalsRemoteReceiverOpen() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.remoteAttach().withName("sender")
                             .withHandle(0)
                             .withRole(Role.SENDER)
                             .withInitialDeliveryCount(0).queue();
        peer.expectAttach();
        peer.expectDetach().respond();

        final AtomicBoolean receiverRemotelyOpened = new AtomicBoolean();
        final AtomicReference<Receiver> receiver = new AtomicReference<>();

        Connection connection = engine.start();

        connection.receiverOpenHandler(result -> {
            receiverRemotelyOpened.set(true);
            receiver.set(result);
        });

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();

        assertTrue("Receiver remote opened event did not fire", receiverRemotelyOpened.get());

        receiver.get().open();
        receiver.get().close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testCannotOpenReceiverAfterSessionClosed() throws Exception {
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

        Receiver receiver = session.receiver("test");

        session.close();

        try {
            receiver.open();
            fail("Should not be able to open a link from a closed session.");
        } catch (IllegalStateException ise) {}

        receiver.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testCannotOpenReceiverAfterSessionRemotelyClosed() throws Exception {
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
        Receiver receiver = session.receiver("test");
        session.open();

        try {
            receiver.open();
            fail("Should not be able to open a link from a remotely closed session.");
        } catch (IllegalStateException ise) {}

        receiver.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testOpenReceiverBeforeOpenConnection() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        // Create the connection but don't open, then open a session and a receiver and
        // the session begin and receiver attach shouldn't go out until the connection
        // is opened locally.
        Connection connection = engine.start();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("receiver");
        receiver.open();

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.expectAttach().withHandle(0).withName("receiver").withRole(Role.RECEIVER).respond();

        // Now open the connection, expect the Open, Begin, and Attach frames
        connection.open();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testOpenReceiverBeforeOpenSession() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();

        // Create the connection and open it, then create a session and a receiver
        // and observe that the receiver doesn't send its attach until the session
        // is opened.
        Connection connection = engine.start();
        connection.open();
        Session session = connection.session();
        Receiver receiver = session.receiver("receiver");
        receiver.open();

        peer.expectBegin().respond();
        peer.expectAttach().withHandle(0).withName("receiver").withRole(Role.RECEIVER).respond();

        // Now open the session, expect the Begin, and Attach frames
        session.open();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testReceiverDetachAfterEndSent() {
        doTestReceiverCloseOrDetachAfterEndSent(false);
    }

    @Test
    public void testReceiverCloseAfterEndSent() {
        doTestReceiverCloseOrDetachAfterEndSent(true);
    }

    public void doTestReceiverCloseOrDetachAfterEndSent(boolean close) {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.expectAttach().withHandle(0).withName("receiver").withRole(Role.RECEIVER).respond();
        peer.expectEnd().respond();

        // Create the connection and open it, then create a session and a receiver
        // and observe that the receiver doesn't send its detach if the session has
        // already been closed.
        Connection connection = engine.start();
        connection.open();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("receiver");
        receiver.open();

        // Cause an End frame to be sent
        session.close();

        // The sender should not emit an end as the session was closed which implicitly
        // detached the link.
        if (close) {
            receiver.close();
        } else {
            receiver.detach();
        }

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testReceiverDetachAfterCloseSent() {
        doTestReceiverClosedOrDetachedAfterCloseSent(false);
    }

    @Test
    public void testReceiverCloseAfterCloseSent() {
        doTestReceiverClosedOrDetachedAfterCloseSent(true);
    }

    public void doTestReceiverClosedOrDetachedAfterCloseSent(boolean close) {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.expectAttach().withHandle(0).withName("receiver").withRole(Role.RECEIVER).respond();
        peer.expectClose().respond();

        // Create the connection and open it, then create a session and a receiver
        // and observe that the receiver doesn't send its detach if the connection has
        // already been closed.
        Connection connection = engine.start();
        connection.open();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("receiver");
        receiver.open();

        // Cause an Close frame to be sent
        connection.close();

        // The receiver should not emit an detach as the connection was closed which implicitly
        // detached the link.
        if (close) {
            receiver.close();
        } else {
            receiver.detach();
        }

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testReceiverSendsFlowWhenCreditSet() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.expectFlow().withLinkCredit(100);
        peer.expectDetach().respond();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("test");
        receiver.open();
        receiver.addCredit(100);
        receiver.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testReceiverSendsFlowAfterOpenedWhenCreditSetBeforeOpened() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.expectFlow().withLinkCredit(100);
        peer.expectDetach().respond();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("test");
        receiver.addCredit(100);
        receiver.open();
        receiver.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testReceiverSendsFlowAfterConnectionOpenFinallySent() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");

        // Create and open all resources except don't open the connection and then
        // we will observe that the receiver flow doesn't fire until it has sent its
        // attach following the session send its Begin.
        Connection connection = engine.start();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("test");
        receiver.addCredit(1);
        receiver.open();

        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.expectFlow().withLinkCredit(1);

        connection.open();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test //TODO: questionable. If its going to no-op the credit then it should perhaps not do this (open before parent) to begin with, as strange to send the attaches but not credit?
    public void testReceiverOmitsFlowAfterConnectionOpenFinallySentWhenAfterDetached() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        // Create and open all resources except don't open the connection and then
        // we will observe that the receiver flow doesn't fire since the link was
        // detached prior to being able to send any state updates.
        Connection connection = engine.start();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("test");
        receiver.addCredit(1);
        receiver.open();
        receiver.detach();

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.expectDetach().respond();

        connection.open();

        peer.waitForScriptToComplete();

        assertEquals(0, receiver.getCredit());
        assertNull(failure);
    }

    @Test(timeout=20000)
    public void testReceiverDrainAllOutstanding() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("test");
        receiver.open();

        int creditWindow = 100;

        // Add some credit, verify not draining
        Matcher<Boolean> notDrainingMatcher = anyOf(equalTo(false), nullValue());
        peer.expectFlow().withDrain(notDrainingMatcher).withLinkCredit(creditWindow).withDeliveryCount(0);
        receiver.addCredit(creditWindow);

        peer.waitForScriptToComplete();

        // Check that calling drain sends flow, and calls handler on response draining all credit
        AtomicBoolean handlerCalled = new AtomicBoolean();
        receiver.creditStateUpdateHandler(x -> {
            handlerCalled.set(true);
        });

        peer.expectFlow().withDrain(true).withLinkCredit(creditWindow).withDeliveryCount(0)
                         .respond()
                         .withDrain(true).withLinkCredit(0).withDeliveryCount(creditWindow);

        receiver.drain();

        peer.waitForScriptToComplete();
        assertTrue("Handler was not called", handlerCalled.get());

        peer.expectDetach().respond();
        receiver.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testReceiverThrowsOnAddCreditAfterConnectionClosed() throws Exception {
                Engine engine = EngineFactory.PROTON.createNonSaslEngine();

        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.expectClose().respond();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("test");
        receiver.open();
        connection.close();

        try {
            receiver.addCredit(100);
            fail("Should not be able to add credit after connection was closed");
        } catch (IllegalStateException ise) {
        }

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testReceiverThrowsOnAddCreditAfterSessionClosed() throws Exception {
                Engine engine = EngineFactory.PROTON.createNonSaslEngine();

        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.expectEnd().respond();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("test");
        receiver.open();
        session.close();

        try {
            receiver.addCredit(100);
            fail("Should not be able to add credit after session was closed");
        } catch (IllegalStateException ise) {
        }

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testReceiverDispatchesIncomingDelivery() throws Exception {
                Engine engine = EngineFactory.PROTON.createNonSaslEngine();

        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.expectFlow().withLinkCredit(100);
        peer.remoteTransfer().withDeliveryId(0)
                             .withDeliveryTag(new byte[] {0})
                             .withMore(false)
                             .withMessageFormat(0).queue();
        peer.expectDetach().respond();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("test");

        final AtomicBoolean deliveryArrived = new AtomicBoolean();
        final AtomicReference<IncomingDelivery> receivedDelivery = new AtomicReference<>();
        receiver.deliveryReadHandler(delivery -> {
            deliveryArrived.set(true);
            receivedDelivery.set(delivery);
        });
        receiver.open();
        receiver.addCredit(100);
        receiver.close();

        assertTrue("Delivery did not arrive at the receiver", deliveryArrived.get());
        assertFalse("Deliver should not be partial", receivedDelivery.get().isPartial());

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testReceiverSendsDispostionForTransfer() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.expectFlow().withLinkCredit(100);
        peer.remoteTransfer().withDeliveryId(0)
                             .withDeliveryTag(new byte[] {0})
                             .withMore(false)
                             .withMessageFormat(0).queue();
        peer.expectDisposition().withFirst(0)
                                .withSettled(true)
                                .withRole(Role.RECEIVER)
                                .withState(Accepted.getInstance());
        peer.expectDetach().respond();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("test");

        final AtomicBoolean deliveryArrived = new AtomicBoolean();
        final AtomicReference<IncomingDelivery> receivedDelivery = new AtomicReference<>();
        receiver.deliveryReadHandler(delivery -> {
            deliveryArrived.set(true);
            receivedDelivery.set(delivery);

            delivery.disposition(Accepted.getInstance(), true);
        });
        receiver.open();
        receiver.addCredit(100);

        assertTrue("Delivery did not arrive at the receiver", deliveryArrived.get());
        assertFalse("Deliver should not be partial", receivedDelivery.get().isPartial());
        assertFalse(receiver.hasUnsettled());

        receiver.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testReceiverSendsDispostionOnlyOnceForTransfer() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.expectFlow().withLinkCredit(100);
        peer.remoteTransfer().withDeliveryId(0)
                             .withDeliveryTag(new byte[] {0})
                             .withMore(false)
                             .withMessageFormat(0).queue();
        peer.expectDisposition().withFirst(0)
                                .withSettled(true)
                                .withRole(Role.RECEIVER)
                                .withState(Accepted.getInstance());
        peer.expectDetach().respond();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("test");

        final AtomicBoolean deliveryArrived = new AtomicBoolean();
        final AtomicReference<IncomingDelivery> receivedDelivery = new AtomicReference<>();
        receiver.deliveryReadHandler(delivery -> {
            deliveryArrived.set(true);
            receivedDelivery.set(delivery);

            delivery.disposition(Accepted.getInstance(), true);
        });
        receiver.open();
        receiver.addCredit(100);

        assertTrue("Delivery did not arrive at the receiver", deliveryArrived.get());
        assertFalse("Deliver should not be partial", receivedDelivery.get().isPartial());

        // Already settled so this should trigger error
        try {
            receivedDelivery.get().disposition(Released.getInstance(), true);
            fail("Should not be able to set a second disposition");
        } catch (IllegalStateException ise) {
            // Expected that we can't settle twice.
        }

        receiver.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testReceiverSendsUpdatedDispostionsForTransferBeforeSettlement() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.expectFlow().withLinkCredit(100);
        peer.remoteTransfer().withDeliveryId(0)
                             .withDeliveryTag(new byte[] {0})
                             .withMore(false)
                             .withMessageFormat(0).queue();
        peer.expectDisposition().withFirst(0)
                                .withSettled(false)
                                .withRole(Role.RECEIVER)
                                .withState(Accepted.getInstance());
        peer.expectDisposition().withFirst(0)
                                .withSettled(true)
                                .withRole(Role.RECEIVER)
                                .withState(Released.getInstance());
        peer.expectDetach().respond();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("test");

        final AtomicBoolean deliveryArrived = new AtomicBoolean();
        final AtomicReference<IncomingDelivery> receivedDelivery = new AtomicReference<>();
        receiver.deliveryReadHandler(delivery -> {
            deliveryArrived.set(true);
            receivedDelivery.set(delivery);

            delivery.disposition(Accepted.getInstance(), false);
        });
        receiver.open();
        receiver.addCredit(100);

        assertTrue("Delivery did not arrive at the receiver", deliveryArrived.get());
        assertFalse("Deliver should not be partial", receivedDelivery.get().isPartial());
        assertTrue(receiver.hasUnsettled());

        // Second disposition should be sent as we didn't settle previously.
        receivedDelivery.get().disposition(Released.getInstance(), true);

        receiver.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    /**
     * Verify that no Disposition frame is emitted by the Transport should a Delivery
     * have disposition applied after the Close frame was sent.
     */
    @Test
    public void testDispositionNoAllowedAfterCloseSent() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.expectFlow().withLinkCredit(1);
        peer.remoteTransfer().withDeliveryId(0)
                             .withDeliveryTag(new byte[] {0})
                             .withMore(false)
                             .withMessageFormat(0).queue();
        peer.expectClose();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("test");

        final AtomicBoolean deliveryArrived = new AtomicBoolean();
        final AtomicReference<IncomingDelivery> receivedDelivery = new AtomicReference<>();
        receiver.deliveryReadHandler(delivery -> {
            deliveryArrived.set(true);
            receivedDelivery.set(delivery);
        });

        receiver.open();
        receiver.addCredit(1);

        assertTrue("Delivery did not arrive at the receiver", deliveryArrived.get());
        assertFalse("Deliver should not be partial", receivedDelivery.get().isPartial());

        connection.close();

        try {
            receivedDelivery.get().disposition(Released.getInstance());
            fail("Should not be able to set a disposition after the connection was closed");
        } catch (IllegalStateException ise) {}

        try {
            receivedDelivery.get().disposition(Released.getInstance(), true);
            fail("Should not be able to set a disposition after the connection was closed");
        } catch (IllegalStateException ise) {}

        try {
            receivedDelivery.get().settle();
            fail("Should not be able to settle after the connection was closed");
        } catch (IllegalStateException ise) {}

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testReceiverReportsDeliveryUpdatedOnDisposition() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.expectFlow().withLinkCredit(100);
        peer.remoteTransfer().withDeliveryId(0)
                             .withDeliveryTag(new byte[] {0})
                             .withMore(false)
                             .withMessageFormat(0).queue();
        peer.remoteDisposition().withSettled(true)
                                .withRole(Role.SENDER)
                                .withState(Accepted.getInstance())
                                .withFirst(0).queue();
        peer.expectDetach().respond();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("test");

        final AtomicBoolean deliveryArrived = new AtomicBoolean();
        final AtomicReference<IncomingDelivery> receivedDelivery = new AtomicReference<>();
        receiver.deliveryReadHandler(delivery -> {
            deliveryArrived.set(true);
            receivedDelivery.set(delivery);
        });

        final AtomicBoolean deliveryUpdatedAndSettled = new AtomicBoolean();
        final AtomicReference<IncomingDelivery> updatedDelivery = new AtomicReference<>();
        receiver.deliveryStateUpdatedHandler(delivery -> {
            if (delivery.isRemotelySettled()) {
                deliveryUpdatedAndSettled.set(true);
            }

            updatedDelivery.set(delivery);
        });

        receiver.open();
        receiver.addCredit(100);
        receiver.close();

        assertTrue("Delivery did not arrive at the receiver", deliveryArrived.get());
        assertFalse("Delivery should not be partial", receivedDelivery.get().isPartial());
        assertFalse("Delivery should not be partial", updatedDelivery.get().isPartial());
        assertTrue("Delivery should have been updated to settled", deliveryUpdatedAndSettled.get());
        assertSame("Delivery should be same object as first received", receivedDelivery.get(), updatedDelivery.get());

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testReceiverReportsDeliveryUpdatedOnDispositionForMultipleTransfers() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.expectFlow().withLinkCredit(2);
        peer.remoteTransfer().withDeliveryId(0)
                             .withDeliveryTag(new byte[] {0})
                             .withMore(false)
                             .withMessageFormat(0).queue();
        peer.remoteTransfer().withDeliveryId(1)
                             .withDeliveryTag(new byte[] {1})
                             .withMore(false)
                             .withMessageFormat(0).queue();
        peer.remoteDisposition().withSettled(true)
                                .withRole(Role.SENDER)
                                .withState(Accepted.getInstance())
                                .withFirst(0)
                                .withLast(1).queue();
        peer.expectDetach().respond();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("test");

        final AtomicInteger deliveryCounter = new AtomicInteger();
        final AtomicInteger dispositionCounter = new AtomicInteger();

        final ArrayList<IncomingDelivery> deliveries = new ArrayList<>();

        receiver.deliveryReadHandler(delivery -> {
            deliveryCounter.incrementAndGet();
        });

        receiver.deliveryStateUpdatedHandler(delivery -> {
            if (delivery.isRemotelySettled()) {
                dispositionCounter.incrementAndGet();
                deliveries.add(delivery);
            }
        });

        receiver.open();
        receiver.addCredit(2);
        receiver.close();

        assertEquals("Not all deliveries arrived", 2, deliveryCounter.get());
        assertEquals("Not all deliveries received dispositions", 2, deliveries.size());

        byte deliveryTag = 0;

        for (IncomingDelivery delivery : deliveries) {
            assertEquals("Delivery not updated in correct order", deliveryTag++, delivery.getTag().tagBuffer().getByte(0));
            assertTrue("Delivery should be marked as remotely setted", delivery.isRemotelySettled());
        }

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testReceiverReportsDeliveryUpdatedNextFrameForMultiFrameTransfer() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        String text = "test-string-for-split-frame-delivery";
        byte[] encoded = text.getBytes(StandardCharsets.UTF_8);

        Binary first = new Binary(encoded, 0, encoded.length / 2);
        Binary second = new Binary(encoded, encoded.length / 2, encoded.length - (encoded.length / 2));

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.expectFlow().withLinkCredit(2);
        peer.remoteTransfer().withDeliveryId(0)
                             .withDeliveryTag(new byte[] {0})
                             .withMore(true)
                             .withMessageFormat(0)
                             .withBody().withData(first).also().queue();
        peer.remoteTransfer().withDeliveryId(0)
                             .withDeliveryTag(new byte[] {0})
                             .withMore(false)
                             .withMessageFormat(0)
                             .withBody().withData(second).also().queue();
        peer.expectDetach().respond();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("test");

        final AtomicBoolean deliveryArrived = new AtomicBoolean();
        final AtomicReference<IncomingDelivery> receivedDelivery = new AtomicReference<>();
        final AtomicInteger deliverReads = new AtomicInteger();

        receiver.deliveryReadHandler(delivery -> {
            deliveryArrived.set(true);
            receivedDelivery.set(delivery);
            deliverReads.incrementAndGet();
        });

        receiver.open();
        receiver.addCredit(2);

        assertTrue("Delivery did not arrive at the receiver", deliveryArrived.get());
        assertFalse("Delivery should not be partial", receivedDelivery.get().isPartial());
        assertEquals("Deliver should have been read twice for two transfers", 2, deliverReads.get());
        assertSame("Delivery should be same object as first received", receivedDelivery.get(), receivedDelivery.get());

        ProtonBuffer payload = receivedDelivery.get().readAll();

        assertNotNull(payload);

        // We are cheating a bit here as this ins't how the encoding would normally work.
        Data section1 = decoder.readObject(payload, decoderState, Data.class);
        Data section2 = decoder.readObject(payload, decoderState, Data.class);

        Binary data1 = section1.getValue();
        Binary data2 = section2.getValue();

        ProtonBuffer combined = ProtonByteBufferAllocator.DEFAULT.allocate(encoded.length);

        combined.writeBytes(data1.asByteBuffer());
        combined.writeBytes(data2.asByteBuffer());

        assertEquals("Encoded and Decoded strings don't match", text, combined.toString(StandardCharsets.UTF_8));

        receiver.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testReceiverReportsUpdateWhenLastFrameOfMultiFrameTransferHasNoPayload() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        String text = "test-string-for-split-frame-delivery";
        byte[] encoded = text.getBytes(StandardCharsets.UTF_8);

        Binary first = new Binary(encoded, 0, encoded.length / 2);
        Binary second = new Binary(encoded, encoded.length / 2, encoded.length - (encoded.length / 2));

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.expectFlow().withLinkCredit(1);
        peer.remoteTransfer().withDeliveryId(0)
                             .withDeliveryTag(new byte[] {0})
                             .withMore(true)
                             .withMessageFormat(0)
                             .withBody().withData(first).also().queue();
        peer.remoteTransfer().withDeliveryId(0)
                             .withDeliveryTag(new byte[] {0})
                             .withMore(true)
                             .withMessageFormat(0)
                             .withBody().withData(second).also().queue();
        peer.remoteTransfer().withDeliveryId(0)
                             .withMore(true)
                             .withMessageFormat(0)
                             .queue();
        peer.remoteTransfer().withDeliveryId(0)
                             .withMore(false)
                             .withMessageFormat(0)
                             .queue();
        peer.expectDetach().respond();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("test");

        final AtomicBoolean deliveryArrived = new AtomicBoolean();
        final AtomicReference<IncomingDelivery> receivedDelivery = new AtomicReference<>();
        final AtomicInteger deliverReads = new AtomicInteger();

        receiver.deliveryReadHandler(delivery -> {
            deliveryArrived.set(true);
            receivedDelivery.set(delivery);
            deliverReads.incrementAndGet();
        });

        receiver.open();
        receiver.addCredit(1);

        assertTrue("Delivery did not arrive at the receiver", deliveryArrived.get());
        assertFalse("Delivery should not be partial", receivedDelivery.get().isPartial());
        assertEquals("Deliver should have been read twice for two transfers", 4, deliverReads.get());
        assertSame("Delivery should be same object as first received", receivedDelivery.get(), receivedDelivery.get());

        ProtonBuffer payload = receivedDelivery.get().readAll();

        assertNotNull(payload);

        // We are cheating a bit here as this ins't how the encoding would normally work.
        Data section1 = decoder.readObject(payload, decoderState, Data.class);
        Data section2 = decoder.readObject(payload, decoderState, Data.class);

        Binary data1 = section1.getValue();
        Binary data2 = section2.getValue();

        ProtonBuffer combined = ProtonByteBufferAllocator.DEFAULT.allocate(encoded.length);

        combined.writeBytes(data1.asByteBuffer());
        combined.writeBytes(data2.asByteBuffer());

        assertEquals("Encoded and Decoded strings don't match", text, combined.toString(StandardCharsets.UTF_8));

        receiver.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testMultiplexMultiFrameDeliveriesOnSingleSessionIncoming() throws Exception {
        doMultiplexMultiFrameDeliveryOnSingleSessionIncomingTestImpl(true);
    }

    @Test
    public void testMultiplexMultiFrameDeliveryOnSingleSessionIncoming() throws Exception {
        doMultiplexMultiFrameDeliveryOnSingleSessionIncomingTestImpl(false);
    }

    private void doMultiplexMultiFrameDeliveryOnSingleSessionIncomingTestImpl(boolean bothDeliveriesMultiFrame) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().withHandle(0).withName("receiver-1").respond();
        peer.expectAttach().withHandle(1).withName("receiver-2").respond();
        peer.expectFlow().withHandle(0).withLinkCredit(5);
        peer.expectFlow().withHandle(1).withLinkCredit(5);

        Connection connection = engine.start();
        connection.open();
        Session session = connection.session();
        session.open();

        Receiver receiver1 = session.receiver("receiver-1");
        Receiver receiver2 = session.receiver("receiver-2");

        final AtomicReference<IncomingDelivery> receivedDelivery1 = new AtomicReference<>();
        final AtomicReference<IncomingDelivery> receivedDelivery2 = new AtomicReference<>();

        final AtomicBoolean delivery1Updated = new AtomicBoolean();
        final AtomicBoolean delivery2Updated = new AtomicBoolean();

        final String deliveryTag1 = "tag1";
        final String deliveryTag2 = "tag2";

        final byte[] payload1 = new byte[] { 1, 1 };
        final byte[] payload2 = new byte[] { 2, 2 };

        // Receiver 1 handlers for delivery processing.
        receiver1.deliveryReadHandler(delivery -> {
            receivedDelivery1.set(delivery);
        });
        receiver1.deliveryStateUpdatedHandler(delivery -> {
            delivery1Updated.set(true);
        });

        // Receiver 2 handlers for delivery processing.
        receiver2.deliveryReadHandler(delivery -> {
            receivedDelivery2.set(delivery);
        });
        receiver2.deliveryStateUpdatedHandler(delivery -> {
            delivery2Updated.set(true);
        });

        receiver1.open();
        receiver2.open();

        receiver1.addCredit(5);
        receiver2.addCredit(5);

        assertNull("Should not have any delivery data yet on receiver 1", receivedDelivery1.get());
        assertNull("Should not have any delivery date yet on receiver 2", receivedDelivery2.get());

        peer.remoteTransfer().withDeliveryId(0)
                             .withHandle(0)
                             .withDeliveryTag(deliveryTag1.getBytes(StandardCharsets.UTF_8))
                             .withMore(true)
                             .withMessageFormat(0)
                             .withPayload(payload1).now();
        peer.remoteTransfer().withDeliveryId(1)
                             .withHandle(1)
                             .withDeliveryTag(deliveryTag2.getBytes(StandardCharsets.UTF_8))
                             .withMore(bothDeliveriesMultiFrame)
                             .withMessageFormat(0)
                             .withPayload(payload2).now();

        assertNotNull("Should have a delivery event on receiver 1", receivedDelivery1.get());
        assertNotNull("Should have a delivery event on receiver 2", receivedDelivery2.get());

        assertTrue("Delivery on Receiver 1 Should not be complete", receivedDelivery1.get().isPartial());
        if (bothDeliveriesMultiFrame) {
            assertTrue("Delivery on Receiver 2 Should be complete", receivedDelivery2.get().isPartial());
        } else {
            assertFalse("Delivery on Receiver 2 Should not be complete", receivedDelivery2.get().isPartial());
        }

        peer.remoteTransfer().withDeliveryId(0)
                             .withHandle(0)
                             .withDeliveryTag(deliveryTag1.getBytes(StandardCharsets.UTF_8))
                             .withMore(false)
                             .withMessageFormat(0)
                             .withPayload(payload1).now();
        if (bothDeliveriesMultiFrame) {
            peer.remoteTransfer().withDeliveryId(1)
                                 .withHandle(1)
                                 .withDeliveryTag(deliveryTag2.getBytes(StandardCharsets.UTF_8))
                                 .withMore(false)
                                 .withMessageFormat(0)
                                 .withPayload(payload2).now();
        }

        assertFalse("Delivery on Receiver 1 Should be complete", receivedDelivery1.get().isPartial());
        assertFalse("Delivery on Receiver 2 Should be complete", receivedDelivery2.get().isPartial());

        peer.expectDisposition().withFirst(1)
                                .withSettled(true)
                                .withRole(Role.RECEIVER)
                                .withState(Accepted.getInstance());
        peer.expectDisposition().withFirst(0)
                                .withSettled(true)
                                .withRole(Role.RECEIVER)
                                .withState(Accepted.getInstance());

        assertArrayEquals(deliveryTag1.getBytes(StandardCharsets.UTF_8), receivedDelivery1.get().getTag().tagBuffer().getArray());
        assertArrayEquals(deliveryTag2.getBytes(StandardCharsets.UTF_8), receivedDelivery2.get().getTag().tagBuffer().getArray());

        ProtonBuffer payloadBuffer1 = receivedDelivery1.get().readAll();
        ProtonBuffer payloadBuffer2 = receivedDelivery2.get().readAll();

        assertEquals("Received 1 payload size is wrong", payload1.length * 2, payloadBuffer1.getReadableBytes());
        assertEquals("Received 2 payload size is wrong", payload2.length * (bothDeliveriesMultiFrame ? 2 : 1), payloadBuffer2.getReadableBytes());

        receivedDelivery2.get().disposition(Accepted.getInstance(), true);
        receivedDelivery1.get().disposition(Accepted.getInstance(), true);

        // Check post conditions and done.
        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test
    public void testReceiverDeliveryIdTrackingHandlesAbortedDelivery() {
        // Check aborted=true, more=false, settled=true.
        doTestReceiverDeliveryIdTrackingHandlesAbortedDelivery(false, true);
        // Check aborted=true, more=false, settled=unset(false)
        // Aborted overrides settled not being set.
        doTestReceiverDeliveryIdTrackingHandlesAbortedDelivery(false, null);
        // Check aborted=true, more=false, settled=false
        // Aborted overrides settled being explicitly false.
        doTestReceiverDeliveryIdTrackingHandlesAbortedDelivery(false, false);
        // Check aborted=true, more=true, settled=true
        // Aborted overrides the more=true.
        doTestReceiverDeliveryIdTrackingHandlesAbortedDelivery(true, true);
        // Check aborted=true, more=true, settled=unset(false)
        // Aborted overrides the more=true, and settled being unset.
        doTestReceiverDeliveryIdTrackingHandlesAbortedDelivery(true, null);
        // Check aborted=true, more=true, settled=false
        // Aborted overrides the more=true, and settled explicitly false.
        doTestReceiverDeliveryIdTrackingHandlesAbortedDelivery(true, false);
    }

    private void doTestReceiverDeliveryIdTrackingHandlesAbortedDelivery(boolean setMoreOnAbortedTransfer, Boolean setSettledOnAbortedTransfer) {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.expectFlow().withLinkCredit(2);

        Connection connection = engine.start();
        connection.open();
        Session session = connection.session();
        session.open();

        Receiver receiver = session.receiver("receiver");
        receiver.addCredit(2);

        final AtomicReference<IncomingDelivery> receivedDelivery = new AtomicReference<>();
        final AtomicInteger deliveryCounter = new AtomicInteger();
        final AtomicBoolean deliveryUpdated = new AtomicBoolean();
        final byte[] payload = new byte[] { 1 };

        // Receiver 1 handlers for delivery processing.
        receiver.deliveryReadHandler(delivery -> {
            deliveryCounter.incrementAndGet();
            receivedDelivery.set(delivery);
        });
        receiver.deliveryStateUpdatedHandler(delivery -> {
            deliveryUpdated.set(true);
        });

        receiver.open();

        assertNull("Should not have any delivery data yet on receiver 1", receivedDelivery.get());
        assertEquals("Should not have any delivery data yet on receiver 1", 0, deliveryCounter.get());
        assertFalse("Should not have any delivery data yet on receiver 1", deliveryUpdated.get());

        // First chunk indicates more to come.
        peer.remoteTransfer().withDeliveryId(0)
                             .withDeliveryTag(new byte[] {1})
                             .withMore(true)
                             .withMessageFormat(0)
                             .withPayload(payload).now();

        assertNotNull("Should have delivery data on receiver", receivedDelivery.get());
        assertEquals("Should have delivery data on receiver", 1, deliveryCounter.get());
        assertFalse("Should not have any delivery updates yet on receiver", deliveryUpdated.get());

        // Second chunk indicates more to come as a twist but also signals aborted.
        peer.remoteTransfer().withDeliveryId(0)
                             .withSettled(setSettledOnAbortedTransfer)
                             .withMore(setMoreOnAbortedTransfer)
                             .withAborted(true)
                             .withMessageFormat(0)
                             .withPayload(payload).now();

        assertNotNull("Should have delivery data on receiver", receivedDelivery.get());
        assertEquals("Should have delivery data on receiver", 2, deliveryCounter.get());
        assertFalse("Should not have a delivery updates on receiver", deliveryUpdated.get());
        assertTrue("Should now show that delivery is aborted", receivedDelivery.get().isAborted());
        assertTrue("Should now show that delivery is remotely settled", receivedDelivery.get().isRemotelySettled());
        assertNull("Aboarted Delivery should discard read bytes", receivedDelivery.get().readAll());

        // Another delivery now which should arrive just fine, no further frames on this one.
        peer.remoteTransfer().withDeliveryId(1)
                             .withDeliveryTag(new byte[] {2})
                             .withMore(false)
                             .withMessageFormat(0)
                             .withPayload(payload).now();

        assertNotNull("Should have delivery data on receiver", receivedDelivery.get());
        assertEquals("Should have delivery data on receiver", 3, deliveryCounter.get());
        assertFalse("Should not have a delivery updates on receiver", deliveryUpdated.get());
        assertFalse("Should now show that delivery is not aborted", receivedDelivery.get().isAborted());
        assertEquals("Should have delivery tagged as two", 2, receivedDelivery.get().getTag().tagBuffer().getByte(0));

        // Test that delivery count updates correctly on next flow
        peer.expectFlow().withLinkCredit(10).withDeliveryCount(2);

        receiver.addCredit(10);

        peer.expectDetach().respond();
        peer.expectEnd().respond();
        peer.expectClose().respond();

        receiver.close();
        session.close();
        connection.close();

        // Check post conditions and done.
        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test
    public void testDeliveryWithIdOmittedOnContinuationTransfers() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().withHandle(0).withName("receiver-1").respond();
        peer.expectAttach().withHandle(1).withName("receiver-2").respond();
        peer.expectFlow().withHandle(0).withLinkCredit(5);
        peer.expectFlow().withHandle(1).withLinkCredit(5);

        Connection connection = engine.start();
        connection.open();
        Session session = connection.session();
        session.open();

        Receiver receiver1 = session.receiver("receiver-1");
        Receiver receiver2 = session.receiver("receiver-2");

        final AtomicReference<IncomingDelivery> receivedDelivery1 = new AtomicReference<>();
        final AtomicReference<IncomingDelivery> receivedDelivery2 = new AtomicReference<>();

        final AtomicInteger receiver1Transfers = new AtomicInteger();
        final AtomicInteger receiver2Transfers = new AtomicInteger();

        final AtomicBoolean delivery1Updated = new AtomicBoolean();
        final AtomicBoolean delivery2Updated = new AtomicBoolean();

        final String deliveryTag1 = "tag1";
        final String deliveryTag2 = "tag2";

        // Receiver 1 handlers for delivery processing.
        receiver1.deliveryReadHandler(delivery -> {
            receivedDelivery1.set(delivery);
            receiver1Transfers.incrementAndGet();
        });
        receiver1.deliveryStateUpdatedHandler(delivery -> {
            delivery1Updated.set(true);
            receiver1Transfers.incrementAndGet();
        });

        // Receiver 2 handlers for delivery processing.
        receiver2.deliveryReadHandler(delivery -> {
            receivedDelivery2.set(delivery);
            receiver2Transfers.incrementAndGet();
        });
        receiver2.deliveryStateUpdatedHandler(delivery -> {
            delivery2Updated.set(true);
            receiver2Transfers.incrementAndGet();
        });

        receiver1.open();
        receiver2.open();

        receiver1.addCredit(5);
        receiver2.addCredit(5);

        assertNull("Should not have any delivery data yet on receiver 1", receivedDelivery1.get());
        assertNull("Should not have any delivery date yet on receiver 2", receivedDelivery2.get());
        assertEquals("Receiver 1 should not have any transfers yet", 0, receiver1Transfers.get());
        assertEquals("Receiver 2 should not have any transfers yet", 0, receiver2Transfers.get());

        peer.remoteTransfer().withDeliveryId(0)
                             .withHandle(0)
                             .withDeliveryTag(deliveryTag1.getBytes(StandardCharsets.UTF_8))
                             .withMore(true)
                             .withMessageFormat(0)
                             .withPayload(new byte[] {1}).now();
        peer.remoteTransfer().withDeliveryId(1)
                             .withHandle(1)
                             .withDeliveryTag(deliveryTag2.getBytes(StandardCharsets.UTF_8))
                             .withMore(true)
                             .withMessageFormat(0)
                             .withPayload(new byte[] {10}).now();

        assertNotNull("Should have a delivery event on receiver 1", receivedDelivery1.get());
        assertNotNull("Should have a delivery event on receiver 2", receivedDelivery2.get());
        assertEquals("Receiver 1 should have 1 transfers", 1, receiver1Transfers.get());
        assertEquals("Receiver 2 should have 1 transfers", 1, receiver2Transfers.get());
        assertNotSame(receivedDelivery1.get(), receivedDelivery2.get());

        peer.remoteTransfer().withHandle(1)
                             .withDeliveryTag(deliveryTag2.getBytes(StandardCharsets.UTF_8))
                             .withMore(true)
                             .withMessageFormat(0)
                             .withPayload(new byte[] {11}).now();
        peer.remoteTransfer().withHandle(0)
                             .withDeliveryTag(deliveryTag1.getBytes(StandardCharsets.UTF_8))
                             .withMore(true)
                             .withMessageFormat(0)
                             .withPayload(new byte[] {2}).now();

        assertNotNull("Should have a delivery event on receiver 1", receivedDelivery1.get());
        assertNotNull("Should have a delivery event on receiver 2", receivedDelivery2.get());
        assertEquals("Receiver 1 should have 2 transfers", 2, receiver1Transfers.get());
        assertEquals("Receiver 2 should have 2 transfers", 2, receiver2Transfers.get());
        assertNotSame(receivedDelivery1.get(), receivedDelivery2.get());

        peer.remoteTransfer().withHandle(0)
                             .withDeliveryTag(deliveryTag1.getBytes(StandardCharsets.UTF_8))
                             .withMore(false)
                             .withMessageFormat(0)
                             .withPayload(new byte[] {3}).now();
        peer.remoteTransfer().withHandle(1)
                             .withDeliveryTag(deliveryTag2.getBytes(StandardCharsets.UTF_8))
                             .withMore(true)
                             .withMessageFormat(0)
                             .withPayload(new byte[] {12}).now();

        assertNotNull("Should have a delivery event on receiver 1", receivedDelivery1.get());
        assertNotNull("Should have a delivery event on receiver 2", receivedDelivery2.get());
        assertEquals("Receiver 1 should have 3 transfers", 3, receiver1Transfers.get());
        assertEquals("Receiver 2 should have 3 transfers", 3, receiver2Transfers.get());
        assertNotSame(receivedDelivery1.get(), receivedDelivery2.get());

        peer.remoteTransfer().withHandle(1)
                             .withDeliveryTag(deliveryTag2.getBytes(StandardCharsets.UTF_8))
                             .withMore(false)
                             .withMessageFormat(0)
                             .withPayload(new byte[] {13}).now();

        assertNotNull("Should have a delivery event on receiver 1", receivedDelivery1.get());
        assertNotNull("Should have a delivery event on receiver 2", receivedDelivery2.get());
        assertEquals("Receiver 1 should have 3 transfers", 3, receiver1Transfers.get());
        assertEquals("Receiver 2 should have 4 transfers", 4, receiver2Transfers.get());
        assertNotSame(receivedDelivery1.get(), receivedDelivery2.get());
        assertFalse("Delivery on Receiver 1 Should be complete", receivedDelivery1.get().isPartial());
        assertFalse("Delivery on Receiver 2 Should be complete", receivedDelivery2.get().isPartial());

        assertArrayEquals(deliveryTag1.getBytes(StandardCharsets.UTF_8), receivedDelivery1.get().getTag().tagBuffer().getArray());
        assertArrayEquals(deliveryTag2.getBytes(StandardCharsets.UTF_8), receivedDelivery2.get().getTag().tagBuffer().getArray());

        ProtonBuffer delivery1Buffer = receivedDelivery1.get().readAll();
        ProtonBuffer delivery2Buffer = receivedDelivery2.get().readAll();

        for (int i = 1; i < 4; ++i) {
            assertEquals(i, delivery1Buffer.readByte());
        }

        for (int i = 10; i < 14; ++i) {
            assertEquals(i, delivery2Buffer.readByte());
        }

        assertNull(receivedDelivery1.get().readAll());
        assertNull(receivedDelivery2.get().readAll());

        peer.expectDetach().withHandle(0).respond();
        peer.expectDetach().withHandle(1).respond();
        peer.expectEnd().respond();
        peer.expectClose().respond();

        receiver1.close();
        receiver2.close();
        session.close();
        connection.close();

        // Check post conditions and done.
        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test
    public void testDeliveryIdThresholdsAndWraps() {
        // Check start from 0
        doDeliveryIdThresholdsWrapsTestImpl(UnsignedInteger.ZERO, UnsignedInteger.ONE, UnsignedInteger.valueOf(2));
        // Check run up to max-int (interesting boundary for underlying impl)
        doDeliveryIdThresholdsWrapsTestImpl(UnsignedInteger.valueOf(Integer.MAX_VALUE - 2), UnsignedInteger.valueOf(Integer.MAX_VALUE -1), UnsignedInteger.valueOf(Integer.MAX_VALUE));
        // Check crossing from signed range value into unsigned range value (interesting boundary for underlying impl)
        long maxIntAsLong = Integer.MAX_VALUE;
        doDeliveryIdThresholdsWrapsTestImpl(UnsignedInteger.valueOf(maxIntAsLong), UnsignedInteger.valueOf(maxIntAsLong + 1L), UnsignedInteger.valueOf(maxIntAsLong + 2L));
        // Check run up to max-uint
        doDeliveryIdThresholdsWrapsTestImpl(UnsignedInteger.valueOf(0xFFFFFFFFL - 2), UnsignedInteger.valueOf(0xFFFFFFFFL - 1), UnsignedInteger.MAX_VALUE);
        // Check wrapping from max unsigned value back to min(/0).
        doDeliveryIdThresholdsWrapsTestImpl(UnsignedInteger.MAX_VALUE, UnsignedInteger.ZERO, UnsignedInteger.ONE);
    }

    private void doDeliveryIdThresholdsWrapsTestImpl(UnsignedInteger deliveryId1, UnsignedInteger deliveryId2, UnsignedInteger deliveryId3) {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond().withNextOutgoingId(deliveryId1.intValue());
        peer.expectAttach().respond();
        peer.expectFlow().withLinkCredit(5);

        Connection connection = engine.start();
        connection.open();
        Session session = connection.session();
        session.open();

        Receiver receiver = session.receiver("receiver");

        final AtomicReference<IncomingDelivery> receivedDelivery1 = new AtomicReference<>();
        final AtomicReference<IncomingDelivery> receivedDelivery2 = new AtomicReference<>();
        final AtomicReference<IncomingDelivery> receivedDelivery3 = new AtomicReference<>();

        final AtomicInteger deliveryCounter = new AtomicInteger();

        final String deliveryTag1 = "tag1";
        final String deliveryTag2 = "tag2";
        final String deliveryTag3 = "tag3";

        // Receiver handlers for delivery processing.
        receiver.deliveryReadHandler(delivery -> {
            switch (deliveryCounter.get()) {
                case 0:
                    receivedDelivery1.set(delivery);
                    break;
                case 1:
                    receivedDelivery2.set(delivery);
                    break;
                case 2:
                    receivedDelivery3.set(delivery);
                    break;
                default:
                    break;
            }
            deliveryCounter.incrementAndGet();
        });
        receiver.deliveryStateUpdatedHandler(delivery -> {
            deliveryCounter.incrementAndGet();
        });

        receiver.open();
        receiver.addCredit(5);

        assertNull("Should not have received delivery 1", receivedDelivery1.get());
        assertNull("Should not have received delivery 2", receivedDelivery2.get());
        assertNull("Should not have received delivery 3", receivedDelivery3.get());
        assertEquals("Receiver should not have any deliveries yet", 0, deliveryCounter.get());

        peer.remoteTransfer().withDeliveryId(deliveryId1.intValue())
                             .withDeliveryTag(deliveryTag1.getBytes(StandardCharsets.UTF_8))
                             .withMessageFormat(0)
                             .withPayload(new byte[] {1}).now();

        assertNotNull("Should have received delivery 1", receivedDelivery1.get());
        assertNull("Should not have received delivery 2", receivedDelivery2.get());
        assertNull("Should not have received delivery 3", receivedDelivery3.get());
        assertEquals("Receiver should have 1 deliveries now", 1, deliveryCounter.get());

        peer.remoteTransfer().withDeliveryId(deliveryId2.intValue())
                             .withDeliveryTag(deliveryTag2.getBytes(StandardCharsets.UTF_8))
                             .withMessageFormat(0)
                             .withPayload(new byte[] {2}).now();

        assertNotNull("Should have received delivery 1", receivedDelivery1.get());
        assertNotNull("Should have received delivery 2", receivedDelivery2.get());
        assertNull("Should not have received delivery 3", receivedDelivery3.get());
        assertEquals("Receiver should have 2 deliveries now", 2, deliveryCounter.get());

        peer.remoteTransfer().withDeliveryId(deliveryId3.intValue())
                             .withDeliveryTag(deliveryTag3.getBytes(StandardCharsets.UTF_8))
                             .withMessageFormat(0)
                             .withPayload(new byte[] {3}).now();

        assertNotNull("Should have received delivery 1", receivedDelivery1.get());
        assertNotNull("Should have received delivery 2", receivedDelivery2.get());
        assertNotNull("Should have received delivery 3", receivedDelivery3.get());
        assertEquals("Receiver should have 3 deliveries now", 3, deliveryCounter.get());

        assertNotSame("delivery duplicate detected", receivedDelivery1.get(), receivedDelivery2.get());
        assertNotSame("delivery duplicate detected", receivedDelivery2.get(), receivedDelivery3.get());
        assertNotSame("delivery duplicate detected", receivedDelivery1.get(), receivedDelivery3.get());

        // Verify deliveries arrived with expected payload
        assertArrayEquals(deliveryTag1.getBytes(StandardCharsets.UTF_8), receivedDelivery1.get().getTag().tagBuffer().getArray());
        assertArrayEquals(deliveryTag2.getBytes(StandardCharsets.UTF_8), receivedDelivery2.get().getTag().tagBuffer().getArray());
        assertArrayEquals(deliveryTag3.getBytes(StandardCharsets.UTF_8), receivedDelivery3.get().getTag().tagBuffer().getArray());

        ProtonBuffer delivery1Buffer = receivedDelivery1.get().readAll();
        ProtonBuffer delivery2Buffer = receivedDelivery2.get().readAll();
        ProtonBuffer delivery3Buffer = receivedDelivery3.get().readAll();

        assertEquals("Delivery 1 payload not as expected", 1, delivery1Buffer.readByte());
        assertEquals("Delivery 2 payload not as expected", 2, delivery2Buffer.readByte());
        assertEquals("Delivery 3 payload not as expected", 3, delivery3Buffer.readByte());

        peer.expectDetach().respond();
        peer.expectEnd().respond();
        peer.expectClose().respond();

        receiver.close();
        session.close();
        connection.close();

        // Check post conditions and done.
        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test(timeout = 20000)
    public void testReceiverFlowSentAfterAttachWrittenWhenCreditPrefilled() throws Exception {
        doTestReceiverFlowSentAfterAttachWritten(true);
    }

    @Test(timeout = 20000)
    public void testReceiverFlowSentAfterAttachWrittenWhenCreditAddedBeforeAttachResponse() throws Exception {
        doTestReceiverFlowSentAfterAttachWritten(false);
    }

    private void doTestReceiverFlowSentAfterAttachWritten(boolean creditBeforeOpen) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();

        Connection connection = engine.start().open();
        Session session = connection.session().open();

        Receiver receiver = session.receiver("receiver");

        if (creditBeforeOpen) {
            // Add credit before open, no frame should be written until opened.
            receiver.addCredit(5);
        }

        // Expect attach but don't respond to observe that flow is sent regardless.
        peer.waitForScriptToComplete();
        peer.expectAttach();
        peer.expectFlow().withLinkCredit(5).withDeliveryCount(nullValue());

        receiver.open();

        if (!creditBeforeOpen) {
            // Add credit after open, frame should be written regardless of no attach response
            receiver.addCredit(5);
        }

        peer.respondToLastAttach().now();
        peer.waitForScriptToComplete();
        peer.expectDetach().respond();
        peer.expectEnd().respond();
        peer.expectClose().respond();

        receiver.detach();
        session.close();
        connection.close();

        // Check post conditions and done.
        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test
    public void testReceiverHandlesDeferredOpenAndBeginAttachResponses() throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        final AtomicBoolean receiverRemotelyOpened = new AtomicBoolean();

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen();
        peer.expectBegin();
        peer.expectAttach().withRole(Role.RECEIVER)
                           .withSource().withDynamic(true)
                           .withAddress((String) null);

        Connection connection = engine.start();

        connection.open();
        Session session = connection.session();
        session.open();

        Receiver receiver = session.receiver("receiver-1");
        receiver.setSource(new Source().setDynamic(true).setAddress(null));
        receiver.openHandler(result -> receiverRemotelyOpened.set(true)).open();

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

        // This should happen after we inject the held open and attach
        peer.expectClose().respond();

        // Inject held responses to get the ball rolling again
        peer.remoteOpen().withOfferedCapabilities("ANONYMOUS_REALY").now();
        peer.respondToLastBegin().now();
        peer.respondToLastAttach().now();

        assertTrue("Receiver remote opened event did not fire", receiverRemotelyOpened.get());
        assertNotNull(receiver.getRemoteSource().getAddress());

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

        Receiver receiver = session.receiver("test");
        receiver.open();

        engine.shutdown();

        // Should clean up and not throw as we knowingly shutdown engine operations.
        receiver.close();
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

        Receiver receiver = session.receiver("test");
        receiver.open();

        engine.engineFailed(new IOException());

        try {
            receiver.close();
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
        receiver.close();
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

    public void doTestCloseOrDetachWithErrorCondition(boolean close) throws Exception {
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

        Receiver receiver = session.receiver("receiver-1");
        receiver.open();
        receiver.setCondition(new ErrorCondition(Symbol.valueOf(condition), description));

        if (close) {
            receiver.close();
        } else {
            receiver.detach();
        }

        connection.close();

        peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
    }

    @Test(timeout = 30000)
    public void testReceiverAddCreditFailsAfterReceiverLocallyClosed() throws Exception {
        doTestReceiverAddCreditFailsWhenLinkIsNotOperable(true, false, false);
    }

    @Test(timeout = 30000)
    public void testReceiverAddCreditFailsAfterReceiverLocallyDetached() throws Exception {
        doTestReceiverAddCreditFailsWhenLinkIsNotOperable(true, false, true);
    }

    @Test(timeout = 30000)
    public void testReceiverAddCreditFailsAfterReceiverRemotelyClosed() throws Exception {
        doTestReceiverAddCreditFailsWhenLinkIsNotOperable(false, true, false);
    }

    @Test(timeout = 30000)
    public void testReceiverAddCreditFailsAfterReceiverRemotelyDetached() throws Exception {
        doTestReceiverAddCreditFailsWhenLinkIsNotOperable(false, true, true);
    }

    @Test(timeout = 30000)
    public void testReceiverAddCreditFailsAfterReceiverFullyClosed() throws Exception {
        doTestReceiverAddCreditFailsWhenLinkIsNotOperable(true, true, false);
    }

    @Test(timeout = 30000)
    public void testReceiverAddCreditFailsAfterReceiverFullyDetached() throws Exception {
        doTestReceiverAddCreditFailsWhenLinkIsNotOperable(true, true, true);
    }

    private void doTestReceiverAddCreditFailsWhenLinkIsNotOperable(boolean localClose, boolean remoteClose, boolean detach) {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();

        Connection connection = engine.start();
        connection.open();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("test");
        receiver.open();

        if (localClose) {
            if (remoteClose) {
                peer.expectDetach().respond();
            } else {
                peer.expectDetach();
            }

            if (detach) {
                receiver.detach();
            } else {
                receiver.close();
            }
        } else if (remoteClose) {
            peer.remoteDetach().withClosed(!detach).now();
        }

        try {
            receiver.addCredit(2);
            fail("Receiver should not allow addCredit to be called");
        } catch (IllegalStateException ise) {
            // Expected
        }

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test(timeout = 30000)
    public void testReceiverAddCreditFailsAfterSessionLocallyClosed() throws Exception {
        doTestReceiverAddCreditFailsWhenSessionNotOperable(true, false);
    }

    @Test(timeout = 30000)
    public void testReceiverAddCreditFailsAfterSessionRemotelyClosed() throws Exception {
        doTestReceiverAddCreditFailsWhenSessionNotOperable(false, true);
    }

    @Test(timeout = 30000)
    public void testReceiverAddCreditFailsAfterSessionFullyClosed() throws Exception {
        doTestReceiverAddCreditFailsWhenSessionNotOperable(true, true);
    }

    private void doTestReceiverAddCreditFailsWhenSessionNotOperable(boolean localClose, boolean remoteClose) {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();

        Connection connection = engine.start();
        connection.open();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("test");
        receiver.open();

        if (localClose) {
            if (remoteClose) {
                peer.expectEnd().respond();
            } else {
                peer.expectEnd();
            }

            session.close();
        } else if (remoteClose) {
            peer.remoteEnd().now();
        }

        try {
            receiver.addCredit(2);
            fail("Receiver should not allow addCredit to be called");
        } catch (IllegalStateException ise) {
            // Expected
        }

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test(timeout = 30000)
    public void testReceiverAddCreditFailsAfterConnectionLocallyClosed() throws Exception {
        doTestReceiverAddCreditFailsWhenConnectionNotOperable(true, false);
    }

    @Test(timeout = 30000)
    public void testReceiverAddCreditFailsAfterConnectionRemotelyClosed() throws Exception {
        doTestReceiverAddCreditFailsWhenConnectionNotOperable(false, true);
    }

    @Test(timeout = 30000)
    public void testReceiverAddCreditFailsAfterConnectionFullyClosed() throws Exception {
        doTestReceiverAddCreditFailsWhenConnectionNotOperable(true, true);
    }

    private void doTestReceiverAddCreditFailsWhenConnectionNotOperable(boolean localClose, boolean remoteClose) {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();

        Connection connection = engine.start();
        connection.open();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("test");
        receiver.open();

        if (localClose) {
            if (remoteClose) {
                peer.expectClose().respond();
            } else {
                peer.expectClose();
            }

            connection.close();
        } else if (remoteClose) {
            peer.remoteClose().now();
        }

        try {
            receiver.addCredit(2);
            fail("Receiver should not allow addCredit to be called");
        } catch (IllegalStateException ise) {
            // Expected
        }

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test(timeout = 30000)
    public void testReceiverDispositionFailsAfterReceiverLocallyClosed() throws Exception {
        doTestReceiverDispositionFailsWhenLinkIsNotOperable(true, false, false);
    }

    @Test(timeout = 30000)
    public void testReceiverDispositionFailsAfterReceiverLocallyDetached() throws Exception {
        doTestReceiverDispositionFailsWhenLinkIsNotOperable(true, false, true);
    }

    @Test(timeout = 30000)
    public void testReceiverDispositionFailsAfterReceiverRemotelyClosed() throws Exception {
        doTestReceiverDispositionFailsWhenLinkIsNotOperable(false, true, false);
    }

    @Test(timeout = 30000)
    public void testReceiverDispositionFailsAfterReceiverRemotelyDetached() throws Exception {
        doTestReceiverDispositionFailsWhenLinkIsNotOperable(false, true, true);
    }

    @Test(timeout = 30000)
    public void testReceiverDispositionFailsAfterReceiverFullyClosed() throws Exception {
        doTestReceiverDispositionFailsWhenLinkIsNotOperable(true, true, false);
    }

    @Test(timeout = 30000)
    public void testReceiverDispositionFailsAfterReceiverFullyDetached() throws Exception {
        doTestReceiverDispositionFailsWhenLinkIsNotOperable(true, true, true);
    }

    private void doTestReceiverDispositionFailsWhenLinkIsNotOperable(boolean localClose, boolean remoteClose, boolean detach) {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectBegin().respond();
        peer.expectAttach().respond();

        Connection connection = engine.start();
        connection.open();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("test");
        receiver.open();

        // Should no-op with no deliveries
        receiver.disposition(delivery -> true, Accepted.getInstance(), true);

        if (localClose) {
            if (remoteClose) {
                peer.expectDetach().respond();
            } else {
                peer.expectDetach();
            }

            if (detach) {
                receiver.detach();
            } else {
                receiver.close();
            }
        } else if (remoteClose) {
            peer.remoteDetach().withClosed(!detach).now();
        }

        try {
            receiver.disposition(delivery -> true, Accepted.getInstance(), true);
            fail("Receiver should not allow dispotiion to be called");
        } catch (IllegalStateException ise) {
            // Expected
        }

        peer.waitForScriptToComplete();

        assertNull(failure);
    }
}
