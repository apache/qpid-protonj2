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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.qpid.proton4j.amqp.driver.AMQPTestDriver;
import org.apache.qpid.proton4j.amqp.driver.ScriptWriter;
import org.apache.qpid.proton4j.amqp.messaging.Accepted;
import org.apache.qpid.proton4j.amqp.messaging.Released;
import org.apache.qpid.proton4j.amqp.transport.DeliveryState;
import org.apache.qpid.proton4j.amqp.transport.Role;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.engine.Connection;
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
    public void testSenderOpenAndCloseAreIdempotent() throws Exception {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);
        ScriptWriter script = driver.createScriptWriter();

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().respond().withContainerId("driver");
        script.expectBegin().respond();
        script.expectAttach().respond();
        script.expectDetach().respond();

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

        driver.assertScriptComplete();

        assertNull(failure);
    }

    @Test
    public void testEngineEmitsAttachAfterLocalSenderOpened() throws Exception {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);
        ScriptWriter script = driver.createScriptWriter();

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().respond().withContainerId("driver");
        script.expectBegin().respond();
        script.expectAttach().respond();
        script.expectDetach().respond();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Sender sender = session.sender("test");
        sender.open();
        sender.close();

        driver.assertScriptComplete();

        assertNull(failure);
    }

    @Test
    public void testOpenBeginAttachBeforeRemoteResponds() throws Exception {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);
        // Create the test driver and link it to the engine for output handling.
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);
        ScriptWriter script = driver.createScriptWriter();

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen();
        script.expectBegin();
        script.expectAttach();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Sender sender = session.sender("test");
        sender.open();

        driver.assertScriptComplete();

        assertNull(failure);
    }

    @Test
    public void testSenderFireOpenedEventAfterRemoteAttachArrives() throws Exception {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);
        ScriptWriter script = driver.createScriptWriter();

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().respond().withContainerId("driver");
        script.expectBegin().respond();
        script.expectAttach().respond();
        script.expectDetach().respond();

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

        driver.assertScriptComplete();

        assertNull(failure);
    }

    @Test
    public void testOpenAndCloseMultipleSenders() throws Exception {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);
        ScriptWriter script = driver.createScriptWriter();

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().respond().withContainerId("driver");
        script.expectBegin().respond();
        script.expectAttach().withHandle(0).respond();
        script.expectAttach().withHandle(1).respond();
        script.expectDetach().withHandle(1).respond();
        script.expectDetach().withHandle(0).respond();

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

        driver.assertScriptComplete();

        assertNull(failure);
    }

    @Test
    public void testSenderFireClosedEventAfterRemoteDetachArrives() throws Exception {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);
        ScriptWriter script = driver.createScriptWriter();

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().respond().withContainerId("driver");
        script.expectBegin().respond();
        script.expectAttach().respond();
        script.expectDetach().respond();

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

        driver.assertScriptComplete();

        assertNull(failure);
    }

    @Test
    public void testConnectionSignalsRemoteSenderOpen() throws Exception {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);
        ScriptWriter script = driver.createScriptWriter();

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().respond().withContainerId("driver");
        script.expectBegin().respond();
        script.remoteAttach().withName("sender")
                             .withHandle(0)
                             .withRole(Role.RECEIVER)
                             .withInitialDeliveryCount(0)
                             .onChannel(0).queue();
        script.expectAttach();
        script.expectDetach().respond();

        final AtomicBoolean senderRemotelyOpened = new AtomicBoolean();
        final AtomicReference<Sender> sender = new AtomicReference<>();

        Connection connection = engine.start();

        connection.senderOpenEventHandler(result -> {
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

        driver.assertScriptComplete();

        assertNull(failure);
    }

    @Test
    public void testCannotOpenSenderAfterSessionClosed() throws Exception {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);
        ScriptWriter script = driver.createScriptWriter();

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().respond().withContainerId("driver");
        script.expectBegin().respond();
        script.expectEnd().respond();

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

        driver.assertScriptComplete();

        assertNull(failure);
    }

    @Test
    public void testCannotOpenSenderAfterSessionRemotelyClosed() throws Exception {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);
        ScriptWriter script = driver.createScriptWriter();

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().respond().withContainerId("driver");
        script.expectBegin().respond();
        script.remoteEnd().queue(); // TODO - Last opened is used here, but a thenEnd() on the expect begin would be more clear

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();

        Sender sender = session.sender("test");

        try {
            sender.open();
            fail("Should not be able to open a link from a remotely closed session.");
        } catch (IllegalStateException ise) {}

        sender.close();

        driver.assertScriptComplete();

        assertNull(failure);
    }

    @Test
    public void testGetCurrentDeliveryFromSender() throws Exception {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);
        ScriptWriter script = driver.createScriptWriter();

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().respond().withContainerId("driver");
        script.expectBegin().respond();
        script.expectAttach().withHandle(0).respond();
        script.expectDetach().withHandle(0).respond();

        Connection connection = engine.start();

        connection.open();
        Session session = connection.session();
        session.open();

        Sender sender = session.sender("sender-1");

        sender.open();

        OutgoingDelivery delivery = sender.delivery();
        assertNotNull(delivery);

        assertFalse(delivery.isAborted());
        assertTrue(delivery.isPartial());
        assertFalse(delivery.isSettled());
        assertFalse(delivery.isRemotelySettled());

        // Always return same delivery until completed.
        assertSame(delivery, sender.delivery());

        sender.close();

        driver.assertScriptComplete();

        assertNull(failure);
    }

    @Test
    public void testSenderGetsCreditOnIncomingFlow() throws Exception {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);
        ScriptWriter script = driver.createScriptWriter();

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().respond().withContainerId("driver");
        script.expectBegin().respond();
        script.expectAttach().withRole(Role.SENDER).respond();
        script.remoteFlow().withDeliveryCount(0)
                           .withLinkCredit(10)
                           .withIncomingWindow(1024)
                           .withOutgoingWindow(10)
                           .withNextIncomingId(0)
                           .withNextOutgoingId(1).queue();
        script.expectDetach().withHandle(0).respond();

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

        driver.assertScriptComplete();

        assertNull(failure);
    }

    @Test
    public void testSendSmallPayloadWhenCreditAvailable() throws Exception {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);
        ScriptWriter script = driver.createScriptWriter();

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().respond().withContainerId("driver");
        script.expectBegin().respond();
        script.expectAttach().withRole(Role.SENDER).respond();
        script.remoteFlow().withDeliveryCount(0)     // TODO - Would be nice to automate filling in these
                           .withLinkCredit(10)       //        these bits using last session opened values
                           .withIncomingWindow(1024) //        plus some defaults or generated values.
                           .withOutgoingWindow(10)
                           .withNextIncomingId(0)
                           .withNextOutgoingId(1).queue();
        script.expectTransfer().withHandle(0)
                               .withSettled(false)
                               .withState((DeliveryState) null)
                               .withDeliveryId(0)
                               .withDeliveryTag(new byte[] {0});
        script.expectDetach().withHandle(0).respond();

        Connection connection = engine.start();

        connection.open();
        Session session = connection.session();
        session.open();

        ProtonBuffer payload = ProtonByteBufferAllocator.DEFAULT.wrap(new byte[] {0, 1, 2, 3, 4});

        Sender sender = session.sender("sender-1");

        assertFalse(sender.isSendable());

        sender.sendableEventHandler(handler -> {
            handler.delivery().setTag(new byte[] {0}).writeBytes(payload);
        });

        sender.open();
        sender.close();

        driver.assertScriptComplete();

        assertNull(failure);
    }

    @Test
    public void testSenderSignalsDeliveryUpdatedOnSettled() throws Exception {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);
        ScriptWriter script = driver.createScriptWriter();

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().respond().withContainerId("driver");
        script.expectBegin().respond();
        script.expectAttach().withRole(Role.SENDER).respond();
        script.remoteFlow().withDeliveryCount(0)
                           .withLinkCredit(10)
                           .withIncomingWindow(1024)
                           .withOutgoingWindow(10)
                           .withNextIncomingId(0)
                           .withNextOutgoingId(1).queue();
        script.expectTransfer().withHandle(0)
                               .withSettled(false)
                               .withState((DeliveryState) null)
                               .withDeliveryId(0)
                               .withDeliveryTag(new byte[] {0});
        script.remoteDisposition().withSettled(true)
                                  .withRole(Role.RECEIVER)
                                  .withState(Accepted.getInstance())
                                  .withFirst(0).queue();
        script.expectDetach().withHandle(0).respond();

        Connection connection = engine.start();

        connection.open();
        Session session = connection.session();
        session.open();

        ProtonBuffer payload = ProtonByteBufferAllocator.DEFAULT.wrap(new byte[] {0, 1, 2, 3, 4});

        Sender sender = session.sender("sender-1");

        final AtomicBoolean deliveryUpdatedAndSettled = new AtomicBoolean();
        final AtomicReference<OutgoingDelivery> updatedDelivery = new AtomicReference<>();
        sender.deliveryUpdatedEventHandler(delivery -> {
            if (delivery.isRemotelySettled()) {
                deliveryUpdatedAndSettled.set(true);
            }

            updatedDelivery.set(delivery);
        });

        assertFalse(sender.isSendable());

        sender.sendableEventHandler(handler -> {
            handler.delivery().setTag(new byte[] {0}).writeBytes(payload);
        });

        sender.open();

        assertTrue("Delivery should have been updated and state settled", deliveryUpdatedAndSettled.get());
        assertEquals(Accepted.getInstance(), updatedDelivery.get().getRemoteState());

        sender.close();

        driver.assertScriptComplete();

        assertNull(failure);
    }

    @Test
    public void testOpenSenderBeforeOpenConnection() {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);
        ScriptWriter script = driver.createScriptWriter();

        // Create the connection but don't open, then open a session and a sender and
        // the session begin and sender attach shouldn't go out until the connection
        // is opened locally.
        Connection connection = engine.start();
        Session session = connection.session();
        session.open();
        Sender sender = session.sender("sender");
        sender.open();

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().respond();
        script.expectBegin().respond();
        script.expectAttach().withHandle(0).withName("sender").withRole(Role.SENDER).respond();

        // Now open the connection, expect the Open, Begin, and Attach frames
        connection.open();

        driver.assertScriptComplete();

        assertNull(failure);
    }

    @Test
    public void testOpenSenderBeforeOpenSession() {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);
        ScriptWriter script = driver.createScriptWriter();

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().respond();

        // Create the connection and open it, then create a session and a sender
        // and observe that the sender doesn't send its attach until the session
        // is opened.
        Connection connection = engine.start();
        connection.open();
        Session session = connection.session();
        Sender sender = session.sender("sender");
        sender.open();

        script.expectBegin().respond();
        script.expectAttach().withHandle(0).withName("sender").withRole(Role.SENDER).respond();

        // Now open the session, expect the Begin, and Attach frames
        session.open();

        driver.assertScriptComplete();

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
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);
        ScriptWriter script = driver.createScriptWriter();

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().respond();
        script.expectBegin().respond();
        script.expectAttach().withHandle(0).withName("sender").withRole(Role.SENDER).respond();
        script.expectEnd().respond();

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

        driver.assertScriptComplete();

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
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);
        ScriptWriter script = driver.createScriptWriter();

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().respond();
        script.expectBegin().respond();
        script.expectAttach().withHandle(0).withName("sender").withRole(Role.SENDER).respond();
        script.expectClose().respond();

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

        driver.assertScriptComplete();

        assertNull(failure);
    }

    @Test
    public void testNoDispositionSentAfterDeliverySettledForSender() throws Exception {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);
        ScriptWriter script = driver.createScriptWriter();

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().respond().withContainerId("driver");
        script.expectBegin().respond();
        script.expectAttach().withRole(Role.SENDER).respond();
        script.remoteFlow().withDeliveryCount(0)
                           .withLinkCredit(10)
                           .withIncomingWindow(1024)
                           .withOutgoingWindow(10)
                           .withNextIncomingId(0)
                           .withNextOutgoingId(1).queue();
        script.expectTransfer().withHandle(0)
                               .withSettled(false)
                               .withState((DeliveryState) null)
                               .withDeliveryId(0)
                               .withDeliveryTag(new byte[] {0});
        script.expectDisposition().withFirst(0)
                                  .withSettled(true)
                                  .withState(Accepted.getInstance());
        script.expectDetach().withHandle(0).respond();

        Connection connection = engine.start();

        connection.open();
        Session session = connection.session();
        session.open();

        ProtonBuffer payload = ProtonByteBufferAllocator.DEFAULT.wrap(new byte[] {0, 1, 2, 3, 4});

        Sender sender = session.sender("sender-1");

        final AtomicBoolean deliverySentAfterSenable = new AtomicBoolean();
        sender.sendableEventHandler(handler -> {
            handler.delivery().setTag(new byte[] {0}).writeBytes(payload);
            deliverySentAfterSenable.set(true);
        });

        sender.open();

        assertTrue("Delivery should have been sent after credit arrived", deliverySentAfterSenable.get());

        OutgoingDelivery delivery1 = sender.delivery();
        delivery1.disposition(Accepted.getInstance(), true);
        OutgoingDelivery delivery2 = sender.delivery();
        // TODO - Currently we advance to next only after a settle which isn't really workable
        //        and we need an advance type API to allow sends without settling
        assertNotSame(delivery1, delivery2);
        delivery2.disposition(Released.getInstance(), true);

        sender.close();

        driver.assertScriptComplete();

        assertNull(failure);
    }

    @Ignore("Fails for now since we don't full propagate close to all resources.")
    @Test
    public void testSenderCannotSendAfterConnectionClosed() throws Exception {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);
        ScriptWriter script = driver.createScriptWriter();

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().respond().withContainerId("driver");
        script.expectBegin().respond();
        script.expectAttach().withRole(Role.SENDER).respond();
        script.remoteFlow().withDeliveryCount(0)
                           .withLinkCredit(10)
                           .withIncomingWindow(1024)
                           .withOutgoingWindow(10)
                           .withNextIncomingId(0)
                           .withNextOutgoingId(1).queue();
        script.expectClose().respond();

        Connection connection = engine.start();

        connection.open();
        Session session = connection.session();
        session.open();

        Sender sender = session.sender("sender-1");

        assertFalse(sender.isSendable());

        OutgoingDelivery delivery = sender.delivery();
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

        driver.assertScriptComplete();

        assertNull(failure);
    }
}
