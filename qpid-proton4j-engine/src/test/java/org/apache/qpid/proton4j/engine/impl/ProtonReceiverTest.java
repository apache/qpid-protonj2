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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.UnsignedInteger;
import org.apache.qpid.proton4j.amqp.driver.AMQPTestDriver;
import org.apache.qpid.proton4j.amqp.driver.ScriptWriter;
import org.apache.qpid.proton4j.amqp.messaging.Accepted;
import org.apache.qpid.proton4j.amqp.messaging.Data;
import org.apache.qpid.proton4j.amqp.messaging.Released;
import org.apache.qpid.proton4j.amqp.transport.Role;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.engine.Connection;
import org.apache.qpid.proton4j.engine.IncomingDelivery;
import org.apache.qpid.proton4j.engine.Receiver;
import org.apache.qpid.proton4j.engine.Session;
import org.junit.Test;

/**
 * Test the {@link ProtonReceiver}
 */
public class ProtonReceiverTest extends ProtonEngineTestSupport {

    @Test
    public void testReceiverOpenAndCloseAreIdempotent() throws Exception {
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
        Receiver receiver = session.receiver("test");
        receiver.open();

        // Should not emit another attach frame
        receiver.open();

        receiver.close();

        // Should not emit another detach frame
        receiver.close();

        driver.assertScriptComplete();

        assertNull(failure);
    }

    @Test
    public void testEngineEmitsAttachAfterLocalReceiverOpened() throws Exception {
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
        Receiver receiver = session.receiver("test");
        receiver.open();
        receiver.close();

        driver.assertScriptComplete();

        assertNull(failure);
    }

    @Test
    public void testOpenBeginAttachBeforeRemoteResponds() throws Exception {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);
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
        Receiver receiver = session.receiver("test");
        receiver.open();

        driver.assertScriptComplete();

        assertNull(failure);
    }

    @Test
    public void testReceiverFireOpenedEventAfterRemoteAttachArrives() throws Exception {
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

        driver.assertScriptComplete();

        assertNull(failure);
    }

    @Test
    public void testReceiverFireClosedEventAfterRemoteDetachArrives() throws Exception {
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

        driver.assertScriptComplete();

        assertNull(failure);
    }

    @Test
    public void testOpenAndCloseMultipleReceivers() throws Exception {
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

        Receiver receiver1 = session.receiver("receiver-1");
        receiver1.open();
        Receiver receiver2 = session.receiver("receiver-2");
        receiver2.open();

        // Close in reverse order
        receiver2.close();
        receiver1.close();

        driver.assertScriptComplete();

        assertNull(failure);
    }

    @Test
    public void testConnectionSignalsRemoteReceiverOpen() throws Exception {
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
                             .withRole(Role.SENDER)
                             .withInitialDeliveryCount(0).queue();
        script.expectAttach();
        script.expectDetach().respond();

        final AtomicBoolean receiverRemotelyOpened = new AtomicBoolean();
        final AtomicReference<Receiver> receiver = new AtomicReference<>();

        Connection connection = engine.start();

        connection.receiverOpenEventHandler(result -> {
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

        Receiver receiver = session.receiver("test");

        session.close();

        try {
            receiver.open();
            fail("Should not be able to open a link from a closed session.");
        } catch (IllegalStateException ise) {}

        receiver.close();

        driver.assertScriptComplete();

        assertNull(failure);
    }

    @Test
    public void testCannotOpenReceiverAfterSessionRemotelyClosed() throws Exception {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);
        ScriptWriter script = driver.createScriptWriter();

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().respond().withContainerId("driver");
        script.expectBegin().respond();
        script.remoteEnd().queue(); // TODO This would be more fluent if there was a thenEnd() on the expect

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();

        Receiver receiver = session.receiver("test");

        try {
            receiver.open();
            fail("Should not be able to open a link from a remotely closed session.");
        } catch (IllegalStateException ise) {}

        receiver.close();

        driver.assertScriptComplete();

        assertNull(failure);
    }

    @Test
    public void testOpenReceiverBeforeOpenConnection() {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);
        ScriptWriter script = driver.createScriptWriter();

        // Create the connection but don't open, then open a session and a receiver and
        // the session begin and receiver attach shouldn't go out until the connection
        // is opened locally.
        Connection connection = engine.start();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("receiver");
        receiver.open();

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().respond();
        script.expectBegin().respond();
        script.expectAttach().withHandle(0).withName("receiver").withRole(Role.RECEIVER).respond();

        // Now open the connection, expect the Open, Begin, and Attach frames
        connection.open();

        driver.assertScriptComplete();

        assertNull(failure);
    }

    @Test
    public void testOpenReceiverBeforeOpenSession() {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);
        ScriptWriter script = driver.createScriptWriter();

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().respond();

        // Create the connection and open it, then create a session and a receiver
        // and observe that the receiver doesn't send its attach until the session
        // is opened.
        Connection connection = engine.start();
        connection.open();
        Session session = connection.session();
        Receiver receiver = session.receiver("receiver");
        receiver.open();

        script.expectBegin().respond();
        script.expectAttach().withHandle(0).withName("receiver").withRole(Role.RECEIVER).respond();

        // Now open the session, expect the Begin, and Attach frames
        session.open();

        driver.assertScriptComplete();

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
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);
        ScriptWriter script = driver.createScriptWriter();

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().respond();
        script.expectBegin().respond();
        script.expectAttach().withHandle(0).withName("receiver").withRole(Role.RECEIVER).respond();
        script.expectEnd().respond();

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

        driver.assertScriptComplete();

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
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);
        ScriptWriter script = driver.createScriptWriter();

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().respond();
        script.expectBegin().respond();
        script.expectAttach().withHandle(0).withName("receiver").withRole(Role.RECEIVER).respond();
        script.expectClose().respond();

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

        driver.assertScriptComplete();

        assertNull(failure);
    }

    @Test
    public void testReceiverSendsFlowWhenCreditSet() throws Exception {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);
        ScriptWriter script = driver.createScriptWriter();

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().respond().withContainerId("driver");
        script.expectBegin().respond();
        script.expectAttach().respond();
        script.expectFlow().withLinkCredit(100);
        script.expectDetach().respond();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("test");
        receiver.open();
        receiver.setCredit(100);
        receiver.close();

        driver.assertScriptComplete();

        assertNull(failure);
    }

    @Test
    public void testReceiverSendsFlowAfterOpenedWhenCreditSetBeforeOpened() throws Exception {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);
        ScriptWriter script = driver.createScriptWriter();

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().respond().withContainerId("driver");
        script.expectBegin().respond();
        script.expectAttach().respond();
        script.expectFlow().withLinkCredit(100);
        script.expectDetach().respond();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("test");
        receiver.setCredit(100);
        receiver.open();
        receiver.close();

        driver.assertScriptComplete();

        assertNull(failure);
    }

    @Test
    public void testReceiverSendsFlowAfterConnectionOpenFinallySent() throws Exception {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);
        ScriptWriter script = driver.createScriptWriter();

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().respond().withContainerId("driver");

        // Create and open all resources except don't open the connection and then
        // we will observe that the receiver flow doesn't fire until it has sent its
        // attach following the session send its Begin.
        Connection connection = engine.start();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("test");
        receiver.setCredit(1);
        receiver.open();

        // TODO - Another test with a detach before open when flow was called.

        script.expectBegin().respond();
        script.expectAttach().respond();
        script.expectFlow().withLinkCredit(1);

        connection.open();

        driver.assertScriptComplete();

        assertNull(failure);
    }

    @Test
    public void testReceiverFailsOnFlowAfterConnectionClosed() throws Exception {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);
        ScriptWriter script = driver.createScriptWriter();

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().respond().withContainerId("driver");
        script.expectBegin().respond();
        script.expectAttach().respond();
        script.expectClose().respond();

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
            receiver.setCredit(100);
            fail("Should not be able to flow credit after connection was closed");
        } catch (IllegalStateException ise) {
        }

        driver.assertScriptComplete();

        assertNull(failure);
    }

    @Test
    public void testReceiverFailsOnFlowAfterSessionClosed() throws Exception {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);
        ScriptWriter script = driver.createScriptWriter();

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().respond().withContainerId("driver");
        script.expectBegin().respond();
        script.expectAttach().respond();
        script.expectEnd().respond();

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
            receiver.setCredit(100);
            fail("Should not be able to flow credit after connection was closed");
        } catch (IllegalStateException ise) {
        }

        driver.assertScriptComplete();

        assertNull(failure);
    }

    @Test
    public void testReceiverDispatchesIncomingDelivery() throws Exception {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);
        ScriptWriter script = driver.createScriptWriter();

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().respond().withContainerId("driver");
        script.expectBegin().respond();
        script.expectAttach().respond();
        script.expectFlow().withLinkCredit(100);
        script.remoteTransfer().withDeliveryId(0)
                               .withDeliveryTag(new byte[] {0})
                               .withMore(false)
                               .withMessageFormat(0).queue();
        script.expectDetach().respond();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("test");

        final AtomicBoolean deliveryArrived = new AtomicBoolean();
        final AtomicReference<IncomingDelivery> receivedDelivery = new AtomicReference<>();
        receiver.deliveryReceivedEventHandler(delivery -> {
            deliveryArrived.set(true);
            receivedDelivery.set(delivery);
        });
        receiver.open();
        receiver.setCredit(100);
        receiver.close();

        assertTrue("Delivery did not arrive at the receiver", deliveryArrived.get());
        assertFalse("Deliver should not be partial", receivedDelivery.get().isPartial());

        driver.assertScriptComplete();

        assertNull(failure);
    }

    @Test
    public void testReceiverSendsDispostionForTransfer() throws Exception {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);
        ScriptWriter script = driver.createScriptWriter();

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().respond().withContainerId("driver");
        script.expectBegin().respond();
        script.expectAttach().respond();
        script.expectFlow().withLinkCredit(100);
        script.remoteTransfer().withDeliveryId(0)
                               .withDeliveryTag(new byte[] {0})
                               .withMore(false)
                               .withMessageFormat(0).queue();
        script.expectDisposition().withFirst(0)
                                  .withSettled(true)
                                  .withRole(Role.RECEIVER)
                                  .withState(Accepted.getInstance());
        script.expectDetach().respond();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("test");

        final AtomicBoolean deliveryArrived = new AtomicBoolean();
        final AtomicReference<IncomingDelivery> receivedDelivery = new AtomicReference<>();
        receiver.deliveryReceivedEventHandler(delivery -> {
            deliveryArrived.set(true);
            receivedDelivery.set(delivery);

            delivery.disposition(Accepted.getInstance(), true);
        });
        receiver.open();
        receiver.setCredit(100);

        assertTrue("Delivery did not arrive at the receiver", deliveryArrived.get());
        assertFalse("Deliver should not be partial", receivedDelivery.get().isPartial());

        receiver.close();

        driver.assertScriptComplete();

        assertNull(failure);
    }

    @Test
    public void testReceiverSendsDispostionOnlyOnceForTransfer() throws Exception {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);
        ScriptWriter script = driver.createScriptWriter();

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().respond().withContainerId("driver");
        script.expectBegin().respond();
        script.expectAttach().respond();
        script.expectFlow().withLinkCredit(100);
        script.remoteTransfer().withDeliveryId(0)
                               .withDeliveryTag(new byte[] {0})
                               .withMore(false)
                               .withMessageFormat(0).queue();
        script.expectDisposition().withFirst(0)
                                  .withSettled(true)
                                  .withRole(Role.RECEIVER)
                                  .withState(Accepted.getInstance());
        script.expectDetach().respond();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("test");

        final AtomicBoolean deliveryArrived = new AtomicBoolean();
        final AtomicReference<IncomingDelivery> receivedDelivery = new AtomicReference<>();
        receiver.deliveryReceivedEventHandler(delivery -> {
            deliveryArrived.set(true);
            receivedDelivery.set(delivery);

            delivery.disposition(Accepted.getInstance(), true);
        });
        receiver.open();
        receiver.setCredit(100);

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

        driver.assertScriptComplete();

        assertNull(failure);
    }

    @Test
    public void testReceiverSendsUpdatedDispostionsForTransferBeforeSettlement() throws Exception {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);
        ScriptWriter script = driver.createScriptWriter();

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().respond().withContainerId("driver");
        script.expectBegin().respond();
        script.expectAttach().respond();
        script.expectFlow().withLinkCredit(100);
        script.remoteTransfer().withDeliveryId(0)
                               .withDeliveryTag(new byte[] {0})
                               .withMore(false)
                               .withMessageFormat(0).queue();
        script.expectDisposition().withFirst(0)
                                  .withSettled(false)
                                  .withRole(Role.RECEIVER)
                                  .withState(Accepted.getInstance());
        script.expectDisposition().withFirst(0)
                                  .withSettled(true)
                                  .withRole(Role.RECEIVER)
                                  .withState(Released.getInstance());
        script.expectDetach().respond();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("test");

        final AtomicBoolean deliveryArrived = new AtomicBoolean();
        final AtomicReference<IncomingDelivery> receivedDelivery = new AtomicReference<>();
        receiver.deliveryReceivedEventHandler(delivery -> {
            deliveryArrived.set(true);
            receivedDelivery.set(delivery);

            delivery.disposition(Accepted.getInstance(), false);
        });
        receiver.open();
        receiver.setCredit(100);

        assertTrue("Delivery did not arrive at the receiver", deliveryArrived.get());
        assertFalse("Deliver should not be partial", receivedDelivery.get().isPartial());

        // Second disposition should be sent as we didn't settle previously.
        receivedDelivery.get().disposition(Released.getInstance(), true);

        receiver.close();

        driver.assertScriptComplete();

        assertNull(failure);
    }

    /**
     * Verify that no Disposition frame is emitted by the Transport should a Delivery
     * have disposition applied after the Close frame was sent.
     */
    @Test
    public void testDispositionNoAllowedAfterCloseSent() {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);
        ScriptWriter script = driver.createScriptWriter();

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().respond().withContainerId("driver");
        script.expectBegin().respond();
        script.expectAttach().respond();
        script.expectFlow().withLinkCredit(1);
        script.remoteTransfer().withDeliveryId(0)
                               .withDeliveryTag(new byte[] {0})
                               .withMore(false)
                               .withMessageFormat(0).queue();
        script.expectClose();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("test");

        final AtomicBoolean deliveryArrived = new AtomicBoolean();
        final AtomicReference<IncomingDelivery> receivedDelivery = new AtomicReference<>();
        receiver.deliveryReceivedEventHandler(delivery -> {
            deliveryArrived.set(true);
            receivedDelivery.set(delivery);
        });

        receiver.open();
        receiver.setCredit(1);

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

        driver.assertScriptComplete();

        assertNull(failure);
    }

    @Test
    public void testReceiverReportsDeliveryUpdatedOnDisposition() throws Exception {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);
        ScriptWriter script = driver.createScriptWriter();

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().respond().withContainerId("driver");
        script.expectBegin().respond();
        script.expectAttach().respond();
        script.expectFlow().withLinkCredit(100);
        script.remoteTransfer().withDeliveryId(0)
                               .withDeliveryTag(new byte[] {0})
                               .withMore(false)
                               .withMessageFormat(0).queue();
        script.remoteDisposition().withSettled(true)
                                  .withRole(Role.SENDER)
                                  .withState(Accepted.getInstance())
                                  .withFirst(0).queue();
        script.expectDetach().respond();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("test");

        final AtomicBoolean deliveryArrived = new AtomicBoolean();
        final AtomicReference<IncomingDelivery> receivedDelivery = new AtomicReference<>();
        receiver.deliveryReceivedEventHandler(delivery -> {
            deliveryArrived.set(true);
            receivedDelivery.set(delivery);
        });

        final AtomicBoolean deliveryUpdatedAndSettled = new AtomicBoolean();
        final AtomicReference<IncomingDelivery> updatedDelivery = new AtomicReference<>();
        receiver.deliveryUpdatedEventHandler(delivery -> {
            if (delivery.isRemotelySettled()) {
                deliveryUpdatedAndSettled.set(true);
            }

            updatedDelivery.set(delivery);
        });

        receiver.open();
        receiver.setCredit(100);
        receiver.close();

        assertTrue("Delivery did not arrive at the receiver", deliveryArrived.get());
        assertFalse("Delivery should not be partial", receivedDelivery.get().isPartial());
        assertFalse("Delivery should not be partial", updatedDelivery.get().isPartial());
        assertTrue("Delivery should have been updated to settled", deliveryUpdatedAndSettled.get());
        assertSame("Delivery should be same object as first received", receivedDelivery.get(), updatedDelivery.get());

        driver.assertScriptComplete();

        assertNull(failure);
    }

    @Test
    public void testReceiverReportsDeliveryUpdatedOnDispositionForMultipleTransfers() throws Exception {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);
        ScriptWriter script = driver.createScriptWriter();

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().respond().withContainerId("driver");
        script.expectBegin().respond();
        script.expectAttach().respond();
        script.expectFlow().withLinkCredit(2);
        script.remoteTransfer().withDeliveryId(0)
                               .withDeliveryTag(new byte[] {0})
                               .withMore(false)
                               .withMessageFormat(0).queue();
        script.remoteTransfer().withDeliveryId(1)
                               .withDeliveryTag(new byte[] {1})
                               .withMore(false)
                               .withMessageFormat(0).queue();
        script.remoteDisposition().withSettled(true)
                                  .withRole(Role.SENDER)
                                  .withState(Accepted.getInstance())
                                  .withFirst(0)
                                  .withLast(1).queue();
        script.expectDetach().respond();

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

        receiver.deliveryReceivedEventHandler(delivery -> {
            deliveryCounter.incrementAndGet();
        });

        receiver.deliveryUpdatedEventHandler(delivery -> {
            if (delivery.isRemotelySettled()) {
                dispositionCounter.incrementAndGet();
                deliveries.add(delivery);
            }
        });

        receiver.open();
        receiver.setCredit(2);
        receiver.close();

        assertEquals("Not all deliveries arrived", 2, deliveryCounter.get());
        assertEquals("Not all deliveries received dispositions", 2, deliveries.size());

        byte deliveryTag = 0;

        for (IncomingDelivery delivery : deliveries) {
            assertEquals("Delivery not updated in correct order", deliveryTag++, delivery.getTag()[0]);
            assertTrue("Delivery should be marked as remotely setted", delivery.isRemotelySettled());
        }

        driver.assertScriptComplete();

        assertNull(failure);
    }

    @Test
    public void testReceiverReportsDeliveryUpdatedNextFrameForMultiFrameTransfer() throws Exception {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);
        ScriptWriter script = driver.createScriptWriter();

        String text = "test-string-for-split-frame-delivery";
        byte[] encoded = text.getBytes(StandardCharsets.UTF_8);

        Binary first = new Binary(encoded, 0, encoded.length / 2);
        Binary second = new Binary(encoded, encoded.length / 2, encoded.length - (encoded.length / 2));

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().respond().withContainerId("driver");
        script.expectBegin().respond();
        script.expectAttach().respond();
        script.expectFlow().withLinkCredit(2);
        script.remoteTransfer().withDeliveryId(0)
                               .withDeliveryTag(new byte[] {0})
                               .withMore(true)
                               .withMessageFormat(0)
                               .withBody().withData(first).also().queue();
        script.remoteTransfer().withDeliveryId(0)
                               .withDeliveryTag(new byte[] {0})
                               .withMore(false)
                               .withMessageFormat(0)
                               .withBody().withData(second).also().queue();
        script.expectDetach().respond();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.open();
        Session session = connection.session();
        session.open();
        Receiver receiver = session.receiver("test");

        final AtomicBoolean deliveryArrived = new AtomicBoolean();
        final AtomicReference<IncomingDelivery> receivedDelivery = new AtomicReference<>();
        receiver.deliveryReceivedEventHandler(delivery -> {
            deliveryArrived.set(true);
            receivedDelivery.set(delivery);
        });

        final AtomicReference<IncomingDelivery> updatedDelivery = new AtomicReference<>();
        receiver.deliveryUpdatedEventHandler(delivery -> {
            updatedDelivery.set(delivery);
        });

        receiver.open();
        receiver.setCredit(2);

        assertTrue("Delivery did not arrive at the receiver", deliveryArrived.get());
        assertFalse("Delivery should not be partial", receivedDelivery.get().isPartial());
        assertFalse("Delivery should not be partial", updatedDelivery.get().isPartial());
        assertSame("Delivery should be same object as first received", receivedDelivery.get(), updatedDelivery.get());

        ProtonBuffer payload = updatedDelivery.get().readAll();

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

        driver.assertScriptComplete();

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
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);
        ScriptWriter script = driver.createScriptWriter();

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().respond().withContainerId("driver");
        script.expectBegin().respond();
        script.expectAttach().withHandle(0).withName("receiver-1").respond();
        script.expectAttach().withHandle(1).withName("receiver-2").respond();
        script.expectFlow().withHandle(0).withLinkCredit(5);
        script.expectFlow().withHandle(1).withLinkCredit(5);

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
        receiver1.deliveryReceivedEventHandler(delivery -> {
            receivedDelivery1.set(delivery);
        });
        receiver1.deliveryUpdatedEventHandler(delivery -> {
            delivery1Updated.set(true);
        });

        // Receiver 2 handlers for delivery processing.
        receiver2.deliveryReceivedEventHandler(delivery -> {
            receivedDelivery2.set(delivery);
        });
        receiver2.deliveryUpdatedEventHandler(delivery -> {
            delivery2Updated.set(true);
        });

        receiver1.open();
        receiver2.open();

        receiver1.setCredit(5);
        receiver2.setCredit(5);

        assertNull("Should not have any delivery data yet on receiver 1", receivedDelivery1.get());
        assertNull("Should not have any delivery date yet on receiver 2", receivedDelivery2.get());

        script.remoteTransfer().withDeliveryId(0)
                               .withHandle(0)
                               .withDeliveryTag(deliveryTag1.getBytes(StandardCharsets.UTF_8))
                               .withMore(true)
                               .withMessageFormat(0)
                               .withPayload(payload1).now();
        script.remoteTransfer().withDeliveryId(1)
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

        script.remoteTransfer().withDeliveryId(0)
                               .withHandle(0)
                               .withDeliveryTag(deliveryTag1.getBytes(StandardCharsets.UTF_8))
                               .withMore(false)
                               .withMessageFormat(0)
                               .withPayload(payload1).now();
        if (bothDeliveriesMultiFrame) {
            script.remoteTransfer().withDeliveryId(1)
                                   .withHandle(1)
                                   .withDeliveryTag(deliveryTag2.getBytes(StandardCharsets.UTF_8))
                                   .withMore(false)
                                   .withMessageFormat(0)
                                   .withPayload(payload2).now();
        }

        assertFalse("Delivery on Receiver 1 Should be complete", receivedDelivery1.get().isPartial());
        assertFalse("Delivery on Receiver 2 Should be complete", receivedDelivery2.get().isPartial());

        script.expectDisposition().withFirst(1)
                                  .withSettled(true)
                                  .withRole(Role.RECEIVER)
                                  .withState(Accepted.getInstance());
        script.expectDisposition().withFirst(0)
                                  .withSettled(true)
                                  .withRole(Role.RECEIVER)
                                  .withState(Accepted.getInstance());

        assertArrayEquals(deliveryTag1.getBytes(StandardCharsets.UTF_8), receivedDelivery1.get().getTag());
        assertArrayEquals(deliveryTag2.getBytes(StandardCharsets.UTF_8), receivedDelivery2.get().getTag());

        ProtonBuffer payloadBuffer1 = receivedDelivery1.get().readAll();
        ProtonBuffer payloadBuffer2 = receivedDelivery2.get().readAll();

        assertEquals("Received 1 payload size is wrong", payload1.length * 2, payloadBuffer1.getReadableBytes());
        assertEquals("Received 2 payload size is wrong", payload2.length * (bothDeliveriesMultiFrame ? 2 : 1), payloadBuffer2.getReadableBytes());

        receivedDelivery2.get().disposition(Accepted.getInstance(), true);
        receivedDelivery1.get().disposition(Accepted.getInstance(), true);

        // Check post conditions and done.
        driver.assertScriptComplete();
        assertNull(failure);
    }

    @Test
    public void testReceiverDeliveryIdTrackingHandlesAbortedDelivery() {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);
        ScriptWriter script = driver.createScriptWriter();

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().respond().withContainerId("driver");
        script.expectBegin().respond();
        script.expectAttach().respond();
        script.expectFlow().withLinkCredit(5);

        Connection connection = engine.start();
        connection.open();
        Session session = connection.session();
        session.open();

        Receiver receiver = session.receiver("receiver");
        receiver.setCredit(5);

        final AtomicReference<IncomingDelivery> receivedDelivery = new AtomicReference<>();
        final AtomicInteger deliveryCounter = new AtomicInteger();
        final AtomicBoolean deliveryUpdated = new AtomicBoolean();
        final byte[] payload = new byte[] { 1 };

        // Receiver 1 handlers for delivery processing.
        receiver.deliveryReceivedEventHandler(delivery -> {
            deliveryCounter.incrementAndGet();
            receivedDelivery.set(delivery);
        });
        receiver.deliveryUpdatedEventHandler(delivery -> {
            deliveryUpdated.set(true);
        });

        receiver.open();

        assertNull("Should not have any delivery data yet on receiver 1", receivedDelivery.get());
        assertEquals("Should not have any delivery data yet on receiver 1", 0, deliveryCounter.get());
        assertFalse("Should not have any delivery data yet on receiver 1", deliveryUpdated.get());

        // First chunk indicates more to come.
        script.remoteTransfer().withDeliveryId(0)
                               .withDeliveryTag(new byte[] {1})
                               .withMore(true)
                               .withMessageFormat(0)
                               .withPayload(payload).now();

        assertNotNull("Should have delivery data on receiver", receivedDelivery.get());
        assertEquals("Should have delivery data on receiver", 1, deliveryCounter.get());
        assertFalse("Should not have any delivery updates yet on receiver", deliveryUpdated.get());

        // Second chunk indicates more to come as a twist but also signals aborted.
        script.remoteTransfer().withDeliveryId(0)
                               .withMore(true)
                               .withAborted(true)
                               .withMessageFormat(0)
                               .withPayload(payload).now();

        assertNotNull("Should have delivery data on receiver", receivedDelivery.get());
        assertEquals("Should have delivery data on receiver", 1, deliveryCounter.get());
        assertTrue("Should have a delivery updates on receiver", deliveryUpdated.get());
        assertTrue("Should now show that delivery is aborted", receivedDelivery.get().isAborted());
        assertTrue("Should now show that delivery is remotely settled", receivedDelivery.get().isRemotelySettled());

        // TODO - At this point any bytes sent in the aborted delivery should be discarded and
        //        session window should no longer track them
        // assertNull(receivedDelivery.get().readAll());

        // Another delivery now which should arrive just fine, no further frames on this one.
        script.remoteTransfer().withDeliveryId(1)
                               .withDeliveryTag(new byte[] {2})
                               .withMore(false)
                               .withMessageFormat(0)
                               .withPayload(payload).now();

        assertNotNull("Should have delivery data on receiver", receivedDelivery.get());
        assertEquals("Should have delivery data on receiver", 2, deliveryCounter.get());
        assertTrue("Should have a delivery updates on receiver", deliveryUpdated.get());
        assertFalse("Should now show that delivery is not aborted", receivedDelivery.get().isAborted());
        assertEquals("Should have delivery tagged as two", 2, receivedDelivery.get().getTag()[0]);

        script.expectDetach().respond();
        script.expectEnd().respond();
        script.expectClose().respond();

        receiver.close();
        session.close();
        connection.close();

        // Check post conditions and done.
        driver.assertScriptComplete();
        assertNull(failure);
    }

    @Test
    public void testDeliveryWithIdOmittedOnContinuationTransfers() {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);
        ScriptWriter script = driver.createScriptWriter();

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().respond().withContainerId("driver");
        script.expectBegin().respond();
        script.expectAttach().withHandle(0).withName("receiver-1").respond();
        script.expectAttach().withHandle(1).withName("receiver-2").respond();
        script.expectFlow().withHandle(0).withLinkCredit(5);
        script.expectFlow().withHandle(1).withLinkCredit(5);

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
        receiver1.deliveryReceivedEventHandler(delivery -> {
            receivedDelivery1.set(delivery);
            receiver1Transfers.incrementAndGet();
        });
        receiver1.deliveryUpdatedEventHandler(delivery -> {
            delivery1Updated.set(true);
            receiver1Transfers.incrementAndGet();
        });

        // Receiver 2 handlers for delivery processing.
        receiver2.deliveryReceivedEventHandler(delivery -> {
            receivedDelivery2.set(delivery);
            receiver2Transfers.incrementAndGet();
        });
        receiver2.deliveryUpdatedEventHandler(delivery -> {
            delivery2Updated.set(true);
            receiver2Transfers.incrementAndGet();
        });

        receiver1.open();
        receiver2.open();

        receiver1.setCredit(5);
        receiver2.setCredit(5);

        assertNull("Should not have any delivery data yet on receiver 1", receivedDelivery1.get());
        assertNull("Should not have any delivery date yet on receiver 2", receivedDelivery2.get());
        assertEquals("Receiver 1 should not have any transfers yet", 0, receiver1Transfers.get());
        assertEquals("Receiver 2 should not have any transfers yet", 0, receiver2Transfers.get());

        script.remoteTransfer().withDeliveryId(0)
                               .withHandle(0)
                               .withDeliveryTag(deliveryTag1.getBytes(StandardCharsets.UTF_8))
                               .withMore(true)
                               .withMessageFormat(0)
                               .withPayload(new byte[] {1}).now();
        script.remoteTransfer().withDeliveryId(1)
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

        script.remoteTransfer().withHandle(1)
                               .withDeliveryTag(deliveryTag2.getBytes(StandardCharsets.UTF_8))
                               .withMore(true)
                               .withMessageFormat(0)
                               .withPayload(new byte[] {11}).now();
        script.remoteTransfer().withHandle(0)
                               .withDeliveryTag(deliveryTag1.getBytes(StandardCharsets.UTF_8))
                               .withMore(true)
                               .withMessageFormat(0)
                               .withPayload(new byte[] {2}).now();

        assertNotNull("Should have a delivery event on receiver 1", receivedDelivery1.get());
        assertNotNull("Should have a delivery event on receiver 2", receivedDelivery2.get());
        assertEquals("Receiver 1 should have 2 transfers", 2, receiver1Transfers.get());
        assertEquals("Receiver 2 should have 2 transfers", 2, receiver2Transfers.get());
        assertNotSame(receivedDelivery1.get(), receivedDelivery2.get());

        script.remoteTransfer().withHandle(0)
                               .withDeliveryTag(deliveryTag1.getBytes(StandardCharsets.UTF_8))
                               .withMore(false)
                               .withMessageFormat(0)
                               .withPayload(new byte[] {3}).now();
        script.remoteTransfer().withHandle(1)
                               .withDeliveryTag(deliveryTag2.getBytes(StandardCharsets.UTF_8))
                               .withMore(true)
                               .withMessageFormat(0)
                               .withPayload(new byte[] {12}).now();

        assertNotNull("Should have a delivery event on receiver 1", receivedDelivery1.get());
        assertNotNull("Should have a delivery event on receiver 2", receivedDelivery2.get());
        assertEquals("Receiver 1 should have 3 transfers", 3, receiver1Transfers.get());
        assertEquals("Receiver 2 should have 3 transfers", 3, receiver2Transfers.get());
        assertNotSame(receivedDelivery1.get(), receivedDelivery2.get());

        script.remoteTransfer().withHandle(1)
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

        assertArrayEquals(deliveryTag1.getBytes(StandardCharsets.UTF_8), receivedDelivery1.get().getTag());
        assertArrayEquals(deliveryTag2.getBytes(StandardCharsets.UTF_8), receivedDelivery2.get().getTag());

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

        script.expectDetach().withHandle(0).respond();
        script.expectDetach().withHandle(1).respond();
        script.expectEnd().respond();
        script.expectClose().respond();

        receiver1.close();
        receiver2.close();
        session.close();
        connection.close();

        // Check post conditions and done.
        driver.assertScriptComplete();
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
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);
        ScriptWriter script = driver.createScriptWriter();

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().respond();
        script.expectBegin().respond().withNextOutgoingId(deliveryId1.intValue());
        script.expectAttach().respond();
        script.expectFlow().withLinkCredit(5);

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
        receiver.deliveryReceivedEventHandler(delivery -> {
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
        receiver.deliveryUpdatedEventHandler(delivery -> {
            deliveryCounter.incrementAndGet();
        });

        receiver.open();
        receiver.setCredit(5);

        assertNull("Should not have received delivery 1", receivedDelivery1.get());
        assertNull("Should not have received delivery 2", receivedDelivery2.get());
        assertNull("Should not have received delivery 3", receivedDelivery3.get());
        assertEquals("Receiver should not have any deliveries yet", 0, deliveryCounter.get());

        script.remoteTransfer().withDeliveryId(deliveryId1.intValue())
                               .withDeliveryTag(deliveryTag1.getBytes(StandardCharsets.UTF_8))
                               .withMessageFormat(0)
                               .withPayload(new byte[] {1}).now();

        assertNotNull("Should have received delivery 1", receivedDelivery1.get());
        assertNull("Should not have received delivery 2", receivedDelivery2.get());
        assertNull("Should not have received delivery 3", receivedDelivery3.get());
        assertEquals("Receiver should not have any deliveries yet", 1, deliveryCounter.get());

        script.remoteTransfer().withDeliveryId(deliveryId2.intValue())
                               .withDeliveryTag(deliveryTag2.getBytes(StandardCharsets.UTF_8))
                               .withMessageFormat(0)
                               .withPayload(new byte[] {2}).now();

        assertNotNull("Should have received delivery 1", receivedDelivery1.get());
        assertNotNull("Should have received delivery 2", receivedDelivery2.get());
        assertNull("Should not have received delivery 3", receivedDelivery3.get());
        assertEquals("Receiver should not have any deliveries yet", 2, deliveryCounter.get());

        script.remoteTransfer().withDeliveryId(deliveryId3.intValue())
                               .withDeliveryTag(deliveryTag3.getBytes(StandardCharsets.UTF_8))
                               .withMessageFormat(0)
                               .withPayload(new byte[] {3}).now();

        assertNotNull("Should have received delivery 1", receivedDelivery1.get());
        assertNotNull("Should have received delivery 2", receivedDelivery2.get());
        assertNotNull("Should have received delivery 3", receivedDelivery3.get());
        assertEquals("Receiver should not have any deliveries yet", 3, deliveryCounter.get());

        assertNotSame("delivery duplicate detected", receivedDelivery1.get(), receivedDelivery2.get());
        assertNotSame("delivery duplicate detected", receivedDelivery2.get(), receivedDelivery3.get());
        assertNotSame("delivery duplicate detected", receivedDelivery1.get(), receivedDelivery3.get());

        // Verify deliveries arrived with expected payload
        assertArrayEquals(deliveryTag1.getBytes(StandardCharsets.UTF_8), receivedDelivery1.get().getTag());
        assertArrayEquals(deliveryTag2.getBytes(StandardCharsets.UTF_8), receivedDelivery2.get().getTag());
        assertArrayEquals(deliveryTag3.getBytes(StandardCharsets.UTF_8), receivedDelivery3.get().getTag());

        ProtonBuffer delivery1Buffer = receivedDelivery1.get().readAll();
        ProtonBuffer delivery2Buffer = receivedDelivery2.get().readAll();
        ProtonBuffer delivery3Buffer = receivedDelivery3.get().readAll();

        assertEquals("Delivery 1 payload not as expected", 1, delivery1Buffer.readByte());
        assertEquals("Delivery 2 payload not as expected", 2, delivery2Buffer.readByte());
        assertEquals("Delivery 3 payload not as expected", 3, delivery3Buffer.readByte());

        script.expectDetach().respond();
        script.expectEnd().respond();
        script.expectClose().respond();

        receiver.close();
        session.close();
        connection.close();

        // Check post conditions and done.
        driver.assertScriptComplete();
        assertNull(failure);
    }
}
