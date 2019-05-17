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
import org.apache.qpid.proton4j.amqp.driver.AMQPTestDriver;
import org.apache.qpid.proton4j.amqp.driver.ScriptWriter;
import org.apache.qpid.proton4j.amqp.messaging.Accepted;
import org.apache.qpid.proton4j.amqp.messaging.Data;
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
    public void testEngineEmitsAttachAfterLocalReceiverOpened() throws Exception {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);
        // Create the test driver and link it to the engine for output handling.
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
        Receiver receiver = session.receiver("test");
        receiver.open();

        driver.assertScriptComplete();

        assertNull(failure);
    }

    @Test
    public void testReceiverFireOpenedEventAfterRemoteAttachArrives() throws Exception {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);
        // Create the test driver and link it to the engine for output handling.
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
        // Create the test driver and link it to the engine for output handling.
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
        // Create the test driver and link it to the engine for output handling.
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
        // Create the test driver and link it to the engine for output handling.
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);
        ScriptWriter script = driver.createScriptWriter();

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().respond().withContainerId("driver");
        script.expectBegin().respond();
        script.remoteAttach().withName("sender")
                             .withHandle(0)
                             .withRole(Role.SENDER)
                             .withInitialDeliveryCount(0);
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
    public void testCannotOpenSenderAfterSessionRemotelyClosed() throws Exception {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);
        ScriptWriter script = driver.createScriptWriter();

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().respond().withContainerId("driver");
        script.expectBegin().respond();
        script.remoteEnd(); // TODO This would be more fluent if there was a thenEnd() on the expect

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
    public void testReceiverSendsFlowWhenCreditSet() throws Exception {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);
        // Create the test driver and link it to the engine for output handling.
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);
        ScriptWriter script = driver.createScriptWriter();

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().respond().withContainerId("driver");
        script.expectBegin().respond();
        script.expectAttach().respond();
        script.expectFlow().withLinkCredit(100);  // TODO validate mandatory fields are set and they have correct values
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
        // Create the test driver and link it to the engine for output handling.
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);
        ScriptWriter script = driver.createScriptWriter();

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().respond().withContainerId("driver");
        script.expectBegin().respond();
        script.expectAttach().respond();
        script.expectFlow().withLinkCredit(100);  // TODO validate mandatory fields are set and they have correct values
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
    public void testReceiverDispatchesIncomingDelivery() throws Exception {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);
        // Create the test driver and link it to the engine for output handling.
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);
        ScriptWriter script = driver.createScriptWriter();

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().respond().withContainerId("driver");
        script.expectBegin().respond();
        script.expectAttach().respond();
        script.expectFlow().withLinkCredit(100);
        script.remoteTransfer().withDeliveryId(0)
                               .withHandle(0) // TODO - Select last opened receiver link if not set
                               .withDeliveryTag(new byte[] {0})
                               .withMore(false)
                               .withMessageFormat(0);
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
        // Create the test driver and link it to the engine for output handling.
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);
        ScriptWriter script = driver.createScriptWriter();

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().respond().withContainerId("driver");
        script.expectBegin().respond();
        script.expectAttach().respond();
        script.expectFlow().withLinkCredit(100);
        script.remoteTransfer().withDeliveryId(0)
                               .withHandle(0) // TODO - Select last opened receiver link if not set
                               .withDeliveryTag(new byte[] {0})
                               .withMore(false)
                               .withMessageFormat(0);
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
    public void testReceiverReportsDeliveryUpdatedOnDisposition() throws Exception {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);
        // Create the test driver and link it to the engine for output handling.
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);
        ScriptWriter script = driver.createScriptWriter();

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().respond().withContainerId("driver");
        script.expectBegin().respond();
        script.expectAttach().respond();
        script.expectFlow().withLinkCredit(100);
        script.remoteTransfer().withDeliveryId(0)
                               .withHandle(0) // TODO - Select last opened receiver link if not set
                               .withDeliveryTag(new byte[] {0})
                               .withMore(false)
                               .withMessageFormat(0);
        script.remoteDisposition().withSettled(true)
                                  .withRole(Role.SENDER)
                                  .withState(Accepted.getInstance())
                                  .withFirst(0);
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
        // Create the test driver and link it to the engine for output handling.
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);
        ScriptWriter script = driver.createScriptWriter();

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().respond().withContainerId("driver");
        script.expectBegin().respond();
        script.expectAttach().respond();
        script.expectFlow().withLinkCredit(2);
        script.remoteTransfer().withDeliveryId(0)
                               .withHandle(0) // TODO - Select last opened receiver link if not set
                               .withDeliveryTag(new byte[] {0})
                               .withMore(false)
                               .withMessageFormat(0);
        script.remoteTransfer().withDeliveryId(1)
                               .withHandle(0) // TODO - Select last opened receiver link if not set
                               .withDeliveryTag(new byte[] {1})
                               .withMore(false)
                               .withMessageFormat(0);
        script.remoteDisposition().withSettled(true)
                                  .withRole(Role.SENDER)
                                  .withState(Accepted.getInstance())
                                  .withFirst(0)
                                  .withLast(1);
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
        // Create the test driver and link it to the engine for output handling.
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
                               .withHandle(0)  // TODO - Select last opened receiver link if not set
                               .withDeliveryTag(new byte[] {0})
                               .withMore(true)
                               .withMessageFormat(0)
                               .withBody().withData(first);
        script.remoteTransfer().withDeliveryId(0)
                               .withHandle(0)  // TODO - Select last opened receiver link if not set
                               .withDeliveryTag(new byte[] {0})
                               .withMore(false)
                               .withMessageFormat(0)
                               .withBody().withData(second);
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
}
