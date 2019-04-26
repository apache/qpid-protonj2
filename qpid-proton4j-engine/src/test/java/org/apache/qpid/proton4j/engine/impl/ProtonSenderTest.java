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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.qpid.proton4j.amqp.driver.AMQPTestDriver;
import org.apache.qpid.proton4j.amqp.driver.ScriptWriter;
import org.apache.qpid.proton4j.amqp.transport.DeliveryState;
import org.apache.qpid.proton4j.amqp.transport.Role;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.engine.Connection;
import org.apache.qpid.proton4j.engine.OutgoingDelivery;
import org.apache.qpid.proton4j.engine.Sender;
import org.apache.qpid.proton4j.engine.Session;
import org.junit.Test;

/**
 * Test the {@link ProtonSender}
 */
public class ProtonSenderTest extends ProtonEngineTestSupport {

    @Test
    public void testEngineEmitsAttachAfterLocalSenderOpened() throws Exception {
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
        Sender sender = session.sender("test");
        sender.open();
        sender.close();

        driver.assertScriptComplete();

        assertNull(failure);
    }

    @Test
    public void testSenderFireOpenedEventAfterRemoteAttachArrives() throws Exception {
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
        // Create the test driver and link it to the engine for output handling.
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
        // Create the test driver and link it to the engine for output handling.
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
                             .onChannel(0);
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
    public void testGetCurrentDeliveryFromSender() throws Exception {
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
        // Create the test driver and link it to the engine for output handling.
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);
        ScriptWriter script = driver.createScriptWriter();

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().respond().withContainerId("driver");
        script.expectBegin().respond();
        script.expectAttach().withHandle(0).withRole(Role.SENDER).respond();
        script.remoteFlow().withDeliveryCount(0)
                           .withHandle(0)
                           .withLinkCredit(10)
                           .withIncomingWindow(1024)
                           .withOutgoingWindow(10)
                           .withNextIncomingId(0)
                           .withNextOutgoingId(1).onChannel(0);  // TODO - Track sessions and use last opened by default
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
        // Create the test driver and link it to the engine for output handling.
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);
        ScriptWriter script = driver.createScriptWriter();

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().respond().withContainerId("driver");
        script.expectBegin().respond();
        script.expectAttach().withHandle(0).withRole(Role.SENDER).respond();
        script.remoteFlow().withDeliveryCount(0)  // TODO - Would be nice to automate filling in these
                           .withHandle(0)         //        these bits using last session opened values
                           .withLinkCredit(10)    //        plus some defaults or generated values.
                           .withIncomingWindow(1024)
                           .withOutgoingWindow(10)
                           .withNextIncomingId(0)
                           .withNextOutgoingId(1).onChannel(0);  // TODO - Track sessions and use last opened by default
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
}
