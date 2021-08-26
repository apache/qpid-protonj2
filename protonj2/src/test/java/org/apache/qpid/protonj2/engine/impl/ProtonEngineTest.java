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
package org.apache.qpid.protonj2.engine.impl;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.security.sasl.SaslException;

import org.apache.qpid.protonj2.buffer.ProtonByteBuffer;
import org.apache.qpid.protonj2.engine.AMQPPerformativeEnvelopePool;
import org.apache.qpid.protonj2.engine.Connection;
import org.apache.qpid.protonj2.engine.ConnectionState;
import org.apache.qpid.protonj2.engine.Engine;
import org.apache.qpid.protonj2.engine.EngineFactory;
import org.apache.qpid.protonj2.engine.EngineState;
import org.apache.qpid.protonj2.engine.HeaderEnvelope;
import org.apache.qpid.protonj2.engine.SASLEnvelope;
import org.apache.qpid.protonj2.engine.Session;
import org.apache.qpid.protonj2.engine.exceptions.EngineFailedException;
import org.apache.qpid.protonj2.engine.exceptions.EngineNotStartedException;
import org.apache.qpid.protonj2.engine.exceptions.EngineShutdownException;
import org.apache.qpid.protonj2.engine.exceptions.EngineStateException;
import org.apache.qpid.protonj2.engine.exceptions.MalformedAMQPHeaderException;
import org.apache.qpid.protonj2.test.driver.ProtonTestConnector;
import org.apache.qpid.protonj2.types.UnsignedInteger;
import org.apache.qpid.protonj2.types.security.SaslInit;
import org.apache.qpid.protonj2.types.transport.Open;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;

/**
 * Test for basic functionality of the ProtonEngine implementation.
 */
@Timeout(20)
public class ProtonEngineTest extends ProtonEngineTestSupport {

    @Test
    public void testEnginePipelineWriteFailsBeforeStart() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());

        // Engine cannot accept input bytes until started.
        assertFalse(engine.isWritable());

        try {
            engine.pipeline().fireWrite(new ProtonByteBuffer(0), null);
            fail("Should not be able to write until engine has been started");
        } catch (EngineNotStartedException error) {
            // Expected
        }

        try {
            engine.pipeline().fireWrite(HeaderEnvelope.AMQP_HEADER_ENVELOPE);
            fail("Should not be able to write until engine has been started");
        } catch (EngineNotStartedException error) {
            // Expected
        }

        try {
            engine.pipeline().fireWrite(new SASLEnvelope(new SaslInit()));
            fail("Should not be able to write until engine has been started");
        } catch (EngineNotStartedException error) {
            // Expected
        }

        try {
            engine.pipeline().fireWrite(AMQPPerformativeEnvelopePool.outgoingEnvelopePool().take(new Open(), 0, null));
            fail("Should not be able to write until engine has been started");
        } catch (EngineNotStartedException error) {
            // Expected
        }

        assertNull(failure);
    }

    @Test
    public void testEnginePipelineReadFailsBeforeStart() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());

        // Engine cannot accept input bytes until started.
        assertFalse(engine.isWritable());

        try {
            engine.pipeline().fireRead(HeaderEnvelope.AMQP_HEADER_ENVELOPE);
            fail("Should not be able to read data until engine has been started");
        } catch (EngineNotStartedException error) {
            // Expected
        }

        try {
            engine.pipeline().fireRead(new SASLEnvelope(new SaslInit()));
            fail("Should not be able to read data until engine has been started");
        } catch (EngineNotStartedException error) {
            // Expected
        }

        try {
            engine.pipeline().fireRead(AMQPPerformativeEnvelopePool.incomingEnvelopePool().take(new Open(), 0, new ProtonByteBuffer(0)));
            fail("Should not be able to read data until engine has been started");
        } catch (EngineNotStartedException error) {
            // Expected
        }

        try {
            engine.pipeline().fireRead(new ProtonByteBuffer(0));
            fail("Should not be able to write until engine has been started");
        } catch (EngineNotStartedException error) {
            // Expected
        }

        assertNull(failure);
    }

    @Test
    public void testEngineStart() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());

        // Engine cannot accept input bytes until started.
        assertFalse(engine.isWritable());

        Connection connection = engine.start();
        assertNotNull(connection);

        assertFalse(engine.isShutdown());
        assertFalse(engine.isFailed());
        assertNull(engine.failureCause());

        // Should be idempotent and return same Connection
        Connection another = engine.start();
        assertSame(connection, another);

        // Default engine should start and return a connection immediately
        assertTrue(engine.isWritable());
        assertNotNull(connection);
        assertNull(failure);
    }

    @Test
    public void testEngineShutdown() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());

        // Engine cannot accept input bytes until started.
        assertFalse(engine.isWritable());

        Connection connection = engine.start();
        assertNotNull(connection);

        assertTrue(engine.isWritable());
        assertFalse(engine.isShutdown());
        assertFalse(engine.isFailed());
        assertNull(engine.failureCause());
        assertEquals(EngineState.STARTED, engine.state());

        final AtomicBoolean engineShutdownEventFired = new AtomicBoolean();

        engine.shutdownHandler(theEngine -> engineShutdownEventFired.set(true));
        engine.shutdown();

        assertFalse(engine.isWritable());
        assertTrue(engine.isShutdown());
        assertFalse(engine.isFailed());
        assertNull(engine.failureCause());
        assertEquals(EngineState.SHUTDOWN, engine.state());
        assertTrue(engineShutdownEventFired.get());

        assertNotNull(connection);
        assertNull(failure);
    }

    @Test
    public void testEngineFailure() {
        ProtonEngine engine = (ProtonEngine) EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());

        // Engine cannot accept input bytes until started.
        assertFalse(engine.isWritable());

        Connection connection = engine.start();
        assertNotNull(connection);

        assertTrue(engine.isWritable());
        assertFalse(engine.isShutdown());
        assertFalse(engine.isFailed());
        assertNull(engine.failureCause());
        assertEquals(EngineState.STARTED, engine.state());

        engine.engineFailed(new SaslException());

        assertFalse(engine.isWritable());
        assertFalse(engine.isShutdown());
        assertTrue(engine.isFailed());
        assertNotNull(engine.failureCause());
        assertEquals(EngineState.FAILED, engine.state());

        engine.shutdown();

        assertFalse(engine.isWritable());
        assertTrue(engine.isShutdown());
        assertTrue(engine.isFailed());
        assertNotNull(engine.failureCause());
        assertEquals(EngineState.SHUTDOWN, engine.state());

        assertNotNull(connection);
        assertNotNull(failure);
        assertTrue(failure instanceof SaslException);
    }

    @Test
    public void testEngineStartAfterConnectionOpen() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        // Engine cannot accept input bytes until started.
        assertFalse(engine.isWritable());

        Connection connection = engine.connection();
        assertNotNull(connection);

        assertFalse(engine.isShutdown());
        assertFalse(engine.isFailed());
        assertNull(engine.failureCause());

        connection.open();

        peer.waitForScriptToComplete();
        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen();

        // Should be idempotent and return same Connection
        Connection another = engine.start();
        assertSame(connection, another);

        // Default engine should start and return a connection immediately
        assertTrue(engine.isWritable());
        assertNotNull(connection);
        assertNull(failure);

        peer.waitForScriptToComplete();
    }

    @Test
    public void testEngineEmitsAMQPHeaderOnConnectionOpen() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        Connection connection = engine.start();
        assertNotNull(connection);

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");

        connection.setContainerId("test");
        connection.open();

        peer.waitForScriptToComplete();

        assertEquals(ConnectionState.ACTIVE, connection.getState());
        assertEquals(ConnectionState.ACTIVE, connection.getRemoteState());

        assertNull(failure);
    }

    @Test
    public void testTickFailsWhenConnectionNotOpenedNoLocalIdleSet() throws EngineStateException {
        doTestTickFailsBasedOnState(false, false, false, false);
    }

    @Test
    public void testTickFailsWhenConnectionNotOpenedLocalIdleSet() throws EngineStateException {
        doTestTickFailsBasedOnState(true, false, false, false);
    }

    @Test
    public void testTickFailsWhenEngineIsShutdownNoLocalIdleSet() throws EngineStateException {
        doTestTickFailsBasedOnState(false, true, true, true);
    }

    @Test
    public void testTickFailsWhenEngineIsShutdownLocalIdleSet() throws EngineStateException {
        doTestTickFailsBasedOnState(true, true, true, true);
    }

    @Test
    public void testTickFailsWhenEngineIsShutdownButCloseNotCalledNoLocalIdleSet() throws EngineStateException {
        doTestTickFailsBasedOnState(false, true, false, true);
    }

    @Test
    public void testTickFailsWhenEngineIsShutdownButCloseNotCalledLocalIdleSet() throws EngineStateException {
        doTestTickFailsBasedOnState(true, true, false, true);
    }

    private void doTestTickFailsBasedOnState(boolean setLocalTimeout, boolean open, boolean close, boolean shutdown) throws EngineStateException {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        Connection connection = engine.start();
        assertNotNull(connection);

        if (setLocalTimeout) {
            connection.setIdleTimeout(1000);
        }

        if (open) {
            peer.expectAMQPHeader().respondWithAMQPHeader();
            peer.expectOpen().respond();
            connection.open();
        }

        if (close) {
            peer.expectClose().respond();
            connection.close();
        }

        peer.waitForScriptToComplete();
        assertNull(failure);

        if (shutdown) {
            engine.shutdown();
        }

        try {
            engine.tick(5000);
            fail("Should not be able to tick an unopened connection");
        } catch (IllegalStateException | EngineShutdownException error) {
        }
    }

    @Test
    public void testAutoTickFailsWhenConnectionNotOpenedNoLocalIdleSet() throws EngineStateException {
        doTestAutoTickFailsBasedOnState(false, false, false, false);
    }

    @Test
    public void testAutoTickFailsWhenConnectionNotOpenedLocalIdleSet() throws EngineStateException {
        doTestAutoTickFailsBasedOnState(true, false, false, false);
    }

    @Test
    public void testAutoTickFailsWhenEngineShutdownNoLocalIdleSet() throws EngineStateException {
        doTestAutoTickFailsBasedOnState(false, true, true, true);
    }

    @Test
    public void testAutoTickFailsWhenEngineShutdownLocalIdleSet() throws EngineStateException {
        doTestAutoTickFailsBasedOnState(true, true, true, true);
    }

    private void doTestAutoTickFailsBasedOnState(boolean setLocalTimeout, boolean open, boolean close, boolean shutdown) throws EngineStateException {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        Connection connection = engine.start();
        assertNotNull(connection);

        if (setLocalTimeout) {
            connection.setIdleTimeout(1000);
        }

        if (open) {
            peer.expectAMQPHeader().respondWithAMQPHeader();
            peer.expectOpen().respond();
            connection.open();
        }

        if (close) {
            peer.expectClose().respond();
            connection.close();
        }

        peer.waitForScriptToComplete();
        assertNull(failure);

        if (shutdown) {
            engine.shutdown();
        }

        try {
            engine.tickAuto(Mockito.mock(ScheduledExecutorService.class));
            fail("Should not be able to tick an unopened connection");
        } catch (IllegalStateException | EngineShutdownException error) {
        }
    }

    @Test
    public void testTickAutoPreventsDoubleInvocation() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        Connection connection = engine.start();
        assertNotNull(connection);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectClose().respond();

        connection.open();

        engine.tickAuto(Mockito.mock(ScheduledExecutorService.class));

        try {
            engine.tickAuto(Mockito.mock(ScheduledExecutorService.class));
            fail("Should not be able call tickAuto more than once.");
        } catch (IllegalStateException ise) {
        }

        connection.close();

        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test
    public void testCannotCallTickAfterTickAutoCalled() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        Connection connection = engine.start();
        assertNotNull(connection);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectClose().respond();

        connection.open();

        engine.tickAuto(Mockito.mock(ScheduledExecutorService.class));

        try {
            engine.tick(5000);
            fail("Should not be able call tick after enabling the auto tick feature.");
        } catch (IllegalStateException ise) {
        }

        connection.close();

        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test
    public void testTickRemoteTimeout() throws EngineStateException {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        Connection connection = engine.start();
        assertNotNull(connection);

        final int remoteTimeout = 4000;

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().withIdleTimeOut(nullValue()).respond().withIdleTimeOut(remoteTimeout);

        // Set our local idleTimeout
        connection.open();

        long deadline = engine.tick(0);
        assertEquals(2000, deadline, "Expected to be returned a deadline of 2000");  // deadline = 4000 / 2

        deadline = engine.tick(1000);    // Wait for less than the deadline with no data - get the same value
        assertEquals(2000, deadline, "When the deadline hasn't been reached tick() should return the previous deadline");
        assertEquals(0, peer.getEmptyFrameCount(), "When the deadline hasn't been reached tick() shouldn't write data");

        peer.expectEmptyFrame();

        deadline = engine.tick(remoteTimeout / 2); // Wait for the deadline - next deadline should be (4000/2)*2
        assertEquals(4000, deadline, "When the deadline has been reached expected a new deadline to be returned 4000");
        assertEquals(1, peer.getEmptyFrameCount(), "tick() should have written data");

        peer.expectBegin();
        Session session = connection.session().open();

        deadline = engine.tick(3000);
        assertEquals(5000, deadline, "Writing data resets the deadline");
        assertEquals(1, peer.getEmptyFrameCount(), "When the deadline is reset tick() shouldn't write an empty frame");

        peer.expectAttach();
        session.sender("test").open();

        deadline = engine.tick(4000);
        assertEquals(6000, deadline, "Writing data resets the deadline");
        assertEquals(1, peer.getEmptyFrameCount(), "When the deadline is reset tick() shouldn't write an empty frame");

        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test
    public void testTickLocalTimeout() throws EngineStateException {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        Connection connection = engine.start();
        assertNotNull(connection);

        final int localTimeout = 4000;

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().withIdleTimeOut(localTimeout).respond();

        // Set our local idleTimeout
        connection.setIdleTimeout(localTimeout);
        connection.open();

        long deadline = engine.tick(0);
        assertEquals(4000, deadline, "Expected to be returned a deadline of 4000");

        deadline = engine.tick(1000);    // Wait for less than the deadline with no data - get the same value
        assertEquals(4000, deadline, "When the deadline hasn't been reached tick() should return the previous deadline");
        assertEquals(0, peer.getEmptyFrameCount(), "Reading data should never result in a frame being written");

        // remote sends an empty frame now
        peer.remoteEmptyFrame().now();

        deadline = engine.tick(2000);
        assertEquals(6000, deadline, "Reading data resets the deadline");
        assertEquals(0, peer.getEmptyFrameCount(), "Reading data should never result in a frame being written");
        assertEquals(ConnectionState.ACTIVE, connection.getState(), "Reading data before the deadline should keep the connection open");

        peer.expectClose().respond();

        deadline = engine.tick(7000);
        assertEquals(ConnectionState.CLOSED, connection.getState(), "Calling tick() after the deadline should result in the connection being closed");

        peer.waitForScriptToComplete();
        assertNotNull(failure);
    }

    @Test
    public void testTickWithZeroIdleTimeoutsGivesZeroDeadline() throws EngineStateException {
        doTickWithNoIdleTimeoutGivesZeroDeadlineTestImpl(true);
    }

    @Test
    public void testTickWithNullIdleTimeoutsGivesZeroDeadline() throws EngineStateException {
        doTickWithNoIdleTimeoutGivesZeroDeadlineTestImpl(false);
    }

    private void doTickWithNoIdleTimeoutGivesZeroDeadlineTestImpl(boolean useZero) throws EngineStateException {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        Connection connection = engine.start();
        assertNotNull(connection);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        if (useZero) {
            peer.expectOpen().withIdleTimeOut(nullValue()).respond().withIdleTimeOut(0);
        } else {
            peer.expectOpen().withIdleTimeOut(nullValue()).respond();
        }

        connection.open();

        peer.waitForScriptToComplete();
        assertNull(failure);

        assertEquals(0, connection.getRemoteIdleTimeout());

        long deadline = engine.tick(0);
        assertEquals(0, deadline, "Unexpected deadline returned");

        deadline = engine.tick(10);
        assertEquals(0, deadline, "Unexpected deadline returned");

        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test
    public void testTickWithLocalTimeout() throws EngineStateException {
        // all-positive
        doTickWithLocalTimeoutTestImpl(4000, 10000, 14000, 18000, 22000);

        // all-negative
        doTickWithLocalTimeoutTestImpl(2000, -100000, -98000, -96000, -94000);

        // negative to positive missing 0
        doTickWithLocalTimeoutTestImpl(500, -950, -450, 50, 550);

        // negative to positive striking 0
        doTickWithLocalTimeoutTestImpl(3000, -6000, -3000, 1, 3001);
    }

    private void doTickWithLocalTimeoutTestImpl(int localTimeout, long tick1, long expectedDeadline1, long expectedDeadline2, long expectedDeadline3) throws EngineStateException {
        this.failure = null;
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        Connection connection = engine.start();
        assertNotNull(connection);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().withIdleTimeOut(localTimeout).respond();

        // Set our local idleTimeout
        connection.setIdleTimeout(localTimeout);
        connection.open();

        peer.waitForScriptToComplete();
        assertNull(failure);

        long deadline = engine.tick(tick1);
        assertEquals(expectedDeadline1, deadline, "Unexpected deadline returned");

        // Wait for less time than the deadline with no data - get the same value
        long interimTick = tick1 + 10;
        assertTrue(interimTick < expectedDeadline1);
        assertEquals(expectedDeadline1, engine.tick(interimTick), "When the deadline hasn't been reached tick() should return the previous deadline");
        assertEquals(1, peer.getPerformativeCount(), "When the deadline hasn't been reached tick() shouldn't write data");
        assertNull(failure);

        peer.remoteEmptyFrame().now();

        deadline = engine.tick(expectedDeadline1);
        assertEquals(expectedDeadline2, deadline, "When the deadline has been reached expected a new local deadline to be returned");
        assertEquals(1, peer.getPerformativeCount(), "When the deadline hasn't been reached tick() shouldn't write data");
        assertNull(failure);

        peer.remoteEmptyFrame().now();

        deadline = engine.tick(expectedDeadline2);
        assertEquals(expectedDeadline3, deadline, "When the deadline has been reached expected a new local deadline to be returned");
        assertEquals(1, peer.getPerformativeCount(), "When the deadline hasn't been reached tick() shouldn't write data");
        assertNull(failure);

        peer.expectClose().withError(notNullValue()).respond();

        assertEquals(ConnectionState.ACTIVE, connection.getState(), "Connection should be active");
        engine.tick(expectedDeadline3); // Wait for the deadline, but don't receive traffic, allow local timeout to expire
        assertEquals(2, peer.getPerformativeCount(), "tick() should have written data");
        assertEquals(ConnectionState.CLOSED, connection.getState(), "Calling tick() after the deadline should result in the connection being closed");

        peer.waitForScriptToComplete();
        assertNotNull(failure);
    }

    @Test
    public void testTickWithRemoteTimeout() throws EngineStateException {
        // all-positive
        doTickWithRemoteTimeoutTestImpl(4000, 10000, 14000, 18000, 22000);

        // all-negative
        doTickWithRemoteTimeoutTestImpl(2000, -100000, -98000, -96000, -94000);

        // negative to positive missing 0
        doTickWithRemoteTimeoutTestImpl(500, -950, -450, 50, 550);

        // negative to positive striking 0
        doTickWithRemoteTimeoutTestImpl(3000, -6000, -3000, 1, 3001);
    }

    private void doTickWithRemoteTimeoutTestImpl(int remoteTimeoutHalf, long tick1, long expectedDeadline1, long expectedDeadline2, long expectedDeadline3) throws EngineStateException {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        Connection connection = engine.start();
        assertNotNull(connection);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        // Handle the peer transmitting [half] their timeout. We half it on receipt to avoid spurious timeouts
        // if they not have transmitted half their actual timeout, as the AMQP spec only says they SHOULD do that.
        peer.expectOpen().respond().withIdleTimeOut(remoteTimeoutHalf * 2);

        connection.open();

        peer.waitForScriptToComplete();
        assertNull(failure);

        long deadline = engine.tick(tick1);
        assertEquals(expectedDeadline1, deadline, "Unexpected deadline returned");

        // Wait for less time than the deadline with no data - get the same value
        long interimTick = tick1 + 10;
        assertTrue(interimTick < expectedDeadline1);
        assertEquals(expectedDeadline1, engine.tick(interimTick), "When the deadline hasn't been reached tick() should return the previous deadline");
        assertEquals(1, peer.getPerformativeCount(), "When the deadline hasn't been reached tick() shouldn't write data");
        assertEquals(0, peer.getEmptyFrameCount(), "When the deadline hasn't been reached tick() shouldn't write data");

        peer.expectEmptyFrame();

        deadline = engine.tick(expectedDeadline1);
        assertEquals(expectedDeadline2, deadline, "When the deadline has been reached expected a new remote deadline to be returned");
        assertEquals(1, peer.getEmptyFrameCount(), "tick() should have written data");

        peer.expectBegin();

        // Do some actual work, create real traffic, removing the need to send empty frame to satisfy idle-timeout
        connection.session().open();

        assertEquals(2, peer.getPerformativeCount(), "session open should have written data");

        deadline = engine.tick(expectedDeadline2);
        assertEquals(expectedDeadline3, deadline, "When the deadline has been reached expected a new remote deadline to be returned");
        assertEquals(2, peer.getPerformativeCount(), "tick() should not have written data as there was actual activity");
        assertEquals(1, peer.getEmptyFrameCount(), "tick() should not have written data as there was actual activity");

        peer.expectEmptyFrame();

        engine.tick(expectedDeadline3);
        assertEquals(2, peer.getEmptyFrameCount(), "tick() should have written data");

        peer.waitForScriptToComplete();
        assertNull(failure);
    }

    @Test
    public void testTickWithBothTimeouts() throws EngineStateException {
        // all-positive
        doTickWithBothTimeoutsTestImpl(true, 5000, 2000, 10000, 12000, 14000, 15000);
        doTickWithBothTimeoutsTestImpl(false, 5000, 2000, 10000, 12000, 14000, 15000);

        // all-negative
        doTickWithBothTimeoutsTestImpl(true, 10000, 4000, -100000, -96000, -92000, -90000);
        doTickWithBothTimeoutsTestImpl(false, 10000, 4000, -100000, -96000, -92000, -90000);

        // negative to positive missing 0
        doTickWithBothTimeoutsTestImpl(true, 500, 200, -450, -250, -50, 50);
        doTickWithBothTimeoutsTestImpl(false, 500, 200, -450, -250, -50, 50);

        // negative to positive striking 0 with local deadline
        doTickWithBothTimeoutsTestImpl(true, 500, 200, -500, -300, -100, 1);
        doTickWithBothTimeoutsTestImpl(false, 500, 200, -500, -300, -100, 1);

        // negative to positive striking 0 with remote deadline
        doTickWithBothTimeoutsTestImpl(true, 500, 200, -200, 1, 201, 300);
        doTickWithBothTimeoutsTestImpl(false, 500, 200, -200, 1, 201, 300);
    }

    private void doTickWithBothTimeoutsTestImpl(boolean allowLocalTimeout, int localTimeout, int remoteTimeoutHalf, long tick1,
                                                long expectedDeadline1, long expectedDeadline2, long expectedDeadline3) throws EngineStateException {

        this.failure = null;
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        Connection connection = engine.start();
        assertNotNull(connection);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        // Handle the peer transmitting [half] their timeout. We half it on receipt to avoid spurious timeouts
        // if they not have transmitted half their actual timeout, as the AMQP spec only says they SHOULD do that.
        peer.expectOpen().respond().withIdleTimeOut(remoteTimeoutHalf * 2);

        connection.setIdleTimeout(localTimeout);
        connection.open();

        long deadline = engine.tick(tick1);
        assertEquals(expectedDeadline1, deadline, "Unexpected deadline returned");

        // Wait for less time than the deadline with no data - get the same value
        long interimTick = tick1 + 10;
        assertTrue(interimTick < expectedDeadline1);
        assertEquals(expectedDeadline1, engine.tick(interimTick), "When the deadline hasn't been reached tick() should return the previous deadline");
        assertEquals(0, peer.getEmptyFrameCount(), "When the deadline hasn't been reached tick() shouldn't write data");

        peer.expectEmptyFrame();

        deadline = engine.tick(expectedDeadline1);
        assertEquals(expectedDeadline2, deadline, "When the deadline has been reached expected a new remote deadline to be returned");
        assertEquals(1, peer.getEmptyFrameCount(), "tick() should have written data");

        peer.expectEmptyFrame();

        deadline = engine.tick(expectedDeadline2);
        assertEquals(expectedDeadline3, deadline, "When the deadline has been reached expected a new local deadline to be returned");
        assertEquals(2, peer.getEmptyFrameCount(), "tick() should have written data");

        peer.waitForScriptToComplete();

        if (allowLocalTimeout) {
            peer.expectClose().respond();

            assertEquals(ConnectionState.ACTIVE, connection.getState(), "Connection should be active");
            engine.tick(expectedDeadline3); // Wait for the deadline, but don't receive traffic, allow local timeout to expire
            assertEquals(ConnectionState.CLOSED, connection.getState(), "Calling tick() after the deadline should result in the connection being closed");
            assertEquals(2, peer.getEmptyFrameCount(), "tick() should have written data but not an empty frame");

            peer.waitForScriptToComplete();
            assertNotNull(failure);
        } else {
            peer.remoteEmptyFrame().now();

            deadline = engine.tick(expectedDeadline3);
            assertEquals(expectedDeadline2 + (remoteTimeoutHalf), deadline, "Receiving data should have reset the deadline (to the next remote one)");
            assertEquals(2, peer.getEmptyFrameCount(), "tick() shouldn't have written data");
            assertEquals(ConnectionState.ACTIVE, connection.getState(), "Connection should be active");

            peer.waitForScriptToComplete();
            assertNull(failure);
        }
    }

    @Test
    public void testTickWithNanoTimeDerivedValueWhichWrapsLocalThenRemote() throws EngineStateException {
        doTickWithNanoTimeDerivedValueWhichWrapsLocalThenRemoteTestImpl(false);
    }

    @Test
    public void testTickWithNanoTimeDerivedValueWhichWrapsLocalThenRemoteWithLocalTimeout() throws EngineStateException {
        doTickWithNanoTimeDerivedValueWhichWrapsLocalThenRemoteTestImpl(true);
    }

    private void doTickWithNanoTimeDerivedValueWhichWrapsLocalThenRemoteTestImpl(boolean allowLocalTimeout) throws EngineStateException {
        int localTimeout = 5000;
        int remoteTimeoutHalf = 2000;
        assertTrue(remoteTimeoutHalf < localTimeout);

        long offset = 2500;
        assertTrue(offset < localTimeout);
        assertTrue(offset > remoteTimeoutHalf);

        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        Connection connection = engine.start();
        assertNotNull(connection);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        // Handle the peer transmitting [half] their timeout. We half it on receipt to avoid spurious timeouts
        // if they not have transmitted half their actual timeout, as the AMQP spec only says they SHOULD do that.
        peer.expectOpen().respond().withIdleTimeOut(remoteTimeoutHalf * 2);

        connection.setIdleTimeout(localTimeout);
        connection.open();

        long deadline = engine.tick(Long.MAX_VALUE - offset);
        assertEquals(Long.MAX_VALUE - offset + remoteTimeoutHalf, deadline, "Unexpected deadline returned");

        deadline = engine.tick(Long.MAX_VALUE - (offset - 100));    // Wait for less time than the deadline with no data - get the same value
        assertEquals(Long.MAX_VALUE -offset + remoteTimeoutHalf, deadline, "When the deadline hasn't been reached tick() should return the previous deadline");
        assertEquals(0, peer.getEmptyFrameCount(), "When the deadline hasn't been reached tick() shouldn't write data");

        peer.expectEmptyFrame();

        deadline = engine.tick(Long.MAX_VALUE -offset + remoteTimeoutHalf); // Wait for the deadline - next deadline should be previous + remoteTimeoutHalf;
        assertEquals(Long.MIN_VALUE + (2* remoteTimeoutHalf) - offset -1, deadline, "When the deadline has been reached expected a new remote deadline to be returned");
        assertEquals(1, peer.getEmptyFrameCount(), "tick() should have written data");

        peer.expectEmptyFrame();

        deadline = engine.tick(Long.MIN_VALUE + (2* remoteTimeoutHalf) - offset -1); // Wait for the deadline - next deadline should be orig + localTimeout;
        assertEquals(Long.MIN_VALUE + (localTimeout - offset) -1, deadline, "When the deadline has been reached expected a new local deadline to be returned");
        assertEquals(2, peer.getEmptyFrameCount(), "tick() should have written data");

        peer.waitForScriptToComplete();

        if (allowLocalTimeout) {
            peer.expectClose().respond();

            assertEquals(ConnectionState.ACTIVE, connection.getState(), "Connection should be active");
            engine.tick(Long.MIN_VALUE + (localTimeout - offset) -1); // Wait for the deadline, but don't receive traffic, allow local timeout to expire
            assertEquals(ConnectionState.CLOSED, connection.getState(), "Calling tick() after the deadline should result in the connection being closed");
            assertEquals(2, peer.getEmptyFrameCount(), "tick() should have written data but not an empty frame");

            peer.waitForScriptToComplete();
            assertNotNull(failure);
        } else {
            peer.remoteEmptyFrame().now();

            deadline = engine.tick(Long.MIN_VALUE + (localTimeout - offset) -1); // Wait for the deadline - next deadline should be orig + 3*remoteTimeoutHalf;
            assertEquals(Long.MIN_VALUE + (3* remoteTimeoutHalf) - offset -1, deadline, "Receiving data should have reset the deadline (to the remote one)");
            assertEquals(2, peer.getEmptyFrameCount(), "tick() shouldn't have written data");
            assertEquals(ConnectionState.ACTIVE, connection.getState(), "Connection should be active");

            peer.waitForScriptToComplete();
            assertNull(failure);
        }
    }

    @Test
    public void testTickWithNanoTimeDerivedValueWhichWrapsRemoteThenLocal() throws EngineStateException {
        doTickWithNanoTimeDerivedValueWhichWrapsRemoteThenLocalTestImpl(false);
    }

    @Test
    public void testTickWithNanoTimeDerivedValueWhichWrapsRemoteThenLocalWithLocalTimeout() throws EngineStateException {
        doTickWithNanoTimeDerivedValueWhichWrapsRemoteThenLocalTestImpl(true);
    }

    private void doTickWithNanoTimeDerivedValueWhichWrapsRemoteThenLocalTestImpl(boolean allowLocalTimeout) throws EngineStateException {
        int localTimeout = 2000;
        int remoteTimeoutHalf = 5000;
        assertTrue(localTimeout < remoteTimeoutHalf);

        long offset = 2500;
        assertTrue(offset > localTimeout);
        assertTrue(offset < remoteTimeoutHalf);

        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        Connection connection = engine.start();
        assertNotNull(connection);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        // Handle the peer transmitting [half] their timeout. We half it on receipt to avoid spurious timeouts
        // if they not have transmitted half their actual timeout, as the AMQP spec only says they SHOULD do that.
        peer.expectOpen().respond().withIdleTimeOut(remoteTimeoutHalf * 2);

        connection.setIdleTimeout(localTimeout);
        connection.open();

        long deadline = engine.tick(Long.MAX_VALUE - offset);
        assertEquals(Long.MAX_VALUE - offset + localTimeout, deadline, "Unexpected deadline returned");

        deadline = engine.tick(Long.MAX_VALUE - (offset - 100));    // Wait for less time than the deadline with no data - get the same value
        assertEquals(Long.MAX_VALUE - offset + localTimeout, deadline, "When the deadline hasn't been reached tick() should return the previous deadline");
        assertEquals(0, peer.getEmptyFrameCount(), "tick() shouldn't have written data");

        // Receive Empty frame to satisfy local deadline
        peer.remoteEmptyFrame().now();

        deadline = engine.tick(Long.MAX_VALUE - offset + localTimeout); // Wait for the deadline - next deadline should be orig + 2* localTimeout;
        assertEquals(Long.MIN_VALUE + (localTimeout - offset) -1 + localTimeout, deadline, "When the deadline has been reached expected a new local deadline to be returned");
        assertEquals(0, peer.getEmptyFrameCount(), "tick() should not have written data");

        peer.waitForScriptToComplete();

        if (allowLocalTimeout) {
            peer.expectClose().respond();

            assertEquals(ConnectionState.ACTIVE, connection.getState(), "Connection should be active");
            engine.tick(Long.MIN_VALUE + (localTimeout - offset) -1 + localTimeout); // Wait for the deadline, but don't receive traffic, allow local timeout to expire
            assertEquals(ConnectionState.CLOSED, connection.getState(), "Calling tick() after the deadline should result in the connection being closed");
            assertEquals(0, peer.getEmptyFrameCount(), "tick() should have written data but not an empty frame");

            peer.waitForScriptToComplete();
            assertNotNull(failure);
        } else {
            // Receive Empty frame to satisfy local deadline
            peer.remoteEmptyFrame().now();

            deadline = engine.tick(Long.MIN_VALUE + (localTimeout - offset) -1 + localTimeout); // Wait for the deadline - next deadline should be orig + remoteTimeoutHalf;
            assertEquals(Long.MIN_VALUE + remoteTimeoutHalf - offset -1, deadline, "Receiving data should have reset the deadline (to the remote one)");
            assertEquals(0, peer.getEmptyFrameCount(), "tick() shouldn't have written data");

            peer.expectEmptyFrame();

            deadline = engine.tick(Long.MIN_VALUE + remoteTimeoutHalf - offset -1); // Wait for the deadline - next deadline should be orig + 3* localTimeout;
            assertEquals(Long.MIN_VALUE + (3* localTimeout) - offset -1, deadline, "When the deadline has been reached expected a new local deadline to be returned");
            assertEquals(1, peer.getEmptyFrameCount(), "tick() should have written an empty frame");
            assertEquals(ConnectionState.ACTIVE, connection.getState(), "Connection should be active");

            peer.waitForScriptToComplete();
            assertNull(failure);
        }
    }

    @Test
    public void testTickWithNanoTimeDerivedValueWhichWrapsBothRemoteFirst() throws EngineStateException {
        doTickWithNanoTimeDerivedValueWhichWrapsBothRemoteFirstTestImpl(false);
    }

    @Test
    public void testTickWithNanoTimeDerivedValueWhichWrapsBothRemoteFirstWithLocalTimeout() throws EngineStateException {
        doTickWithNanoTimeDerivedValueWhichWrapsBothRemoteFirstTestImpl(true);
    }

    private void doTickWithNanoTimeDerivedValueWhichWrapsBothRemoteFirstTestImpl(boolean allowLocalTimeout) throws EngineStateException {
        int localTimeout = 2000;
        int remoteTimeoutHalf = 2500;
        assertTrue(localTimeout < remoteTimeoutHalf);

        long offset = 500;
        assertTrue(offset < localTimeout);

        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        Connection connection = engine.start();
        assertNotNull(connection);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        // Handle the peer transmitting [half] their timeout. We half it on receipt to avoid spurious timeouts
        // if they not have transmitted half their actual timeout, as the AMQP spec only says they SHOULD do that.
        peer.expectOpen().respond().withIdleTimeOut(remoteTimeoutHalf * 2);

        connection.setIdleTimeout(localTimeout);
        connection.open();

        long deadline = engine.tick(Long.MAX_VALUE - offset);
        assertEquals(Long.MIN_VALUE + (localTimeout - offset) -1, deadline, "Unexpected deadline returned");

        deadline = engine.tick(Long.MAX_VALUE - (offset - 100));    // Wait for less time than the deadline with no data - get the same value
        assertEquals(Long.MIN_VALUE + (localTimeout - offset) -1, deadline, "When the deadline hasn't been reached tick() should return the previous deadline");
        assertEquals(0, peer.getEmptyFrameCount(), "tick() shouldn't have written data");

        // Receive Empty frame to satisfy local deadline
        peer.remoteEmptyFrame().now();

        deadline = engine.tick(Long.MIN_VALUE + (localTimeout - offset) -1); // Wait for the deadline - next deadline should be orig + remoteTimeoutHalf;
        assertEquals(Long.MIN_VALUE + (remoteTimeoutHalf - offset) -1, deadline, "When the deadline has been reached expected a new remote deadline to be returned");
        assertEquals(0, peer.getEmptyFrameCount(), "When the deadline hasn't been reached tick() shouldn't write data");

        peer.expectEmptyFrame();

        deadline = engine.tick(Long.MIN_VALUE + (remoteTimeoutHalf - offset) -1); // Wait for the deadline - next deadline should be orig + 2* localTimeout;
        assertEquals(Long.MIN_VALUE + (localTimeout - offset) -1 + localTimeout, deadline, "When the deadline has been reached expected a new local deadline to be returned");
        assertEquals(1, peer.getEmptyFrameCount(), "tick() should have written data");

        peer.waitForScriptToComplete();

        if (allowLocalTimeout) {
            peer.expectClose().respond();

            assertEquals(ConnectionState.ACTIVE, connection.getState(), "Connection should be active");
            engine.tick(Long.MIN_VALUE + (localTimeout - offset) -1 + localTimeout); // Wait for the deadline, but don't receive traffic, allow local timeout to expire
            assertEquals(ConnectionState.CLOSED, connection.getState(), "Calling tick() after the deadline should result in the connection being closed");
            assertEquals(1, peer.getEmptyFrameCount(), "tick() should have written data but not an empty frame");

            peer.waitForScriptToComplete();
            assertNotNull(failure);
        } else {
            // Receive Empty frame to satisfy local deadline
            peer.remoteEmptyFrame().now();

            deadline = engine.tick(Long.MIN_VALUE + (localTimeout - offset) -1 + localTimeout); // Wait for the deadline - next deadline should be orig + 2*remoteTimeoutHalf;
            assertEquals(Long.MIN_VALUE + (2* remoteTimeoutHalf) - offset -1, deadline, "Receiving data should have reset the deadline (to the remote one)");
            assertEquals(1, peer.getEmptyFrameCount(), "tick() shouldn't have written data");
            assertEquals(ConnectionState.ACTIVE, connection.getState(), "Connection should be active");

            peer.waitForScriptToComplete();
            assertNull(failure);
        }
    }

    @Test
    public void testTickWithNanoTimeDerivedValueWhichWrapsBothLocalFirst() throws EngineStateException {
        doTickWithNanoTimeDerivedValueWhichWrapsBothLocalFirstTestImpl(false);
    }

    @Test
    public void testTickWithNanoTimeDerivedValueWhichWrapsBothLocalFirstWithLocalTimeout() throws EngineStateException {
        doTickWithNanoTimeDerivedValueWhichWrapsBothLocalFirstTestImpl(true);
    }

    private void doTickWithNanoTimeDerivedValueWhichWrapsBothLocalFirstTestImpl(boolean allowLocalTimeout) throws EngineStateException {
        int localTimeout = 5000;
        int remoteTimeoutHalf = 2000;
        assertTrue(remoteTimeoutHalf < localTimeout);

        long offset = 500;
        assertTrue(offset < remoteTimeoutHalf);

        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        Connection connection = engine.start();
        assertNotNull(connection);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        // Handle the peer transmitting [half] their timeout. We half it on receipt to avoid spurious timeouts
        // if they not have transmitted half their actual timeout, as the AMQP spec only says they SHOULD do that.
        peer.expectOpen().respond().withIdleTimeOut(remoteTimeoutHalf * 2);

        connection.setIdleTimeout(localTimeout);
        connection.open();

        long deadline = engine.tick(Long.MAX_VALUE - offset);
        assertEquals(Long.MIN_VALUE + (remoteTimeoutHalf - offset) -1, deadline, "Unexpected deadline returned");

        deadline = engine.tick(Long.MAX_VALUE - (offset - 100));    // Wait for less time than the deadline with no data - get the same value
        assertEquals(Long.MIN_VALUE + (remoteTimeoutHalf - offset) -1, deadline, "When the deadline hasn't been reached tick() should return the previous deadline");
        assertEquals(0, peer.getEmptyFrameCount(), "When the deadline hasn't been reached tick() shouldn't write data");

        peer.expectEmptyFrame();

        deadline = engine.tick(Long.MIN_VALUE + (remoteTimeoutHalf - offset) -1); // Wait for the deadline - next deadline should be previous + remoteTimeoutHalf;
        assertEquals(Long.MIN_VALUE + (remoteTimeoutHalf - offset) -1 + remoteTimeoutHalf, deadline, "When the deadline has been reached expected a new remote deadline to be returned");
        assertEquals(1, peer.getEmptyFrameCount(), "tick() should have written data");

        peer.expectEmptyFrame();

        deadline = engine.tick(Long.MIN_VALUE + (remoteTimeoutHalf - offset) -1 + remoteTimeoutHalf); // Wait for the deadline - next deadline should be orig + localTimeout;
        assertEquals(Long.MIN_VALUE + (localTimeout - offset) -1, deadline, "When the deadline has been reached expected a new local deadline to be returned");
        assertEquals(2, peer.getEmptyFrameCount(), "tick() should have written data");

        peer.waitForScriptToComplete();

        if (allowLocalTimeout) {
            peer.expectClose().respond();

            assertEquals(ConnectionState.ACTIVE, connection.getState(), "Connection should be active");
            engine.tick(Long.MIN_VALUE + (localTimeout - offset) -1); // Wait for the deadline, but don't receive traffic, allow local timeout to expire
            assertEquals(ConnectionState.CLOSED, connection.getState(), "Calling tick() after the deadline should result in the connection being closed");
            assertEquals(2, peer.getEmptyFrameCount(), "tick() should have written data but not an empty frame");

            peer.waitForScriptToComplete();
            assertNotNull(failure);
        } else {
            // Receive Empty frame to satisfy local deadline
            peer.remoteEmptyFrame().now();

            deadline = engine.tick(Long.MIN_VALUE + (localTimeout - offset) -1); // Wait for the deadline - next deadline should be orig + 3*remoteTimeoutHalf;
            assertEquals(Long.MIN_VALUE + (3* remoteTimeoutHalf) - offset -1, deadline, "Receiving data should have reset the deadline (to the remote one)");
            assertEquals(2, peer.getEmptyFrameCount(), "tick() shouldn't have written data");
            assertEquals(ConnectionState.ACTIVE, connection.getState(), "Connection should be active");

            peer.waitForScriptToComplete();
            assertNull(failure);
        }
    }

    @Test
    public void testEngineFailsWithMeaningfulErrorOnNonAMQPHeaderResponseBadByte1() throws EngineStateException {
        doTestEngineFailsWithMalformedHeaderException(new byte[] { 'a', 'M', 'Q', 'P', 0, 1, 0, 0 });
    }

    @Test
    public void testEngineFailsWithMeaningfulErrorOnNonAMQPHeaderResponseBadByte2() throws EngineStateException {
        doTestEngineFailsWithMalformedHeaderException(new byte[] { 'A', 'm', 'Q', 'P', 0, 1, 0, 0 });
    }

    @Test
    public void testEngineFailsWithMeaningfulErrorOnNonAMQPHeaderResponseBadByte3() throws EngineStateException {
        doTestEngineFailsWithMalformedHeaderException(new byte[] { 'A', 'M', 'q', 'P', 0, 1, 0, 0 });
    }

    @Test
    public void testEngineFailsWithMeaningfulErrorOnNonAMQPHeaderResponseBadByte4() throws EngineStateException {
        doTestEngineFailsWithMalformedHeaderException(new byte[] { 'A', 'M', 'Q', 'p', 0, 1, 0, 0 });
    }

    @Test
    public void testEngineFailsWithMeaningfulErrorOnNonAMQPHeaderResponseBadByte5() throws EngineStateException {
        doTestEngineFailsWithMalformedHeaderException(new byte[] { 'A', 'M', 'Q', 'P', 99, 1, 0, 0 });
    }

    @Test
    public void testEngineFailsWithMeaningfulErrorOnNonAMQPHeaderResponseBadByte6() throws EngineStateException {
        doTestEngineFailsWithMalformedHeaderException(new byte[] { 'A', 'M', 'Q', 'P', 0, 99, 0, 0 });
    }

    @Test
    public void testEngineFailsWithMeaningfulErrorOnNonAMQPHeaderResponseBadByte7() throws EngineStateException {
        doTestEngineFailsWithMalformedHeaderException(new byte[] { 'A', 'M', 'Q', 'P', 0, 1, 99, 0 });
    }

    @Test
    public void testEngineFailsWithMeaningfulErrorOnNonAMQPHeaderResponseBadByte8() throws EngineStateException {
        doTestEngineFailsWithMalformedHeaderException(new byte[] { 'A', 'M', 'Q', 'P', 0, 1, 0, 99 });
    }

    private final void doTestEngineFailsWithMalformedHeaderException(byte[] headerBytes) {

        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithBytes(headerBytes);

        Connection connection = engine.start();
        assertNotNull(connection);
        connection.negotiate();

        peer.waitForScriptToCompleteIgnoreErrors();

        assertNotNull(failure);
        assertTrue(failure instanceof MalformedAMQPHeaderException);
    }

    @Test
    public void testEngineConfiguresDefaultMaxFrameSizeLimits() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        Connection connection = engine.start();
        assertNotNull(connection);
        ProtonEngineConfiguration configuration = (ProtonEngineConfiguration) engine.configuration();
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().withMaxFrameSize(ProtonConstants.DEFAULT_MAX_AMQP_FRAME_SIZE).respond();

        connection.open();

        assertEquals(ProtonConstants.DEFAULT_MAX_AMQP_FRAME_SIZE, configuration.getOutboundMaxFrameSize());
        assertEquals(ProtonConstants.DEFAULT_MAX_AMQP_FRAME_SIZE, configuration.getInboundMaxFrameSize());

        // Default engine should start and return a connection immediately
        assertNull(failure);
    }

    @Test
    public void testEngineConfiguresSpecifiedMaxFrameSizeLimitsMatchesDefaultMinMax() {
        doTestEngineConfiguresSpecifiedFrameSizeLimits(512, 512);
    }

    @Test
    public void testEngineConfiguresSpecifiedMaxFrameSizeLimitsRemoteLargerThanLocal() {
        doTestEngineConfiguresSpecifiedFrameSizeLimits(1024, 1025);
    }

    @Test
    public void testEngineConfiguresSpecifiedMaxFrameSizeLimitsRemoteSmallerThanLocal() {
        doTestEngineConfiguresSpecifiedFrameSizeLimits(1024, 1023);
    }

    @Test
    public void testEngineConfiguresSpecifiedMaxFrameSizeLimitsGreaterThanDefaultValues() {
        doTestEngineConfiguresSpecifiedFrameSizeLimits(
            ProtonConstants.DEFAULT_MAX_AMQP_FRAME_SIZE + 32, ProtonConstants.DEFAULT_MAX_AMQP_FRAME_SIZE + 64);
    }

    @Test
    public void testEngineConfiguresRemoteMaxFrameSizeSetToMaxUnsignedLong() {
        doTestEngineConfiguresSpecifiedFrameSizeLimits(
            Integer.MAX_VALUE, UnsignedInteger.MAX_VALUE.intValue());
    }

    private void doTestEngineConfiguresSpecifiedFrameSizeLimits(int localValue, int remoteResponse) {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        Connection connection = engine.start();
        assertNotNull(connection);
        ProtonEngineConfiguration configuration = (ProtonEngineConfiguration) engine.configuration();
        ProtonTestConnector peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().withMaxFrameSize(Integer.toUnsignedLong(localValue))
                         .respond()
                         .withMaxFrameSize(Integer.toUnsignedLong(remoteResponse));

        connection.setMaxFrameSize(Integer.toUnsignedLong(localValue));
        connection.open();

        if (localValue > 0) {
            assertEquals(localValue, configuration.getInboundMaxFrameSize());
        } else {
            assertEquals(Integer.MAX_VALUE, configuration.getInboundMaxFrameSize());
        }

        if (remoteResponse > localValue) {
            assertEquals(localValue, configuration.getOutboundMaxFrameSize());
        } else {
            if (remoteResponse > 0) {
                assertEquals(remoteResponse, configuration.getOutboundMaxFrameSize());
            } else {
                assertEquals(Integer.MAX_VALUE, configuration.getOutboundMaxFrameSize());
            }
        }

        assertEquals(UnsignedInteger.toUnsignedLong(localValue), connection.getMaxFrameSize());
        assertEquals(UnsignedInteger.toUnsignedLong(remoteResponse), connection.getRemoteMaxFrameSize());

        // Default engine should start and return a connection immediately
        assertNull(failure);
    }

    @Test
    public void testEngineErrorsOnLocalMaxFrameSizeLargerThanImposedLimit() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        Connection connection = engine.start();
        assertNotNull(connection);

        assertThrows(IllegalArgumentException.class, () -> connection.setMaxFrameSize(UnsignedInteger.MAX_VALUE.longValue()));
    }

    @Test
    public void testEngineShutdownHandlerThrowsIsIgnoredAndShutdownCompletes() {
        ProtonEngine engine = (ProtonEngine) EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());

        engine.shutdownHandler((theEngine) -> {
            throw new RuntimeException();
        });

        Connection connection = engine.start();
        assertNotNull(connection);

        assertTrue(engine.isWritable());
        assertTrue(engine.isRunning());
        assertFalse(engine.isShutdown());
        assertFalse(engine.isFailed());
        assertNull(engine.failureCause());
        assertEquals(EngineState.STARTED, engine.state());

        try {
            engine.shutdown();
            fail("User event handler throw wasn't propagated");
        } catch (RuntimeException expected) {
            // Expected
        }

        assertFalse(engine.isWritable());
        assertFalse(engine.isRunning());
        assertTrue(engine.isShutdown());
        assertFalse(engine.isFailed());
        assertNull(engine.failureCause());
        assertEquals(EngineState.SHUTDOWN, engine.state());

        // should not perform any additional work.
        engine.shutdown();

        assertNotNull(connection);
        assertNull(failure);
    }

    @Test
    public void testEnginePipelineProtectsFromExternalUserMischief() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        Connection connection = engine.connection().open();

        peer.waitForScriptToComplete();
        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen();

        engine.start();

        assertTrue(engine.isWritable());
        assertNotNull(connection);
        assertNull(failure);

        assertThrows(IllegalAccessError.class, () -> engine.pipeline().fireEngineStarting());
        assertThrows(IllegalAccessError.class, () -> engine.pipeline().fireEngineStateChanged());
        assertThrows(IllegalAccessError.class, () -> engine.pipeline().fireFailed(new EngineFailedException(null)));

        engine.shutdown();

        assertThrows(EngineShutdownException.class, () -> engine.pipeline().first());
        assertThrows(EngineShutdownException.class, () -> engine.pipeline().last());
        assertThrows(EngineShutdownException.class, () -> engine.pipeline().firstContext());
        assertThrows(EngineShutdownException.class, () -> engine.pipeline().lastContext());

        peer.waitForScriptToComplete();
    }
}
