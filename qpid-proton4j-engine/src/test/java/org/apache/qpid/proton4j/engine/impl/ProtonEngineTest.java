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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.qpid.proton4j.amqp.driver.ProtonTestPeer;
import org.apache.qpid.proton4j.engine.Connection;
import org.apache.qpid.proton4j.engine.ConnectionState;
import org.apache.qpid.proton4j.engine.Session;
import org.apache.qpid.proton4j.engine.exceptions.EngineStateException;
import org.junit.Test;

/**
 * Test for basic functionality of the ProtonEngine implementation.
 */
public class ProtonEngineTest extends ProtonEngineTestSupport {

    @Test
    public void testEngineStart() {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);

        // Engine cannot accept input bytes until started.
        assertFalse(engine.isWritable());

        Connection connection = engine.start();
        assertNotNull(connection);

        // Default engine should start and return a connection immediately
        assertTrue(engine.isWritable());
        assertNotNull(connection);
        assertNull(failure);
    }

    @Test
    public void testEngineEmitsAMQPHeaderOnConnectionOpen() {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        Connection connection = engine.start();
        assertNotNull(connection);

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");

        connection.setContainerId("test");
        connection.open();

        peer.waitForScriptToComplete();

        assertEquals(ConnectionState.ACTIVE, connection.getLocalState());
        assertEquals(ConnectionState.ACTIVE, connection.getRemoteState());

        assertNull(failure);
    }

    @Test
    public void testExceptionThrownFromOpenWhenRemoteSignalsFailure() {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        Connection connection = engine.start();
        assertNotNull(connection);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond().withContainerId("driver");
        peer.expectAttach();  // TODO Script error back to source

        connection.setContainerId("test");
        connection.open();

        Session session = connection.session();
        try {
            session.open();
            fail("Should have thrown an exception");
        } catch (Throwable other) {
            // Error was expected - TODO - Refine API to indicate what error is going to appear.
        }

        peer.waitForScriptToCompleteIgnoreErrors();

        // TODO - Should we also fire error here.
        //assertNotNull(failure);
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
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

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
        assertEquals("Unexpected deadline returned", 0, deadline);

        deadline = engine.tick(10);
        assertEquals("Unexpected deadline returned", 0, deadline);

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
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

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
        assertEquals("Unexpected deadline returned", expectedDeadline1, deadline);

        // Wait for less time than the deadline with no data - get the same value
        long interimTick = tick1 + 10;
        assertTrue (interimTick < expectedDeadline1);
        assertEquals("When the deadline hasn't been reached tick() should return the previous deadline",  expectedDeadline1, engine.tick(interimTick));
        assertEquals("When the deadline hasn't been reached tick() shouldn't write data", 1, peer.getPerformativeCount());
        assertNull(failure);

        peer.remoteEmptyFrame().now();

        deadline = engine.tick(expectedDeadline1);
        assertEquals("When the deadline has been reached expected a new local deadline to be returned", expectedDeadline2, deadline);
        assertEquals("When the deadline hasn't been reached tick() shouldn't write data", 1, peer.getPerformativeCount());
        assertNull(failure);

        peer.remoteEmptyFrame().now();

        deadline = engine.tick(expectedDeadline2);
        assertEquals("When the deadline has been reached expected a new local deadline to be returned", expectedDeadline3, deadline);
        assertEquals("When the deadline hasn't been reached tick() shouldn't write data", 1, peer.getPerformativeCount());
        assertNull(failure);

        peer.expectClose().withError(notNullValue()).respond();

        assertEquals("Connection should be active", ConnectionState.ACTIVE, connection.getLocalState());
        engine.tick(expectedDeadline3); // Wait for the deadline, but don't receive traffic, allow local timeout to expire
        assertEquals("tick() should have written data", 2, peer.getPerformativeCount());
        assertEquals("Calling tick() after the deadline should result in the connection being closed", ConnectionState.CLOSED, connection.getLocalState());

        peer.waitForScriptToComplete();
        assertNull(failure);
    }
}
