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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.qpid.proton4j.amqp.driver.AMQPTestDriver;
import org.apache.qpid.proton4j.amqp.driver.ScriptWriter;
import org.apache.qpid.proton4j.engine.Connection;
import org.apache.qpid.proton4j.engine.ConnectionState;
import org.apache.qpid.proton4j.engine.Session;
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

        // Create the test driver and link it to the engine for output handling.
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);

        ScriptWriter script = driver.createScriptWriter();

        Connection connection = engine.start();
        assertNotNull(connection);

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().respond().withContainerId("driver");

        connection.setContainerId("test");
        connection.open();

        driver.assertScriptComplete();

        assertEquals(ConnectionState.ACTIVE, connection.getLocalState());
        assertEquals(ConnectionState.ACTIVE, connection.getRemoteState());

        assertNull(failure);
    }

    @Test
    public void testExceptionThrownFromOpenWhenRemoteSignalsFailure() {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);

        // Create the test driver and link it to the engine for output handling.
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);

        ScriptWriter script = driver.createScriptWriter();

        Connection connection = engine.start();
        assertNotNull(connection);

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().respond().withContainerId("driver");
        script.expectAttach();  // TODO Script error back to source

        connection.setContainerId("test");
        connection.open();

        Session session = connection.session();
        try {
            session.open();
            fail("Should have thrown an exception");
        } catch (Throwable other) {
            // Error was expected - TODO - Refine API to indicate what error is going to appear.
        }

        driver.assertScriptCompleteIngoreErrors();

        // TODO - Should we also fire error here.
        //assertNotNull(failure);
    }
}
