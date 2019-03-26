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

import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.qpid.proton4j.amqp.UnsignedInteger;
import org.apache.qpid.proton4j.amqp.UnsignedShort;
import org.apache.qpid.proton4j.amqp.driver.AMQPTestDriver;
import org.apache.qpid.proton4j.amqp.driver.ScriptWriter;
import org.apache.qpid.proton4j.engine.Connection;
import org.apache.qpid.proton4j.engine.ConnectionState;
import org.apache.qpid.proton4j.engine.exceptions.EngineStateException;
import org.junit.Test;

/**
 * Tests for behaviors of the ProtonConnection class
 */
public class ProtonConnectionTest extends ProtonEngineTestSupport {

    @Test
    public void testConnectionRemoteOpenTriggeredWhenRemoteOpenArrives() throws EngineStateException {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);

        // Create the test driver and link it to the engine for output handling.
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);

        ScriptWriter script = driver.createScriptWriter();

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().respond().withContainerId("driver");

        final AtomicBoolean remoteOpened = new AtomicBoolean();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.openEventHandler((result) -> {
            remoteOpened.set(true);
        });

        connection.open();

        driver.assertScriptComplete();

        assertNull(failure);
    }

    @Test
    public void testConnectionRemoteCloseTriggeredWhenRemoteCloseArrives() throws EngineStateException {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);

        // Create the test driver and link it to the engine for output handling.
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);

        ScriptWriter script = driver.createScriptWriter();

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().respond().withContainerId("driver");
        script.expectClose().respond();

        final AtomicBoolean connectionOpenedSignaled = new AtomicBoolean();
        final AtomicBoolean connectionClosedSignaled = new AtomicBoolean();

        Connection connection = engine.start();

        // Default engine should start and return a connection immediately
        assertNotNull(connection);

        connection.openEventHandler(result -> {
            connectionOpenedSignaled.set(true);
        });
        connection.closeEventHandler(result -> {
            connectionClosedSignaled.set(true);
        });

        connection.open();

        assertEquals(ConnectionState.ACTIVE, connection.getLocalState());
        assertEquals(ConnectionState.ACTIVE, connection.getRemoteState());

        connection.close();

        driver.assertScriptComplete();

        assertEquals(ConnectionState.CLOSED, connection.getLocalState());
        assertEquals(ConnectionState.CLOSED, connection.getRemoteState());

        assertNull(failure);
    }

    @Test
    public void testConnectionOpenCarriesDefaultMaxFrameSize() throws IOException {
        doTestConnectionOpenPopulatesOpenCorrectly(false, false, false, false);
    }

    @Test
    public void testConnectionOpenCarriesSetMaxFrameSize() throws IOException {
        doTestConnectionOpenPopulatesOpenCorrectly(true, false, false, false);
    }

    @Test
    public void testConnectionOpenCarriesDefaultContainerId() throws IOException {
        doTestConnectionOpenPopulatesOpenCorrectly(false, false, false, false);
    }

    @Test
    public void testConnectionOpenCarriesSetContainerId() throws IOException {
        doTestConnectionOpenPopulatesOpenCorrectly(false, true, false, false);
    }

    @Test
    public void testConnectionOpenCarriesDefaultChannelMax() throws IOException {
        doTestConnectionOpenPopulatesOpenCorrectly(false, false, false, false);
    }

    @Test
    public void testConnectionOpenCarriesSetChannelMax() throws IOException {
        doTestConnectionOpenPopulatesOpenCorrectly(false, false, true, false);
    }

    @Test
    public void testConnectionOpenCarriesNoHostname() throws IOException {
        doTestConnectionOpenPopulatesOpenCorrectly(false, false, false, false);
    }

    @Test
    public void testConnectionOpenCarriesSetHostname() throws IOException {
        doTestConnectionOpenPopulatesOpenCorrectly(false, false, false, true);
    }

    private void doTestConnectionOpenPopulatesOpenCorrectly(boolean setMaxFrameSize, boolean setContainerId, boolean setChannelMax, boolean setHostname) {
        final int MAX_FRAME_SIZE = 32767;

        int expectedMaxFrameSize = UnsignedInteger.MAX_VALUE.intValue();
        if (setMaxFrameSize) {
            expectedMaxFrameSize = MAX_FRAME_SIZE;
        }
        String expectedContainerId = "";
        if (setContainerId) {
            expectedContainerId = "test";
        }
        short expectedChannelMax = UnsignedShort.MAX_VALUE.shortValue();
        if (setChannelMax) {
            expectedChannelMax = 512;
        }
        String expectedHostname = null;
        if (setHostname) {
            expectedHostname = "localhost";
        }

        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);

        // Create the test driver and link it to the engine for output handling.
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);

        ScriptWriter script = driver.createScriptWriter();

        script.expectAMQPHeader().respondWithAMQPHeader();
        script.expectOpen().withMaxFrameSize(expectedMaxFrameSize)
                           .withChannelMax(expectedChannelMax)
                           .withContainerId(expectedContainerId)
                           .withHostname(expectedHostname)
                           .withIdleTimeOut(nullValue())
                           .withIncomingLocales(nullValue())
                           .withOutgoingLocales(nullValue())
                           .withDesiredCapabilities(nullValue())
                           .withOfferedCapabilities(nullValue())
                           .withProperties(nullValue())
                           .respond().withContainerId("driver");
        script.expectClose().respond();

        ProtonConnection connection = engine.start();

        if (setMaxFrameSize) {
            connection.setMaxFrameSize(expectedMaxFrameSize);
        }
        if (setContainerId) {
            connection.setContainerId(expectedContainerId);
        }
        if (setChannelMax) {
            connection.setChannelMax(expectedChannelMax);
        }
        if (setHostname) {
            connection.setHostname(expectedHostname);
        }

        connection.open();
        connection.close();

        driver.assertScriptComplete();

        assertNull(failure);
    }
}
