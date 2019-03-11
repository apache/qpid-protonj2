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
import static org.junit.Assert.assertNull;

import org.apache.qpid.proton4j.amqp.driver.AMQPTestDriver;
import org.apache.qpid.proton4j.amqp.driver.types.AMQPHeaderType;
import org.apache.qpid.proton4j.amqp.driver.types.OpenType;
import org.apache.qpid.proton4j.engine.ConnectionState;
import org.junit.Test;

/**
 * Test Proton Engine with the test driver
 */
public class ProtonEngineTestWithDriver extends ProtonEngineTestSupport {

    private Exception failure;
    private ProtonConnection connection;

    @Test
    public void testEngineEmitsAMQPHeaderOnConnectionOpen() {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);

        // Create the test driver and link it to the engine for output handling.
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);

        AMQPHeaderType.expectAMQPHeader(driver).respondWithAMQPHeader();
        OpenType.expectOpen(driver).respond().withContainerId("driver");

        engine.start(result -> {
            result.get().open();
        });

        driver.assertScriptComplete();

        assertNull(failure);
    }

    @Test
    public void testEngineEmitsOpenPerformative() {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();
        engine.errorHandler(result -> failure = result);

        // Create the test driver and link it to the engine for output handling.
        AMQPTestDriver driver = new AMQPTestDriver(engine);
        engine.outputConsumer(driver);

        AMQPHeaderType.expectAMQPHeader(driver).respondWithAMQPHeader();
        OpenType.expectOpen(driver).withContainerId("test").respond().withContainerId("driver");

        engine.start(result -> {
            connection = (ProtonConnection) result.get();
            connection.setContainerId("test");
            connection.open();
        });

        driver.assertScriptComplete();

        assertEquals(ConnectionState.ACTIVE, connection.getLocalState());
        assertEquals(ConnectionState.ACTIVE, connection.getRemoteState());

        // TODO - seems to be a bug following write of the open from the connection.
        //        driver enters buffering of a frame for some reason.
//        CloseType.expectClose(driver).respond();
//        connection.close();
//
//        driver.assertScriptComplete();

        assertNull(failure);
    }
}
