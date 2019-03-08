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

import org.apache.qpid.proton4j.engine.test.EngineTestDriver;
import org.apache.qpid.proton4j.engine.test.types.AMQPHeaderType;
import org.apache.qpid.proton4j.engine.test.types.OpenType;
import org.junit.Test;

/**
 * Test Proton Engine with the test driver
 */
public class ProtonEngineTestWithDriver extends ProtonEngineTestSupport {

    @Test
    public void testEngineEmitsAMQPHeaderOnConnectionOpen() {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();

        // Create the test driver and link it to the engine for output handling.
        EngineTestDriver driver = new EngineTestDriver(engine);
        engine.outputConsumer(driver);

        AMQPHeaderType.raw().expect(driver).withRawHeader().respond(driver);

        engine.start(result -> {
            result.get().open();
        });

        driver.assertScriptComplete();
    }

    @Test
    public void testEngineEmitsOpenPerformative() {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();

        // Create the test driver and link it to the engine for output handling.
        EngineTestDriver driver = new EngineTestDriver(engine);
        engine.outputConsumer(driver);

        AMQPHeaderType.raw().expect(driver).withRawHeader().respond(driver);
        OpenType.open().withContainerId("test").expect(driver).withContainerId("driver").respond(driver);

        engine.start(result -> {
            result.get().open();
        });

        driver.assertScriptComplete();
    }
}
