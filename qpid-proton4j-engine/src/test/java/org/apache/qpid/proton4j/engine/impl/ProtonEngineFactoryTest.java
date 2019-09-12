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
import static org.junit.Assert.assertNotNull;

import org.apache.qpid.proton4j.engine.Engine;
import org.apache.qpid.proton4j.engine.EngineFactory;
import org.apache.qpid.proton4j.engine.EngineSaslDriver.SaslState;
import org.apache.qpid.proton4j.engine.EngineState;
import org.junit.Test;

/**
 * Tests the ProtonEngineFactory implementation.
 */
public class ProtonEngineFactoryTest {

    @Test
    public void testCreateEngine() {
        Engine engine = EngineFactory.PROTON.createEngine();

        assertEquals(EngineState.IDLE, engine.state());
        assertNotNull(engine.saslContext());
        assertEquals(engine.saslContext().getSaslState(), SaslState.IDLE);
    }

    @Test
    public void testCreateNonSaslEngine() {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();

        assertEquals(EngineState.IDLE, engine.state());
        assertNotNull(engine.saslContext());
        assertEquals(engine.saslContext().getSaslState(), SaslState.DISABLED);
    }
}
