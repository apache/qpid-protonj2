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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.qpid.proton4j.engine.Connection;
import org.junit.Test;

/**
 * Test for basic functionality of the ProtonEngine implementation.
 */
public class ProtonEngineTest {

    private Connection connection;

    @Test
    public void testEngineStart() {
        ProtonEngine engine = ProtonEngineFactory.createDefaultEngine();

        // Engine cannot accept input bytes until started.
        assertFalse(engine.isWritable());

        engine.start(result -> {
            assertTrue(result.succeeded());
            connection = result.get();
            assertNotNull(connection);
        });

        // Default engine should start and return a connection immediately
        assertTrue(engine.isWritable());
        assertNotNull(connection);
    }
}
