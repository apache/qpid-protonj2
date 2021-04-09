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
package org.apache.qpid.protonj2.client.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.protonj2.client.ErrorCondition;
import org.junit.jupiter.api.Test;

class ClientErrorConditionTest {

    @Test
    void testCreateWithNullProtonError() {
        assertNull(ClientErrorCondition.asProtonErrorCondition(null));
    }

    @Test
    void testCreateWithErrorConditionThatOnlyHasConditionData() {
        ErrorCondition condition = ErrorCondition.create("amqp:error", null);
        ClientErrorCondition clientCondition = new ClientErrorCondition(condition);

        assertEquals("amqp:error", clientCondition.condition());
        assertNull(clientCondition.description());
        assertNotNull(clientCondition.info());

        org.apache.qpid.protonj2.types.transport.ErrorCondition protonError = clientCondition.getProtonErrorCondition();

        assertNotNull(protonError);
        assertEquals("amqp:error", protonError.getCondition().toString());
        assertNull(protonError.getDescription());
        assertNotNull(protonError.getInfo());
    }

    @Test
    void testCreateWithErrorConditionWithConditionAndDescription() {
        ErrorCondition condition = ErrorCondition.create("amqp:error", "example");
        ClientErrorCondition clientCondition = new ClientErrorCondition(condition);

        assertEquals("amqp:error", clientCondition.condition());
        assertEquals("example", clientCondition.description());
        assertNotNull(clientCondition.info());

        org.apache.qpid.protonj2.types.transport.ErrorCondition protonError = clientCondition.getProtonErrorCondition();

        assertNotNull(protonError);
        assertEquals("amqp:error", protonError.getCondition().toString());
        assertEquals("example", protonError.getDescription());
        assertNotNull(protonError.getInfo());
    }

    @Test
    void testCreateWithErrorConditionWithConditionAndDescriptionAndInfo() {
        Map<String, Object> infoMap = new HashMap<>();
        infoMap.put("test", "value");

        ErrorCondition condition = ErrorCondition.create("amqp:error", "example", infoMap);
        ClientErrorCondition clientCondition = new ClientErrorCondition(condition);

        assertEquals("amqp:error", clientCondition.condition());
        assertEquals("example", clientCondition.description());
        assertEquals(infoMap, clientCondition.info());

        org.apache.qpid.protonj2.types.transport.ErrorCondition protonError = clientCondition.getProtonErrorCondition();

        assertNotNull(protonError);
        assertEquals("amqp:error", protonError.getCondition().toString());
        assertEquals("example", protonError.getDescription());
        assertEquals(infoMap, ClientConversionSupport.toStringKeyedMap(protonError.getInfo()));
    }

    @Test
    void testCreateFromForeignErrorCondition() {
        Map<String, Object> infoMap = new HashMap<>();
        infoMap.put("test", "value");

        ErrorCondition condition = new ErrorCondition() {

            @Override
            public Map<String, Object> info() {
                return infoMap;
            }

            @Override
            public String description() {
                return "example";
            }

            @Override
            public String condition() {
                return "amqp:error";
            }
        };

        org.apache.qpid.protonj2.types.transport.ErrorCondition protonError = ClientErrorCondition.asProtonErrorCondition(condition);

        assertNotNull(protonError);
        assertEquals("amqp:error", protonError.getCondition().toString());
        assertEquals("example", protonError.getDescription());
        assertEquals(infoMap, ClientConversionSupport.toStringKeyedMap(protonError.getInfo()));
    }
}
