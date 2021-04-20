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
package org.apache.qpid.protonj2.types.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.protonj2.types.messaging.Section.SectionType;
import org.junit.jupiter.api.Test;

public class ApplicationPropertiesTest {

    @Test
    public void testToStringOnEmptyObject() {
        assertNotNull(new ApplicationProperties(null).toString());
    }

    @Test
    public void testGetPropertiesFromEmptySection() {
        assertNull(new ApplicationProperties(null).getValue());
    }

    @Test
    public void testCopyFromEmpty() {
        assertNull(new ApplicationProperties(null).copy().getValue());
    }

    @Test
    public void testCopy() {
        Map<String, Object> payload = new HashMap<>();
        payload.put("key", "value");

        ApplicationProperties original = new ApplicationProperties(payload);
        ApplicationProperties copy = original.copy();

        assertNotSame(original, copy);
        assertNotSame(original.getValue(), copy.getValue());
        assertEquals(original.getValue(), copy.getValue());
    }

    @Test
    public void testHashCode() {
        Map<String, Object> payload1 = new HashMap<>();
        payload1.put("key", "value");

        Map<String, Object> payload2 = new HashMap<>();
        payload2.put("key1", "value");
        payload2.put("key2", "value");

        ApplicationProperties original = new ApplicationProperties(payload1);
        ApplicationProperties copy = original.copy();
        ApplicationProperties another = new ApplicationProperties(payload2);

        assertEquals(original.hashCode(), copy.hashCode());
        assertNotEquals(original.hashCode(), another.hashCode());

        ApplicationProperties empty = new ApplicationProperties(null);
        ApplicationProperties empty2 = new ApplicationProperties(null);

        assertEquals(empty2.hashCode(), empty.hashCode());
        assertNotEquals(original.hashCode(), empty.hashCode());
    }

    @Test
    public void testEquals() {
        Map<String, Object> payload1 = new HashMap<>();
        payload1.put("key", "value");

        Map<String, Object> payload2 = new HashMap<>();
        payload2.put("key1", "value");
        payload2.put("key2", "value");

        ApplicationProperties original = new ApplicationProperties(payload1);
        ApplicationProperties copy = original.copy();
        ApplicationProperties another = new ApplicationProperties(payload2);
        ApplicationProperties empty = new ApplicationProperties(null);
        ApplicationProperties empty2 = new ApplicationProperties(null);

        assertEquals(original, original);
        assertEquals(original, copy);
        assertNotEquals(original, another);
        assertNotEquals(original, "test");
        assertNotEquals(original, empty);
        assertNotEquals(empty, original);
        assertEquals(empty, empty2);

        assertFalse(original.equals(null));
    }

    @Test
    public void testGetType() {
        assertEquals(SectionType.ApplicationProperties, new ApplicationProperties(null).getType());
    }
}
