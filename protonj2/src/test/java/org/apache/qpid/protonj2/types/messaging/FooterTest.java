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

import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.messaging.Section.SectionType;
import org.junit.jupiter.api.Test;

public class FooterTest {

    @Test
    public void testToStringOnEmptyObject() {
        assertNotNull(new Footer(null).toString());
    }

    @Test
    public void testGetMapFromEmptySection() {
        assertNull(new Footer(null).getValue());
    }

    @Test
    public void testCopy() {
        Map<Symbol, Object> payload = new HashMap<>();
        payload.put(Symbol.valueOf("key"), "value");

        Footer original = new Footer(payload);
        Footer copy = original.copy();

        assertNotSame(original, copy);
        assertNotSame(original.getValue(), copy.getValue());
        assertEquals(original.getValue(), copy.getValue());
    }

    @Test
    public void testCopyFromEmpty() {
        assertNull(new Footer(null).copy().getValue());
    }

    @Test
    public void testGetType() {
        assertEquals(SectionType.Footer, new Footer(null).getType());
    }

    @Test
    public void testHashCode() {
        Map<Symbol, Object> payload1 = new HashMap<>();
        payload1.put(Symbol.valueOf("key"), "value");

        Map<Symbol, Object> payload2 = new HashMap<>();
        payload1.put(Symbol.valueOf("key1"), "value");
        payload1.put(Symbol.valueOf("key2"), "value");

        Footer original = new Footer(payload1);
        Footer copy = original.copy();
        Footer another = new Footer(payload2);

        assertEquals(original.hashCode(), copy.hashCode());
        assertNotEquals(original.hashCode(), another.hashCode());

        Footer empty = new Footer(null);
        Footer empty2 = new Footer(null);

        assertEquals(empty2.hashCode(), empty.hashCode());
        assertNotEquals(original.hashCode(), empty.hashCode());
    }

    @Test
    public void testEquals() {
        Map<Symbol, Object> payload1 = new HashMap<>();
        payload1.put(Symbol.valueOf("key"), "value");

        Map<Symbol, Object> payload2 = new HashMap<>();
        payload2.put(Symbol.valueOf("key1"), "value");
        payload2.put(Symbol.valueOf("key2"), "value");

        Footer original = new Footer(payload1);
        Footer copy = original.copy();
        Footer another = new Footer(payload2);
        Footer empty = new Footer(null);
        Footer empty2 = new Footer(null);

        assertEquals(original, original);
        assertEquals(original, copy);
        assertNotEquals(original, another);
        assertNotEquals(original, "test");
        assertNotEquals(original, empty);
        assertNotEquals(empty, original);
        assertEquals(empty, empty2);

        assertFalse(original.equals(null));
    }
}
