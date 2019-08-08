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
package org.apache.qpid.proton4j.amqp.messaging;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;

import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.messaging.Section.SectionType;
import org.junit.Test;

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
}
