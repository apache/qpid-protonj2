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
package org.apache.qpid.proton4j.types.messaging;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.proton4j.types.Symbol;
import org.apache.qpid.proton4j.types.UnsignedInteger;
import org.junit.Test;

public class TargetTypeTest {

    @Test
    public void testCreate() {
        Target target = new Target();

        assertNull(target.getAddress());
        assertEquals(TerminusDurability.NONE, target.getDurable());
        assertEquals(TerminusExpiryPolicy.SESSION_END, target.getExpiryPolicy());
        assertEquals(UnsignedInteger.ZERO, target.getTimeout());
        assertFalse(target.isDynamic());
        assertNull(target.getDynamicNodeProperties());
        assertNull(target.getCapabilities());
    }

    @Test
    public void testCopyFromDefault() {
        Target target = new Target();

        assertNull(target.getAddress());
        assertEquals(TerminusDurability.NONE, target.getDurable());
        assertEquals(TerminusExpiryPolicy.SESSION_END, target.getExpiryPolicy());
        assertEquals(UnsignedInteger.ZERO, target.getTimeout());
        assertFalse(target.isDynamic());
        assertNull(target.getDynamicNodeProperties());
        assertNull(target.getCapabilities());

        Target copy = target.copy();

        assertNull(copy.getAddress());
        assertEquals(TerminusDurability.NONE, copy.getDurable());
        assertEquals(TerminusExpiryPolicy.SESSION_END, copy.getExpiryPolicy());
        assertEquals(UnsignedInteger.ZERO, copy.getTimeout());
        assertFalse(copy.isDynamic());
        assertNull(copy.getDynamicNodeProperties());
        assertNull(copy.getCapabilities());
    }

    @Test
    public void testCopyWithValues() {
        Target target = new Target();

        Map<Symbol, Object> dynamicProperties = new HashMap<>();
        dynamicProperties.put(Symbol.valueOf("test"), "test");

        assertNull(target.getAddress());
        assertEquals(TerminusDurability.NONE, target.getDurable());
        assertEquals(TerminusExpiryPolicy.SESSION_END, target.getExpiryPolicy());
        assertEquals(UnsignedInteger.ZERO, target.getTimeout());
        assertFalse(target.isDynamic());
        target.setDynamicNodeProperties(dynamicProperties);
        assertNotNull(target.getDynamicNodeProperties());
        target.setCapabilities(Symbol.valueOf("test"));
        assertNotNull(target.getCapabilities());

        Target copy = target.copy();

        assertNull(copy.getAddress());
        assertEquals(TerminusDurability.NONE, copy.getDurable());
        assertEquals(TerminusExpiryPolicy.SESSION_END, copy.getExpiryPolicy());
        assertEquals(UnsignedInteger.ZERO, copy.getTimeout());
        assertFalse(copy.isDynamic());
        assertNotNull(copy.getDynamicNodeProperties());
        assertEquals(dynamicProperties, copy.getDynamicNodeProperties());
        assertNotNull(copy.getCapabilities());
        assertArrayEquals(new Symbol[] { Symbol.valueOf("test") }, target.getCapabilities());

        assertEquals(target.toString(), copy.toString());
    }

    @Test
    public void testSetExpiryPolicy() {
        Target target = new Target();

        assertEquals(TerminusExpiryPolicy.SESSION_END, target.getExpiryPolicy());
        target.setExpiryPolicy(TerminusExpiryPolicy.CONNECTION_CLOSE);
        assertEquals(TerminusExpiryPolicy.CONNECTION_CLOSE, target.getExpiryPolicy());
        target.setExpiryPolicy(null);
        assertEquals(TerminusExpiryPolicy.SESSION_END, target.getExpiryPolicy());
    }

    @Test
    public void testTerminusDurability() {
        Target target = new Target();

        assertEquals(TerminusDurability.NONE, target.getDurable());
        target.setDurable(TerminusDurability.UNSETTLED_STATE);
        assertEquals(TerminusDurability.UNSETTLED_STATE, target.getDurable());
        target.setDurable(null);
        assertEquals(TerminusDurability.NONE, target.getDurable());
    }

    @Test
    public void testToStringOnEmptyObject() {
        assertNotNull(new Target().toString());
    }
}
