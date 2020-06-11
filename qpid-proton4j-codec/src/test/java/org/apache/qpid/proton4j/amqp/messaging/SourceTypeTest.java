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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.proton4j.types.Symbol;
import org.apache.qpid.proton4j.types.UnsignedInteger;
import org.apache.qpid.proton4j.types.messaging.Source;
import org.apache.qpid.proton4j.types.messaging.TerminusDurability;
import org.apache.qpid.proton4j.types.messaging.TerminusExpiryPolicy;
import org.junit.Test;

public class SourceTypeTest {

    @Test
    public void testSetExpiryPolicy() {
        Source source = new Source();

        assertEquals(TerminusExpiryPolicy.SESSION_END, source.getExpiryPolicy());
        source.setExpiryPolicy(TerminusExpiryPolicy.CONNECTION_CLOSE);
        assertEquals(TerminusExpiryPolicy.CONNECTION_CLOSE, source.getExpiryPolicy());
        source.setExpiryPolicy(null);
        assertEquals(TerminusExpiryPolicy.SESSION_END, source.getExpiryPolicy());
    }

    @Test
    public void testTerminusDurability() {
        Source source = new Source();

        assertEquals(TerminusDurability.NONE, source.getDurable());
        source.setDurable(TerminusDurability.UNSETTLED_STATE);
        assertEquals(TerminusDurability.UNSETTLED_STATE, source.getDurable());
        source.setDurable(null);
        assertEquals(TerminusDurability.NONE, source.getDurable());
    }

    @Test
    public void testCreate() {
        Source source = new Source();

        assertNull(source.getAddress());
        assertEquals(TerminusDurability.NONE, source.getDurable());
        assertEquals(TerminusExpiryPolicy.SESSION_END, source.getExpiryPolicy());
        assertEquals(UnsignedInteger.ZERO, source.getTimeout());
        assertFalse(source.isDynamic());
        assertNull(source.getDynamicNodeProperties());
        assertNull(source.getDistributionMode());
        assertNull(source.getFilter());
        assertNull(source.getDefaultOutcome());
        assertNull(source.getOutcomes());
        assertNull(source.getCapabilities());
    }

    @Test
    public void testCopyFromDefault() {
        Source source = new Source();

        assertNull(source.getAddress());
        assertEquals(TerminusDurability.NONE, source.getDurable());
        assertEquals(TerminusExpiryPolicy.SESSION_END, source.getExpiryPolicy());
        assertEquals(UnsignedInteger.ZERO, source.getTimeout());
        assertFalse(source.isDynamic());
        assertNull(source.getDynamicNodeProperties());
        assertNull(source.getDistributionMode());
        assertNull(source.getFilter());
        assertNull(source.getDefaultOutcome());
        assertNull(source.getOutcomes());
        assertNull(source.getCapabilities());

        Source copy = source.copy();

        assertNull(copy.getAddress());
        assertEquals(TerminusDurability.NONE, copy.getDurable());
        assertEquals(TerminusExpiryPolicy.SESSION_END, copy.getExpiryPolicy());
        assertEquals(UnsignedInteger.ZERO, copy.getTimeout());
        assertFalse(copy.isDynamic());
        assertNull(copy.getDynamicNodeProperties());
        assertNull(copy.getDistributionMode());
        assertNull(copy.getFilter());
        assertNull(copy.getDefaultOutcome());
        assertNull(copy.getOutcomes());
        assertNull(copy.getCapabilities());
    }

    @Test
    public void testCopyWithValues() {
        Source source = new Source();

        Map<Symbol, Object> dynamicProperties = new HashMap<>();
        dynamicProperties.put(Symbol.valueOf("test"), "test");
        Map<Symbol, Object> filter = new HashMap<>();
        filter.put(Symbol.valueOf("filter"), "filter");

        assertNull(source.getAddress());
        assertEquals(TerminusDurability.NONE, source.getDurable());
        assertEquals(TerminusExpiryPolicy.SESSION_END, source.getExpiryPolicy());
        assertEquals(UnsignedInteger.ZERO, source.getTimeout());
        assertFalse(source.isDynamic());
        source.setDynamicNodeProperties(dynamicProperties);
        assertNotNull(source.getDynamicNodeProperties());
        assertNull(source.getDistributionMode());
        source.setFilter(filter);
        assertNotNull(source.getFilter());
        assertNull(source.getDefaultOutcome());
        source.setOutcomes(Symbol.valueOf("accepted"));
        assertNotNull(source.getOutcomes());
        source.setCapabilities(Symbol.valueOf("test"));
        assertNotNull(source.getCapabilities());

        Source copy = source.copy();

        assertNull(copy.getAddress());
        assertEquals(TerminusDurability.NONE, copy.getDurable());
        assertEquals(TerminusExpiryPolicy.SESSION_END, copy.getExpiryPolicy());
        assertEquals(UnsignedInteger.ZERO, copy.getTimeout());
        assertFalse(copy.isDynamic());
        assertNotNull(copy.getDynamicNodeProperties());
        assertEquals(dynamicProperties, copy.getDynamicNodeProperties());
        assertNull(copy.getDistributionMode());
        assertNotNull(copy.getFilter());
        assertEquals(filter, copy.getFilter());
        assertNull(copy.getDefaultOutcome());
        assertNotNull(copy.getOutcomes());
        assertArrayEquals(new Symbol[] { Symbol.valueOf("accepted") }, source.getOutcomes());
        assertNotNull(copy.getCapabilities());
        assertArrayEquals(new Symbol[] { Symbol.valueOf("test") }, source.getCapabilities());

        assertEquals(source.toString(), copy.toString());
    }

    @Test
    public void testToStringOnEmptyObject() {
        assertNotNull(new Source().toString());
    }
}
