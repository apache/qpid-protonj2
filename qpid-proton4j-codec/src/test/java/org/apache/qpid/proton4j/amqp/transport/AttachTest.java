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
package org.apache.qpid.proton4j.amqp.transport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.junit.Test;

public class AttachTest {

    @Test
    public void testGetPerformativeType() {
        assertEquals(Performative.PerformativeType.ATTACH, new Attach().getPerformativeType());
    }

    @Test
    public void testToStringOnFreshInstance() {
        assertNotNull(new Attach().toString());
    }

    @Test
    public void testSetNameRefusesNull() {
        try {
            new Attach().setName(null);
            fail("Link name is mandatory");
        } catch (NullPointerException npe) {
        }
    }

    @Test
    public void testSetRoleRefusesNull() {
        try {
            new Attach().setRole(null);
            fail("Link role is mandatory");
        } catch (NullPointerException npe) {
        }
    }

    @Test
    public void testHandleRangeChecks() {
        Attach attach = new Attach();
        try {
            attach.setHandle(-1);
            fail("Cannot set negative long handle value");
        } catch (IllegalArgumentException iae) {}

        try {
            attach.setHandle(UnsignedInteger.MAX_VALUE.longValue() + 1);
            fail("Cannot set long handle value bigger than uint max");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testDeliveryCountRangeChecks() {
        Attach attach = new Attach();
        try {
            attach.setInitialDeliveryCount(-1);
            fail("Cannot set negative long delivery count value");
        } catch (IllegalArgumentException iae) {}

        try {
            attach.setInitialDeliveryCount(UnsignedInteger.MAX_VALUE.longValue() + 1);
            fail("Cannot set long delivery count value bigger than uint max");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testCopyFromNew() {
        Attach original = new Attach();
        Attach copy = original.copy();

        assertTrue(original.isEmpty());
        assertTrue(copy.isEmpty());

        assertEquals(0, original.getElementCount());
        assertEquals(0, copy.getElementCount());
    }
}
