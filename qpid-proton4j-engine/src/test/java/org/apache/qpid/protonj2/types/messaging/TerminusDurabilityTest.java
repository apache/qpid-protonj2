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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.apache.qpid.protonj2.types.UnsignedInteger;
import org.apache.qpid.protonj2.types.messaging.TerminusDurability;
import org.junit.Test;

public class TerminusDurabilityTest {

    @Test
    public void testValueOf() {
        assertEquals(TerminusDurability.valueOf(0l), TerminusDurability.NONE);
        assertEquals(TerminusDurability.valueOf(1l), TerminusDurability.CONFIGURATION);
        assertEquals(TerminusDurability.valueOf(2l), TerminusDurability.UNSETTLED_STATE);

        try {
            TerminusDurability.valueOf(100);
            fail("Should not accept null value");
        } catch (IllegalArgumentException iae) {}

        try {
            TerminusDurability.valueOf(-1);
            fail("Should not accept null value");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testValueOfUnsignedInteger() {
        assertEquals(TerminusDurability.valueOf(UnsignedInteger.valueOf(0)), TerminusDurability.NONE);
        assertEquals(TerminusDurability.valueOf(UnsignedInteger.valueOf(1)), TerminusDurability.CONFIGURATION);
        assertEquals(TerminusDurability.valueOf(UnsignedInteger.valueOf(2)), TerminusDurability.UNSETTLED_STATE);

        try {
            TerminusDurability.valueOf(UnsignedInteger.valueOf(22));
            fail("Should not accept null value");
        } catch (IllegalArgumentException iae) {}
    }
}
