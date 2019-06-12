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
package org.apache.qpid.proton4j;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.qpid.proton4j.amqp.UnsignedInteger;
import org.junit.Test;

public class UnsignedIntegerTest {

    @Test
    public void testToString() {
        assertEquals("0",  UnsignedInteger.valueOf(0).toString());
        assertEquals("65535", UnsignedInteger.valueOf(65535).toString());
        assertEquals("127", UnsignedInteger.valueOf(127).toString());
    }

    @Test
    public void testHashcode() {
        UnsignedInteger ubyte1 = UnsignedInteger.valueOf((short) 1);
        UnsignedInteger ubyte2 = UnsignedInteger.valueOf((short) 2);

        assertNotEquals(ubyte1, ubyte2);
        assertNotEquals(ubyte1.hashCode(), ubyte2.hashCode());

        assertEquals(ubyte1.hashCode(), UnsignedInteger.valueOf((short) 1).hashCode());
        assertEquals(ubyte2.hashCode(), UnsignedInteger.valueOf((short) 2).hashCode());
    }

    @Test
    public void testShortValue() {
        assertEquals((short) 0, UnsignedInteger.valueOf(0).shortValue());
        assertEquals((short) 65535, UnsignedInteger.valueOf(65535).shortValue());
        assertEquals((short) 1, UnsignedInteger.valueOf(1).shortValue());
        assertEquals((short) 127, UnsignedInteger.valueOf(127).shortValue());
    }

    @Test
    public void testIntValue() {
        assertEquals(0, UnsignedInteger.valueOf(0).intValue());
        assertEquals(65535, UnsignedInteger.valueOf(65535).intValue());
        assertEquals(1, UnsignedInteger.valueOf(1).intValue());
        assertEquals(127, UnsignedInteger.valueOf(127).intValue());
    }

    @Test
    public void testLongValue() {
        assertEquals(0l, UnsignedInteger.valueOf(0).longValue());
        assertEquals(65535l, UnsignedInteger.valueOf(65535).longValue());
        assertEquals(1l, UnsignedInteger.valueOf(1).longValue());
        assertEquals(127l, UnsignedInteger.valueOf(127).longValue());
    }

    @Test
    public void testCompareToByte() {
        assertTrue(UnsignedInteger.valueOf(255).compareTo(255) == 0);
        assertTrue(UnsignedInteger.valueOf(0).compareTo(0) == 0);
        assertTrue(UnsignedInteger.valueOf(127).compareTo(126) > 0);
        assertTrue(UnsignedInteger.valueOf(32).compareTo(64) < 0);
    }

    @Test
    public void testCompareToUnsignedInteger() {
        assertTrue(UnsignedInteger.valueOf(65535).compareTo(UnsignedInteger.valueOf(65535)) == 0);
        assertTrue(UnsignedInteger.valueOf(0).compareTo(UnsignedInteger.valueOf(0)) == 0);
        assertTrue(UnsignedInteger.valueOf(127).compareTo(UnsignedInteger.valueOf(126)) > 0);
        assertTrue(UnsignedInteger.valueOf(32).compareTo(UnsignedInteger.valueOf(64)) < 0);
    }

    @Test
    public void testCompareToIntInt() {
        assertTrue(UnsignedInteger.compare(65536, 65536) == 0);
        assertTrue(UnsignedInteger.compare(0, 0) == 0);
        assertTrue(UnsignedInteger.compare(1, 2) < 0);
        assertTrue(UnsignedInteger.compare(127, 32) > 0);
    }

    @Test
    public void testValueOfLongWithNegativeNumberThrowsNFE() throws Exception {
        try {
            UnsignedInteger.valueOf(-1l);
            fail("Expected exception was not thrown");
        } catch (NumberFormatException nfe) {
            // expected
        }
    }

    @Test
    public void testValueOfLongWithLargeNumberThrowsNFE() throws Exception {
        try {
            UnsignedInteger.valueOf(Long.MAX_VALUE);
            fail("Expected exception was not thrown");
        } catch (NumberFormatException nfe) {
            // expected
        }
    }

    @Test
    public void testValueOfStringWithNegativeNumberThrowsNFE() throws Exception {
        try {
            UnsignedInteger.valueOf("-1");
            fail("Expected exception was not thrown");
        } catch (NumberFormatException nfe) {
            // expected
        }
    }

    @Test
    public void testValueOfStringWithTextThrowsNFE() throws Exception {
        try {
            UnsignedInteger.valueOf("TEST");
            fail("Expected exception was not thrown");
        } catch (NumberFormatException nfe) {
            // expected
        }
    }

    @Test
    public void testValueOfStringWithOutOfRangeValueThrowsNFE() throws Exception {
        try {
            UnsignedInteger.valueOf("" + Long.MAX_VALUE);
            fail("Expected exception was not thrown");
        } catch (NumberFormatException nfe) {
            // expected
        }
    }
}
