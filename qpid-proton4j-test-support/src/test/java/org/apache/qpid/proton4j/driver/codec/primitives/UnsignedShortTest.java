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
package org.apache.qpid.proton4j.driver.codec.primitives;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.qpid.proton4j.test.driver.codec.primitives.UnsignedShort;
import org.junit.Test;

public class UnsignedShortTest {

    @Test
    public void testToString() {
        assertEquals("0",  UnsignedShort.valueOf((short) 0).toString());
        assertEquals("65535", UnsignedShort.valueOf((short) 65535).toString());
        assertEquals("127", UnsignedShort.valueOf((short) 127).toString());
    }

    @Test
    public void testHashcode() {
        UnsignedShort ubyte1 = UnsignedShort.valueOf((short) 1);
        UnsignedShort ubyte2 = UnsignedShort.valueOf((short) 2);

        assertNotEquals(ubyte1, ubyte2);
        assertNotEquals(ubyte1.hashCode(), ubyte2.hashCode());

        assertEquals(ubyte1.hashCode(), UnsignedShort.valueOf((short) 1).hashCode());
        assertEquals(ubyte2.hashCode(), UnsignedShort.valueOf((short) 2).hashCode());
    }

    @Test
    public void testShortValue() {
        assertEquals((short) 0, UnsignedShort.valueOf((short) 0).shortValue());
        assertEquals((short) 65535, UnsignedShort.valueOf((short) 65535).shortValue());
        assertEquals((short) 1, UnsignedShort.valueOf((short) 1).shortValue());
        assertEquals((short) 127, UnsignedShort.valueOf((short) 127).shortValue());
    }

    @Test
    public void testIntValue() {
        assertEquals(0, UnsignedShort.valueOf((short) 0).intValue());
        assertEquals(65535, UnsignedShort.valueOf((short) 65535).intValue());
        assertEquals(1, UnsignedShort.valueOf((short) 1).intValue());
        assertEquals(127, UnsignedShort.valueOf((short) 127).intValue());
    }

    @Test
    public void testLongValue() {
        assertEquals(0l, UnsignedShort.valueOf((short) 0).longValue());
        assertEquals(65535l, UnsignedShort.valueOf((short) 65535).longValue());
        assertEquals(1l, UnsignedShort.valueOf((short) 1).longValue());
        assertEquals(127l, UnsignedShort.valueOf((short) 127).longValue());
    }

    @Test
    public void testCompareToByte() {
        assertTrue(UnsignedShort.valueOf((short) 255).compareTo((short) 255) == 0);
        assertTrue(UnsignedShort.valueOf((short) 0).compareTo((short) 0) == 0);
        assertTrue(UnsignedShort.valueOf((short) 127).compareTo((short) 126) > 0);
        assertTrue(UnsignedShort.valueOf((short) 32).compareTo((short) 64) < 0);
    }

    @Test
    public void testCompareToUnsignedShort() {
        assertTrue(UnsignedShort.valueOf((short) 65535).compareTo(UnsignedShort.valueOf((short) 65535)) == 0);
        assertTrue(UnsignedShort.valueOf((short) 0).compareTo(UnsignedShort.valueOf((short) 0)) == 0);
        assertTrue(UnsignedShort.valueOf((short) 127).compareTo(UnsignedShort.valueOf((short) 126)) > 0);
        assertTrue(UnsignedShort.valueOf((short) 32).compareTo(UnsignedShort.valueOf((short) 64)) < 0);
    }

    @Test
    public void testCompareToIntInt() {
        assertTrue(UnsignedShort.compare((short) 65536, (short) 65536) == 0);
        assertTrue(UnsignedShort.compare((short) 0, (short) 0) == 0);
        assertTrue(UnsignedShort.compare((short) 1, (short) 2) < 0);
        assertTrue(UnsignedShort.compare((short) 127, (short) 32) > 0);
    }

    @Test
    public void testValueOfIntWithNegativeNumberThrowsNFE() throws Exception {
        try {
            UnsignedShort.valueOf(-1);
            fail("Expected exception was not thrown");
        } catch (NumberFormatException nfe) {
            // expected
        }
    }

    @Test
    public void testValueOfIntWithLargeNumberThrowsNFE() throws Exception {
        try {
            UnsignedShort.valueOf(Integer.MAX_VALUE);
            fail("Expected exception was not thrown");
        } catch (NumberFormatException nfe) {
            // expected
        }
    }

    @Test
    public void testValueOfStringWithNegativeNumberThrowsNFE() throws Exception {
        try {
            UnsignedShort.valueOf("-1");
            fail("Expected exception was not thrown");
        } catch (NumberFormatException nfe) {
            // expected
        }
    }

    @Test
    public void testValueOfStringWithTextThrowsNFE() throws Exception {
        try {
            UnsignedShort.valueOf("TEST");
            fail("Expected exception was not thrown");
        } catch (NumberFormatException nfe) {
            // expected
        }
    }

    @Test
    public void testValueOfStringWithOutOfRangeValueThrowsNFE() throws Exception {
        try {
            UnsignedShort.valueOf("65536");
            fail("Expected exception was not thrown");
        } catch (NumberFormatException nfe) {
            // expected
        }
    }

    @Test
    public void testToUnsignedIntValueFromShort() {
        long longValue = (long) Short.MAX_VALUE + 1;

        assertEquals(0l, UnsignedShort.toUnsignedInt((short) 0));
        assertEquals(65535l, UnsignedShort.toUnsignedInt((short) 65535));
        assertEquals(1l, UnsignedShort.toUnsignedInt((short) 1));
        assertEquals(127l, UnsignedShort.toUnsignedInt((short) 127));
        assertEquals(longValue, UnsignedShort.toUnsignedInt((short) (Short.MAX_VALUE + 1)));
    }

    @Test
    public void testToUnsignedLongValueFromShort() {
        long longValue = (long) Short.MAX_VALUE + 1;

        assertEquals(0l, UnsignedShort.toUnsignedLong((short) 0));
        assertEquals(65535l, UnsignedShort.toUnsignedLong((short) 65535));
        assertEquals(1l, UnsignedShort.toUnsignedLong((short) 1));
        assertEquals(127l, UnsignedShort.toUnsignedLong((short) 127));
        assertEquals(longValue, UnsignedShort.toUnsignedLong((short) (Short.MAX_VALUE + 1)));
    }
}
