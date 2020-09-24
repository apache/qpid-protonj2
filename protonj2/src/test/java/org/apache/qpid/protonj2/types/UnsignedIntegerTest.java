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
package org.apache.qpid.protonj2.types;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.Test;

public class UnsignedIntegerTest {

    @Test
    public void testToString() {
        assertEquals("0",  UnsignedInteger.valueOf(0).toString());
        assertEquals("65535", UnsignedInteger.valueOf(65535).toString());
        assertEquals("127", UnsignedInteger.valueOf(127).toString());
    }

    @SuppressWarnings("unlikely-arg-type")
    @Test
    public void testEquals() {
        UnsignedInteger uint1 = UnsignedInteger.valueOf(1);
        UnsignedInteger uint2 = UnsignedInteger.valueOf(2);

        assertEquals(uint1, uint1);
        assertEquals(uint1, UnsignedInteger.valueOf(1));
        assertEquals(uint1, UnsignedInteger.valueOf("1"));
        assertFalse(uint1.equals(uint2));
        assertNotEquals(uint1.hashCode(), uint2.hashCode());

        assertEquals(uint1.hashCode(), UnsignedInteger.valueOf(1).hashCode());
        assertEquals(uint2.hashCode(), UnsignedInteger.valueOf(2).hashCode());

        assertFalse(uint1.equals(null));
        assertFalse(uint1.equals("test"));
    }

    @Test
    public void testValueOfFromLong() {
        long longValue = (long) Integer.MAX_VALUE + 1;

        UnsignedInteger uint1 = UnsignedInteger.valueOf(1l);
        UnsignedInteger uint2 = UnsignedInteger.valueOf(longValue);

        assertEquals(1, uint1.intValue());
        assertEquals(longValue, uint2.longValue());
    }

    @Test
    public void testValueOfFromString() {
        long longValue = (long) Integer.MAX_VALUE + 1;

        UnsignedInteger uint1 = UnsignedInteger.valueOf("1");
        UnsignedInteger uint2 = UnsignedInteger.valueOf(String.valueOf(longValue));

        assertEquals(1, uint1.intValue());
        assertEquals(longValue, uint2.longValue());
    }

    @Test
    public void testHashcode() {
        UnsignedInteger uint1 = UnsignedInteger.valueOf(1);
        UnsignedInteger uint2 = UnsignedInteger.valueOf(2);

        assertNotEquals(uint1, uint2);
        assertNotEquals(uint1.hashCode(), uint2.hashCode());

        assertEquals(uint1.hashCode(), UnsignedInteger.valueOf(1).hashCode());
        assertEquals(uint2.hashCode(), UnsignedInteger.valueOf(2).hashCode());
    }

    @Test
    public void testAdd() {
        UnsignedInteger uint1 = UnsignedInteger.valueOf(1);
        UnsignedInteger result = uint1.add(uint1);
        assertEquals(2, result.intValue());
    }

    @Test
    public void testSubtract() {
        UnsignedInteger uint1 = UnsignedInteger.valueOf(1);
        UnsignedInteger result = uint1.subtract(uint1);
        assertEquals(0, result.intValue());
    }

    @Test
    public void testCompareToPrimitiveInt() {
        UnsignedInteger uint1 = UnsignedInteger.valueOf(1);
        assertEquals(0, uint1.compareTo(1));
        assertEquals(1, uint1.compareTo(0));
        assertEquals(-1, uint1.compareTo(2));
    }

    @Test
    public void testCompareToPrimitiveLong() {
        UnsignedInteger uint1 = UnsignedInteger.valueOf(1);
        assertEquals(0, uint1.compareTo(1l));
        assertEquals(1, uint1.compareTo(0l));
        assertEquals(-1, uint1.compareTo(2l));
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
        long longValue = (long) Integer.MAX_VALUE + 1;

        assertEquals(0l, UnsignedInteger.valueOf(0).longValue());
        assertEquals(65535l, UnsignedInteger.valueOf(65535).longValue());
        assertEquals(1l, UnsignedInteger.valueOf(1).longValue());
        assertEquals(127l, UnsignedInteger.valueOf(127).longValue());
        assertEquals(longValue, UnsignedInteger.valueOf(Integer.MAX_VALUE + 1).longValue());
    }

    @Test
    public void testToUnsignedLongValueFromInt() {
        long longValue = (long) Integer.MAX_VALUE + 1;

        assertEquals(0l, UnsignedInteger.toUnsignedLong(0));
        assertEquals(65535l, UnsignedInteger.toUnsignedLong(65535));
        assertEquals(1l, UnsignedInteger.toUnsignedLong(1));
        assertEquals(127l, UnsignedInteger.toUnsignedLong(127));
        assertEquals(longValue, UnsignedInteger.toUnsignedLong(Integer.MAX_VALUE + 1));
    }

    @Test
    public void testFloatValue() {
        assertEquals(0.0f, UnsignedInteger.valueOf(0).floatValue(), 0.0f);
        assertEquals(65535.0f, UnsignedInteger.valueOf(65535).floatValue(), 0.0f);
        assertEquals(1.0f, UnsignedInteger.valueOf(1).floatValue(), 0.0f);
        assertEquals(127.0f, UnsignedInteger.valueOf(127).floatValue(), 0.0f);
    }

    @Test
    public void testDoubleValue() {
        assertEquals(0.0, UnsignedInteger.valueOf(0).doubleValue(), 0.0);
        assertEquals(65535.0, UnsignedInteger.valueOf(65535).doubleValue(), 0.0);
        assertEquals(1.0, UnsignedInteger.valueOf(1).doubleValue(), 0.0);
        assertEquals(127.0, UnsignedInteger.valueOf(127).doubleValue(), 0.0);
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
    public void testCompareToLongLong() {
        assertTrue(UnsignedInteger.compare(65536l, 65536l) == 0);
        assertTrue(UnsignedInteger.compare(0l, 0l) == 0);
        assertTrue(UnsignedInteger.compare(1l, 2l) < 0);
        assertTrue(UnsignedInteger.compare(127l, 32l) > 0);
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
