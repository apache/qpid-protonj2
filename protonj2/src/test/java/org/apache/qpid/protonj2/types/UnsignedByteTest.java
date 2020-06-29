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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

public class UnsignedByteTest {

    @Test
    public void testToString() {
        assertEquals("0", UnsignedByte.valueOf((byte) 0).toString());
        assertEquals("255", UnsignedByte.valueOf((byte) 255).toString());
        assertEquals("127", UnsignedByte.valueOf((byte) 127).toString());
    }

    @Test
    public void testHashcode() {
        UnsignedByte ubyte1 = UnsignedByte.valueOf((byte) 1);
        UnsignedByte ubyte2 = UnsignedByte.valueOf((byte) 2);

        assertNotEquals(ubyte1, ubyte2);
        assertNotEquals(ubyte1.hashCode(), ubyte2.hashCode());

        assertEquals(ubyte1.hashCode(), UnsignedByte.valueOf((byte) 1).hashCode());
        assertEquals(ubyte2.hashCode(), UnsignedByte.valueOf((byte) 2).hashCode());
    }

    @Test
    public void testShortValue() {
        assertEquals((short) 0, UnsignedByte.valueOf((byte) 0).shortValue());
        assertEquals((short) 255, UnsignedByte.valueOf((byte) 255).shortValue());
        assertEquals((short) 1, UnsignedByte.valueOf((byte) 1).shortValue());
        assertEquals((short) 127, UnsignedByte.valueOf((byte) 127).shortValue());
    }

    @Test
    public void testIntValue() {
        assertEquals(0, UnsignedByte.valueOf((byte) 0).intValue());
        assertEquals(255, UnsignedByte.valueOf((byte) 255).intValue());
        assertEquals(1, UnsignedByte.valueOf((byte) 1).intValue());
        assertEquals(127, UnsignedByte.valueOf((byte) 127).intValue());
    }

    @Test
    public void testLongValue() {
        assertEquals(0l, UnsignedByte.valueOf((byte) 0).longValue());
        assertEquals(255l, UnsignedByte.valueOf((byte) 255).longValue());
        assertEquals(1l, UnsignedByte.valueOf((byte) 1).longValue());
        assertEquals(127l, UnsignedByte.valueOf((byte) 127).longValue());
    }

    @Test
    public void testCompareToByte() {
        assertTrue(UnsignedByte.valueOf((byte) 255).compareTo((byte) 255) == 0);
        assertTrue(UnsignedByte.valueOf((byte) 0).compareTo((byte) 0) == 0);
        assertTrue(UnsignedByte.valueOf((byte) 127).compareTo((byte) 126) > 0);
        assertTrue(UnsignedByte.valueOf((byte) 32).compareTo((byte) 64) < 0);
        assertTrue(UnsignedByte.valueOf((byte) 255).compareTo((byte) 127) > 0);
        assertTrue(UnsignedByte.valueOf((byte) 126).compareTo((byte) 255) < 0);
        assertTrue(UnsignedByte.valueOf((byte) 255).compareTo((byte) 0) > 0);
        assertTrue(UnsignedByte.valueOf((byte) 0).compareTo((byte) 255) < 0);
    }

    @Test
    public void testCompareToUnsignedByte() {
        assertTrue(UnsignedByte.valueOf((byte) 255).compareTo(UnsignedByte.valueOf((byte) 255)) == 0);
        assertTrue(UnsignedByte.valueOf((byte) 0).compareTo(UnsignedByte.valueOf((byte) 0)) == 0);
        assertTrue(UnsignedByte.valueOf((byte) 127).compareTo(UnsignedByte.valueOf((byte) 126)) > 0);
        assertTrue(UnsignedByte.valueOf((byte) 32).compareTo(UnsignedByte.valueOf((byte) 64)) < 0);
        assertTrue(UnsignedByte.valueOf((byte) 255).compareTo(UnsignedByte.valueOf((byte) 127)) > 0);
        assertTrue(UnsignedByte.valueOf((byte) 126).compareTo(UnsignedByte.valueOf((byte) 255)) < 0);
        assertTrue(UnsignedByte.valueOf((byte) 255).compareTo(UnsignedByte.valueOf((byte) 0)) > 0);
        assertTrue(UnsignedByte.valueOf((byte) 0).compareTo(UnsignedByte.valueOf((byte) 255)) < 0);
    }

    @Test
    public void testValueOfStringWithNegativeNumberThrowsNFE() throws Exception {
        try {
            UnsignedByte.valueOf("-1");
            fail("Expected exception was not thrown");
        } catch (NumberFormatException nfe) {
            // expected
        }
    }

    @Test
    public void testValueOfStringWithTextThrowsNFE() throws Exception {
        try {
            UnsignedByte.valueOf("TEST");
            fail("Expected exception was not thrown");
        } catch (NumberFormatException nfe) {
            // expected
        }
    }

    @Test
    public void testValueOfStringWithOutOfRangeValueThrowsNFE() throws Exception {
        try {
            UnsignedByte.valueOf("256");
            fail("Expected exception was not thrown");
        } catch (NumberFormatException nfe) {
            // expected
        }
    }

    @Test
    public void testToUnsignedShortValueFromByte() {
        short shortValue = Byte.MAX_VALUE + 1;

        assertEquals(0, UnsignedByte.toUnsignedShort((byte) 0));
        assertEquals(255, UnsignedByte.toUnsignedShort((byte) 255));
        assertEquals(1, UnsignedByte.toUnsignedShort((byte) 1));
        assertEquals(127, UnsignedByte.toUnsignedShort((byte) 127));
        assertEquals(shortValue, UnsignedByte.toUnsignedShort((byte) (Byte.MAX_VALUE + 1)));
    }

    @Test
    public void testToUnsignedIntValueFromByte() {
        int intValue = Byte.MAX_VALUE + 1;

        assertEquals(0, UnsignedByte.toUnsignedInt((byte) 0));
        assertEquals(255, UnsignedByte.toUnsignedInt((byte) 255));
        assertEquals(1, UnsignedByte.toUnsignedInt((byte) 1));
        assertEquals(127, UnsignedByte.toUnsignedInt((byte) 127));
        assertEquals(intValue, UnsignedByte.toUnsignedInt((byte) (Byte.MAX_VALUE + 1)));
    }

    @Test
    public void testToUnsignedLongValueFromByte() {
        long longValue = (long) Byte.MAX_VALUE + 1;

        assertEquals(0l, UnsignedByte.toUnsignedLong((byte) 0));
        assertEquals(255l, UnsignedByte.toUnsignedLong((byte) 255));
        assertEquals(1l, UnsignedByte.toUnsignedLong((byte) 1));
        assertEquals(127l, UnsignedByte.toUnsignedLong((byte) 127));
        assertEquals(longValue, UnsignedByte.toUnsignedLong((byte) (Byte.MAX_VALUE + 1)));
    }
}
