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
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.math.BigInteger;

import org.junit.jupiter.api.Test;

public class UnsignedLongTest {

    private static final byte[] TWO_TO_64_PLUS_ONE_BYTES =
        new byte[] { 1, 0, 0, 0, 0, 0, 0, 0, 1 };
    private static final byte[] TWO_TO_64_MINUS_ONE_BYTES =
        new byte[] { 1, 1, 1, 1, 1, 1, 1, 1 };

    @Test
    public void testToString() {
        assertEquals("0",  UnsignedLong.valueOf(0).toString());
        assertEquals("65535", UnsignedLong.valueOf(65535).toString());
        assertEquals("127", UnsignedLong.valueOf(127).toString());
    }

    @Test
    public void testEquals() {
        UnsignedLong ubyte1 = UnsignedLong.valueOf(1);
        UnsignedLong ubyte2 = UnsignedLong.valueOf(2);

        assertEquals(ubyte1, ubyte1);
        assertNotEquals(ubyte1, ubyte2);
        assertNotEquals(ubyte1, ubyte2);

        assertNotEquals(ubyte1, "test");
        assertFalse(ubyte1.equals(null));

        assertEquals(ubyte1, UnsignedLong.valueOf(1));
        assertEquals(ubyte2, UnsignedLong.valueOf(2));

        assertSame(ubyte1, UnsignedLong.valueOf(1));
        assertSame(ubyte2, UnsignedLong.valueOf(2));

        UnsignedLong ubyte3 = UnsignedLong.valueOf(32767);
        UnsignedLong ubyte4 = UnsignedLong.valueOf(32767);

        assertNotSame(ubyte3, UnsignedLong.valueOf(32767));
        assertNotSame(ubyte4, UnsignedLong.valueOf(32767));

        assertEquals(ubyte3, UnsignedLong.valueOf(32767));
        assertEquals(ubyte4, UnsignedLong.valueOf(32767));
    }

    @Test
    public void testHashcode() {
        UnsignedLong ubyte1 = UnsignedLong.valueOf((short) 1);
        UnsignedLong ubyte2 = UnsignedLong.valueOf((short) 2);

        assertNotEquals(ubyte1, ubyte2);
        assertNotEquals(ubyte1.hashCode(), ubyte2.hashCode());

        assertEquals(ubyte1.hashCode(), UnsignedLong.valueOf((short) 1).hashCode());
        assertEquals(ubyte2.hashCode(), UnsignedLong.valueOf((short) 2).hashCode());
    }

    @Test
    public void testShortValue() {
        assertEquals((short) 0, UnsignedLong.valueOf(0).shortValue());
        assertEquals((short) 65535, UnsignedLong.valueOf(65535).shortValue());
        assertEquals((short) 1, UnsignedLong.valueOf(1).shortValue());
        assertEquals((short) 127, UnsignedLong.valueOf(127).shortValue());
    }

    @Test
    public void testIntValue() {
        assertEquals(0, UnsignedLong.valueOf(0).intValue());
        assertEquals(65535, UnsignedLong.valueOf(65535).intValue());
        assertEquals(1, UnsignedLong.valueOf(1).intValue());
        assertEquals(127, UnsignedLong.valueOf(127).intValue());
    }

    @Test
    public void testLongValue() {
        assertEquals(0l, UnsignedLong.valueOf(0).longValue());
        assertEquals(65535l, UnsignedLong.valueOf(65535).longValue());
        assertEquals(1l, UnsignedLong.valueOf(1).longValue());
        assertEquals(127l, UnsignedLong.valueOf(127).longValue());
    }

    @Test
    public void testFloatValue() {
        assertEquals(0, UnsignedLong.valueOf(0).floatValue());
        assertEquals(65535, UnsignedLong.valueOf(65535).floatValue());
        assertEquals(1, UnsignedLong.valueOf(1).floatValue());
        assertEquals(127, UnsignedLong.valueOf(127).floatValue());
    }

    @Test
    public void testDoubleValue() {
        assertEquals(0, UnsignedLong.valueOf(0).doubleValue());
        assertEquals(65535, UnsignedLong.valueOf(65535).doubleValue());
        assertEquals(1, UnsignedLong.valueOf(1).doubleValue());
        assertEquals(127, UnsignedLong.valueOf(127).doubleValue());
    }

    @Test
    public void testCompareToByte() {
        assertTrue(UnsignedLong.valueOf(255).compareTo(255) == 0);
        assertTrue(UnsignedLong.valueOf(0).compareTo(0) == 0);
        assertTrue(UnsignedLong.valueOf(127).compareTo(126) > 0);
        assertTrue(UnsignedLong.valueOf(32).compareTo(64) < 0);
    }

    @Test
    public void testCompareToUnsignedInteger() {
        assertTrue(UnsignedLong.valueOf(65535).compareTo(UnsignedLong.valueOf(65535)) == 0);
        assertTrue(UnsignedLong.valueOf(0).compareTo(UnsignedLong.valueOf(0)) == 0);
        assertTrue(UnsignedLong.valueOf(127).compareTo(UnsignedLong.valueOf(126)) > 0);
        assertTrue(UnsignedLong.valueOf(32).compareTo(UnsignedLong.valueOf(64)) < 0);
    }

    @Test
    public void testCompareLongs() {
        assertTrue(UnsignedLong.compare(65535, 65535) == 0);
        assertTrue(UnsignedLong.compare(0, 0) == 0);
        assertTrue(UnsignedLong.compare(127, 126) > 0);
        assertTrue(UnsignedLong.compare(32, 64) < 0);
    }

    @Test
    public void testValueOfStringWithNegativeNumberThrowsNFE() throws Exception {
        assertEquals(Long.MAX_VALUE, UnsignedLong.valueOf("9223372036854775807").longValue());

        try {
            UnsignedLong.valueOf("-1");
            fail("Expected exception was not thrown");
        } catch (NumberFormatException nfe) {
            // expected
        }
    }

    @Test
    public void testValueOfBigIntegerWithNegativeNumberThrowsNFE() throws Exception {
        try {
            UnsignedLong.valueOf(BigInteger.valueOf(-1L));
            fail("Expected exception was not thrown");
        } catch (NumberFormatException nfe) {
            // expected
        }
    }

    @Test
    public void testValuesOfStringWithinRangeSucceed() throws Exception {
        // check 0 (min) to confirm success
        UnsignedLong min = UnsignedLong.valueOf("0");
        assertEquals(0, min.longValue(), "unexpected value");

        // check 2^64 -1 (max) to confirm success
        BigInteger onLimit = new BigInteger(TWO_TO_64_MINUS_ONE_BYTES);
        String onLimitString = onLimit.toString();
        UnsignedLong max = UnsignedLong.valueOf(onLimitString);
        assertEquals(onLimit, max.bigIntegerValue(), "unexpected value");
    }

    @Test
    public void testValuesOfBigIntegerWithinRangeSucceed() throws Exception {
        // check 0 (min) to confirm success
        UnsignedLong min = UnsignedLong.valueOf(BigInteger.ZERO);
        assertEquals(0, min.longValue(), "unexpected value");

        // check 2^64 -1 (max) to confirm success
        BigInteger onLimit = new BigInteger(TWO_TO_64_MINUS_ONE_BYTES);
        UnsignedLong max = UnsignedLong.valueOf(onLimit);
        assertEquals(onLimit, max.bigIntegerValue(), "unexpected value");

        // check Long.MAX_VALUE to confirm success
        UnsignedLong longMax = UnsignedLong.valueOf(BigInteger.valueOf(Long.MAX_VALUE));
        assertEquals(Long.MAX_VALUE, longMax.longValue(), "unexpected value");
    }

    @Test
    public void testValueOfStringAboveMaxValueThrowsNFE() throws Exception {
        // 2^64 + 1 (value 2 over max)
        BigInteger aboveLimit = new BigInteger(TWO_TO_64_PLUS_ONE_BYTES);
        try {
            UnsignedLong.valueOf(aboveLimit.toString());
            fail("Expected exception was not thrown");
        } catch (NumberFormatException nfe) {
            // expected
        }

        // 2^64 (value 1 over max)
        aboveLimit = aboveLimit.subtract(BigInteger.ONE);
        try {
            UnsignedLong.valueOf(aboveLimit.toString());
            fail("Expected exception was not thrown");
        } catch (NumberFormatException nfe) {
            // expected
        }
    }

    @Test
    public void testValueOfBigIntegerAboveMaxValueThrowsNFE() throws Exception {
        // 2^64 + 1 (value 2 over max)
        BigInteger aboveLimit = new BigInteger(TWO_TO_64_PLUS_ONE_BYTES);
        try {
            UnsignedLong.valueOf(aboveLimit);
            fail("Expected exception was not thrown");
        } catch (NumberFormatException nfe) {
            // expected
        }

        // 2^64 (value 1 over max)
        aboveLimit = aboveLimit.subtract(BigInteger.ONE);
        try {
            UnsignedLong.valueOf(aboveLimit);
            fail("Expected exception was not thrown");
        } catch (NumberFormatException nfe) {
            // expected
        }
    }
}
