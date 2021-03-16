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
package org.apache.qpid.protonj2.engine.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.qpid.protonj2.types.UnsignedInteger;
import org.junit.jupiter.api.Test;

class SequenceNumberTest {

    @Test
    void testGetFloatValue() {
        SequenceNumber number = new SequenceNumber(Float.floatToIntBits(3.14f));
        assertEquals(3.14f, number.floatValue());
    }

    @Test
    void testGetDoubleValue() {
        SequenceNumber number = new SequenceNumber((int) Double.doubleToLongBits(3.14));
        assertTrue(number.doubleValue() != 0);
    }

    @Test
    void testCreateSequenceNumber() {
        SequenceNumber number = new SequenceNumber(1);
        assertTrue(number.equals(1));
        assertEquals(1, number.intValue());
    }

    @Test
    void testIncrement() {
        SequenceNumber number = new SequenceNumber(1);
        assertTrue(number.equals(1));
        assertEquals(new SequenceNumber(2), number.increment());
    }

    @Test
    void testDecrement() {
        SequenceNumber number = new SequenceNumber(1);
        assertTrue(number.equals(1));
        assertEquals(new SequenceNumber(0), number.decrement());
    }

    @Test
    void testGetAndIncrement() {
        SequenceNumber number = new SequenceNumber(1);
        assertTrue(number.equals(1));
        assertEquals(new SequenceNumber(1), number.getAndIncrement());
        assertTrue(number.equals(2));
    }

    @Test
    void testGetAndDecrement() {
        SequenceNumber number = new SequenceNumber(1);
        assertTrue(number.equals(1));
        assertEquals(new SequenceNumber(1), number.getAndDecrement());
        assertTrue(number.equals(0));
    }

    @SuppressWarnings("unlikely-arg-type")
    @Test
    void testEquals() {
        SequenceNumber number = new SequenceNumber(1);

        assertFalse(number.equals("1"));
        assertFalse(number.equals(0));
        assertTrue(number.equals(1));
        assertFalse(number.equals(2));
        assertFalse(number.equals(new SequenceNumber(0)));
        assertTrue(number.equals(new SequenceNumber(1)));
        assertFalse(number.equals(new SequenceNumber(2)));
    }

    @Test
    void testCompareToInt() {
        SequenceNumber number = new SequenceNumber(1);

        assertEquals(1, number.compareTo(0));
        assertEquals(0, number.compareTo(1));
        assertEquals(-1, number.compareTo(2));
    }

    @Test
    void testCompareToSequenceNumber() {
        SequenceNumber number = new SequenceNumber(1);

        assertEquals(1, number.compareTo(new SequenceNumber(0)));
        assertEquals(0, number.compareTo(new SequenceNumber(1)));
        assertEquals(-1, number.compareTo(new SequenceNumber(2)));
    }

    @Test
    void testCompareToUnsignedInteger() {
        SequenceNumber number = new SequenceNumber(1);

        assertEquals(1, number.compareTo(new UnsignedInteger(0)));
        assertEquals(0, number.compareTo(new UnsignedInteger(1)));
        assertEquals(-1, number.compareTo(new UnsignedInteger(2)));
    }

    @Test
    void testToString() {
        assertEquals(Integer.toUnsignedString(127), new SequenceNumber(127).toString());
        assertEquals(Integer.toUnsignedString(-128), new SequenceNumber(-128).toString());
        assertEquals(Integer.toUnsignedString(0), new SequenceNumber(0).toString());
    }

    @Test
    void testHashCode() {
        assertEquals(Integer.hashCode(127), new SequenceNumber(127).hashCode());
        assertEquals(Integer.hashCode(-128), new SequenceNumber(-128).hashCode());
        assertEquals(Integer.hashCode(0), new SequenceNumber(0).hashCode());
    }
}
