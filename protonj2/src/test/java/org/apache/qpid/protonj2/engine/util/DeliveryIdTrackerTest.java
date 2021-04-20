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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.qpid.protonj2.types.UnsignedInteger;
import org.junit.jupiter.api.Test;

class DeliveryIdTrackerTest {

    @Test
    void testCreateEmptyTracker() {
        DeliveryIdTracker tracker = new DeliveryIdTracker();
        assertTrue(tracker.isEmpty());
    }

    @Test
    void testResetPutsTrackerInEmptyState() {
        DeliveryIdTracker tracker = new DeliveryIdTracker(1);
        assertFalse(tracker.isEmpty());
        tracker.reset();
        assertTrue(tracker.isEmpty());
    }

    @Test
    void testResetAndSetNewTrackerInEmptyState() {
        DeliveryIdTracker tracker = new DeliveryIdTracker(1);
        assertFalse(tracker.isEmpty());
        tracker.reset();
        assertTrue(tracker.isEmpty());
        tracker.set(255);
        assertFalse(tracker.isEmpty());
        assertEquals(255, tracker.intValue());
        tracker.set(32767);
        assertFalse(tracker.isEmpty());
        assertEquals(32767, tracker.intValue());
    }

    @Test
    void testCreateNonEmptyTracker() {
        DeliveryIdTracker tracker = new DeliveryIdTracker(1);
        assertFalse(tracker.isEmpty());
        assertEquals(1, tracker.byteValue());
        assertEquals(1, tracker.intValue());
        assertEquals(1, tracker.longValue());
    }

    @Test
    void testToString() {
        assertEquals(Integer.toUnsignedString(127), new DeliveryIdTracker(127).toString());
        assertEquals(Integer.toUnsignedString(-128), new DeliveryIdTracker(-128).toString());
        assertEquals(Integer.toUnsignedString(0), new DeliveryIdTracker(0).toString());
    }

    @Test
    void testHashCode() {
        assertEquals(Integer.hashCode(127), new DeliveryIdTracker(127).hashCode());
        assertEquals(Integer.hashCode(-128), new DeliveryIdTracker(-128).hashCode());
        assertEquals(Integer.hashCode(0), new DeliveryIdTracker(0).hashCode());
    }

    @Test
    void testToUnsignedInteger() {
        DeliveryIdTracker tracker = new DeliveryIdTracker(127);
        UnsignedInteger conversion = tracker.toUnsignedInteger();
        assertEquals(127, conversion.byteValue());
        assertEquals(127, conversion.intValue());
        assertEquals(127, conversion.longValue());

        assertNull(new DeliveryIdTracker().toUnsignedInteger());
    }

    @Test
    void testGetFloatValue() {
        DeliveryIdTracker tracker = new DeliveryIdTracker(Float.floatToIntBits(3.14f));
        assertFalse(tracker.isEmpty());
        assertEquals(3.14f, tracker.floatValue());
    }

    @Test
    void testGetDoubleValue() {
        DeliveryIdTracker tracker = new DeliveryIdTracker((int) Double.doubleToLongBits(3.14));
        assertFalse(tracker.isEmpty());
        assertTrue(tracker.doubleValue() != 0);
    }

    @Test
    void testCompareTo() {
        DeliveryIdTracker tracker = new DeliveryIdTracker(1);

        assertEquals(1, tracker.compareTo(0));
        assertEquals(0, tracker.compareTo(1));
        assertEquals(-1, tracker.compareTo(2));

        DeliveryIdTracker empty = new DeliveryIdTracker();
        assertEquals(-1, empty.compareTo(0));
        assertEquals(-1, empty.compareTo(1));
        assertEquals(-1, empty.compareTo(2));
    }

    @Test
    void testCompareToDeliveryTracker() {
        DeliveryIdTracker tracker = new DeliveryIdTracker(1);

        assertEquals(1, tracker.compareTo(new DeliveryIdTracker(0)));
        assertEquals(0, tracker.compareTo(new DeliveryIdTracker(1)));
        assertEquals(-1, tracker.compareTo(new DeliveryIdTracker(2)));

        assertEquals(-1, tracker.compareTo(new DeliveryIdTracker()));
        assertEquals(-1, tracker.compareTo(new DeliveryIdTracker()));
        assertEquals(-1, tracker.compareTo(new DeliveryIdTracker()));
        assertEquals(-1, new DeliveryIdTracker().compareTo(tracker));
    }

    @Test
    void testCompareToSequenceNumber() {
        DeliveryIdTracker tracker = new DeliveryIdTracker(1);

        assertEquals(1, tracker.compareTo(new SequenceNumber(0)));
        assertEquals(0, tracker.compareTo(new SequenceNumber(1)));
        assertEquals(-1, tracker.compareTo(new SequenceNumber(2)));

        DeliveryIdTracker empty = new DeliveryIdTracker();

        assertEquals(-1, empty.compareTo(new SequenceNumber(0)));
        assertEquals(-1, empty.compareTo(new SequenceNumber(1)));
        assertEquals(-1, empty.compareTo(new SequenceNumber(2)));
    }

    @Test
    void testCompareToUnsignedInteger() {
        DeliveryIdTracker tracker = new DeliveryIdTracker(1);

        assertEquals(1, tracker.compareTo(new UnsignedInteger(0)));
        assertEquals(0, tracker.compareTo(new UnsignedInteger(1)));
        assertEquals(-1, tracker.compareTo(new UnsignedInteger(2)));

        DeliveryIdTracker empty = new DeliveryIdTracker();

        assertEquals(-1, empty.compareTo(new UnsignedInteger(0)));
        assertEquals(-1, empty.compareTo(new UnsignedInteger(1)));
        assertEquals(-1, empty.compareTo(new UnsignedInteger(2)));
    }

    @Test
    void testEqualsToDeliveryTracker() {
        DeliveryIdTracker tracker = new DeliveryIdTracker(1);

        assertEquals(new DeliveryIdTracker(1), tracker);
        assertNotEquals(new DeliveryIdTracker(0), tracker);
        assertNotEquals(new DeliveryIdTracker(-1), tracker);
    }

    @Test
    void testEqualsToSequenceNumberTracker() {
        DeliveryIdTracker tracker = new DeliveryIdTracker(1);

        assertEquals(tracker, new SequenceNumber(1));
        assertNotEquals(tracker, new SequenceNumber(0));
        assertNotEquals(tracker, new SequenceNumber(-1));
    }

    @Test
    void testEqualsToUnsignedInteger() {
        DeliveryIdTracker tracker = new DeliveryIdTracker(1);

        assertEquals(tracker, new UnsignedInteger(1));
        assertNotEquals(tracker, new UnsignedInteger(0));
        assertNotEquals(tracker, new UnsignedInteger(-1));
    }

    @SuppressWarnings("unlikely-arg-type")
    @Test
    void testEquals() {
        DeliveryIdTracker tracker = new DeliveryIdTracker(1);

        assertFalse(tracker.equals("1"));
        assertFalse(tracker.equals(0));
        assertTrue(tracker.equals(1));
        assertFalse(tracker.equals(2));
        assertFalse(tracker.equals(new DeliveryIdTracker(0)));
        assertTrue(tracker.equals(new DeliveryIdTracker(1)));
        assertFalse(tracker.equals(new DeliveryIdTracker(2)));
    }
}
