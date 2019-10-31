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
package org.apache.qpid.proton4j.buffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

/**
 * Abstract test base for testing common expected behaviors of ProtonBuffer implementations
 * of ProtonBuffer.
 */
public abstract class ProtonAbstractBufferTest {

    public static final int DEFAULT_CAPACITY = 64;

    //----- Test Buffer creation ---------------------------------------------//

    @Test
    public void testConstructorCapacityAndMaxCapacity() {
        final int baseCapaity = DEFAULT_CAPACITY + 10;

        ProtonBuffer buffer = allocateBuffer(baseCapaity, baseCapaity + 100);

        assertEquals(0, buffer.getReadableBytes());
        assertEquals(baseCapaity, buffer.capacity());
        assertEquals(baseCapaity + 100, buffer.maxCapacity());

        assertTrue(buffer.hasArray());
        assertNotNull(buffer.getArray());
        assertEquals(0, buffer.getArrayOffset());
    }

    @Test
    public void testBufferRespectsMaxCapacity() {
        ProtonBuffer buffer = allocateBuffer(5, 10);

        assertEquals(0, buffer.getReadableBytes());
        assertEquals(5, buffer.capacity());
        assertEquals(10, buffer.maxCapacity());

        for (int i = 0; i < 10; ++i) {
            buffer.writeByte(i);
        }

        try {
            buffer.writeByte(10);
            fail("Should not be able to write more than the max capacity bytes");
        } catch (IndexOutOfBoundsException iobe) {}
    }

    //----- Tests for altering buffer capacity -------------------------------//

    @Test(expected = IllegalArgumentException.class)
    public void testCapacityEnforceMaxCapacity() {
        ProtonBuffer buffer = allocateBuffer(3, 13);
        assertEquals(13, buffer.maxCapacity());
        assertEquals(3, buffer.capacity());
        buffer.capacity(14);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCapacityNegative() {
        ProtonBuffer buffer = allocateBuffer(3, 13);
        assertEquals(13, buffer.maxCapacity());
        assertEquals(3, buffer.capacity());
        buffer.capacity(-1);
    }

    @Test
    public void testCapacityDecrease() {
        ProtonBuffer buffer = allocateBuffer(3, 13);
        assertEquals(13, buffer.maxCapacity());
        assertEquals(3, buffer.capacity());
        buffer.capacity(2);
        assertEquals(2, buffer.capacity());
        assertEquals(13, buffer.maxCapacity());
    }

    @Test
    public void testCapacityIncrease() {
        ProtonBuffer buffer = allocateBuffer(3, 13);
        assertEquals(13, buffer.maxCapacity());
        assertEquals(3, buffer.capacity());
        buffer.capacity(4);
        assertEquals(4, buffer.capacity());
        assertEquals(13, buffer.maxCapacity());
    }

    //----- Tests for altering buffer properties -----------------------------//

    @Test
    public void testSetReadIndexWithNegative() {
        ProtonBuffer buffer = allocateDefaultBuffer();
        try {
            buffer.setReadIndex(-1);
            fail("Should not accept negative values");
        } catch (IndexOutOfBoundsException e) {}
    }

    @Test
    public void testSetReadIndexGreaterThanCapacity() {
        ProtonBuffer buffer = allocateDefaultBuffer();
        try {
            buffer.setReadIndex(buffer.capacity() + buffer.capacity());
            fail("Should not accept values bigger than capacity");
        } catch (IndexOutOfBoundsException e) {}
    }

    @Test
    public void testSetWriteIndexWithNegative() {
        ProtonBuffer buffer = allocateDefaultBuffer();
        try {
            buffer.setWriteIndex(-1);
            fail("Should not accept negative values");
        } catch (IndexOutOfBoundsException e) {}
    }

    @Test
    public void testSetWriteIndexGreaterThanCapacity() {
        ProtonBuffer buffer = allocateDefaultBuffer();
        try {
            buffer.setWriteIndex(buffer.capacity() + buffer.capacity());
            fail("Should not accept values bigger than capacity");
        } catch (IndexOutOfBoundsException e) {}
    }

    @Test
    public void testSetIndexWithNegativeReadIndex() {
        ProtonBuffer buffer = allocateDefaultBuffer();
        try {
            buffer.setIndex(-1, 0);
            fail("Should not accept negative values");
        } catch (IndexOutOfBoundsException e) {}
    }

    @Test
    public void testSetIndexWithNegativeWriteIndex() {
        ProtonBuffer buffer = allocateDefaultBuffer();
        try {
            buffer.setIndex(0, -1);
            fail("Should not accept negative values");
        } catch (IndexOutOfBoundsException e) {}
    }

    @Test
    public void testSetIndexWithReadIndexBiggerThanWrite() {
        ProtonBuffer buffer = allocateDefaultBuffer();
        try {
            buffer.setIndex(50, 40);
            fail("Should not accept bigger read index values");
        } catch (IndexOutOfBoundsException e) {}
    }

    @Test
    public void testSetIndexWithWriteIndexBiggerThanCapacity() {
        ProtonBuffer buffer = allocateDefaultBuffer();
        try {
            buffer.setIndex(0, buffer.capacity() + 1);
            fail("Should not accept write index bigger than capacity");
        } catch (IndexOutOfBoundsException e) {}
    }

    @Test
    public void testIsReadable() {
        ProtonBuffer buffer = allocateDefaultBuffer();
        assertFalse(buffer.isReadable());
        buffer.writeBoolean(false);
        assertTrue(buffer.isReadable());
    }

    @Test
    public void testIsReadableWithAmount() {
        ProtonBuffer buffer = allocateDefaultBuffer();
        assertFalse(buffer.isReadable(1));
        buffer.writeBoolean(false);
        assertTrue(buffer.isReadable(1));
        assertFalse(buffer.isReadable(2));
    }

    @Test
    public void testIsWriteable() {
        ProtonBuffer buffer = allocateDefaultBuffer();
        assertTrue(buffer.isWritable());
        buffer.setWriteIndex(buffer.capacity());
        assertFalse(buffer.isWritable());
    }

    @Test
    public void testIsWriteableWithAmount() {
        ProtonBuffer buffer = allocateDefaultBuffer();
        assertTrue(buffer.isWritable());
        buffer.setWriteIndex(buffer.capacity() - 1);
        assertTrue(buffer.isWritable(1));
        assertFalse(buffer.isWritable(2));
    }

    @Test
    public void testClear() {
        ProtonBuffer buffer = allocateDefaultBuffer();
        assertEquals(0, buffer.getReadIndex());
        assertEquals(0, buffer.getWriteIndex());
        buffer.setIndex(10, 20);
        assertEquals(10, buffer.getReadIndex());
        assertEquals(20, buffer.getWriteIndex());
        buffer.clear();
        assertEquals(0, buffer.getReadIndex());
        assertEquals(0, buffer.getWriteIndex());
    }

    @Test
    public void testSkipBytes() {
        ProtonBuffer buffer = allocateDefaultBuffer();
        buffer.setWriteIndex(buffer.capacity() / 2);
        assertEquals(0, buffer.getReadIndex());
        buffer.skipBytes(buffer.capacity() / 2);
        assertEquals(buffer.capacity() / 2, buffer.getReadIndex());
    }

    @Test
    public void testSkipBytesBeyondReable() {
        ProtonBuffer buffer = allocateDefaultBuffer();
        buffer.setWriteIndex(buffer.capacity() / 2);
        assertEquals(0, buffer.getReadIndex());

        try {
            buffer.skipBytes(buffer.getReadableBytes() + 50);
            fail("Should not be able to skip beyond write index");
        } catch (IndexOutOfBoundsException e) {}
    }

    //----- Tests need to define these allocation methods

    /**
     * @return a ProtonBuffer allocated with defaults for capacity and max-capacity.
     */
    protected abstract ProtonBuffer allocateDefaultBuffer();

    /**
     * @param initialCapacity the initial capacity to assign the returned buffer
     *
     * @return a ProtonBuffer allocated with the given capacity and a default max-capacity.
     */
    protected abstract ProtonBuffer allocateBuffer(int initialCapacity);

    /**
     * @param initialCapacity the initial capacity to assign the returned buffer
     * @param maxCapacity the maximum capacity the buffer is allowed to grow to
     *
     * @return a ProtonBuffer allocated with the given capacity and the given max-capacity.
     */
    protected abstract ProtonBuffer allocateBuffer(int initialCapacity, int maxCapacity);

}
