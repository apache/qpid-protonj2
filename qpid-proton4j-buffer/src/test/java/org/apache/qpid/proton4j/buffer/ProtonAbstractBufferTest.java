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
