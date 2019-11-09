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

import java.nio.ByteBuffer;

import org.junit.Ignore;
import org.junit.Test;

/**
 * Test class for the NIO ByteBuffer wrapper class
 */
public class ProtonNioByteBufferTest extends ProtonAbstractBufferTest {

    @Ignore("Cannot change capacity on Nio buffer")
    @Override
    @Test
    public void testConstructWithDifferingCapacityAndMaxCapacity() {
    }

    @Ignore("Cannot change capacity on Nio buffer")
    @Override
    @Test
    public void testBufferRespectsMaxCapacityAfterGrowingToFit() {
    }

    @Ignore("Cannot change capacity on Nio buffer")
    @Override
    @Test
    public void testIncreaseCapacity() {
    }

    @Ignore("Cannot change capacity on Nio buffer")
    @Override
    @Test
    public void testDecreaseCapacity() {
    }

    @Ignore("Cannot change capacity on Nio buffer")
    @Override
    @Test
    public void testDecreaseCapacityWithWriteIndexWithinNewValue() {
    }

    @Ignore("Cannot change capacity on Nio buffer")
    @Override
    @Test
    public void testCapacityDecrease() {
    }

    @Ignore("Cannot change capacity on Nio buffer")
    @Override
    @Test
    public void testCapacityIncrease() {
    }

    @Ignore("Cannot change capacity on Nio buffer")
    @Override
    @Test
    public void testDecreaseCapacityWithReadIndexIndexBeyondNewValue() {
    }

    @Ignore("Cannot change capacity on Nio buffer")
    @Override
    @Test
    public void testCapacityIncreasesWhenWritesExceedCurrent() {
    }

    @Override
    @Test(expected = UnsupportedOperationException.class)
    public void testCapacityEnforceMaxCapacity() {
        ProtonBuffer buffer = allocateBuffer(13, 13);
        assertEquals(13, buffer.maxCapacity());
        assertEquals(13, buffer.capacity());
        buffer.capacity(14);
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void testCapacityNegative() {
        ProtonBuffer buffer = allocateBuffer(13, 13);
        assertEquals(13, buffer.maxCapacity());
        assertEquals(13, buffer.capacity());
        buffer.capacity(-1);
    }

    @Override
    protected ProtonBuffer allocateDefaultBuffer() {
        return new ProtonNioByteBuffer(ByteBuffer.allocate(DEFAULT_CAPACITY), 0);
    }

    @Override
    protected ProtonBuffer allocateBuffer(int initialCapacity) {
        return new ProtonNioByteBuffer(ByteBuffer.allocate(initialCapacity), 0);
    }

    @Override
    protected ProtonBuffer allocateBuffer(int initialCapacity, int maxCapacity) {
        if (initialCapacity != maxCapacity) {
            throw new UnsupportedOperationException("NIO buffer wrappers cannot grow");
        }

        return new ProtonNioByteBuffer(ByteBuffer.allocate(initialCapacity), 0);
    }

    @Override
    protected ProtonBuffer wrapBuffer(byte[] array) {
        return new ProtonNioByteBuffer(ByteBuffer.wrap(array));
    }
}
