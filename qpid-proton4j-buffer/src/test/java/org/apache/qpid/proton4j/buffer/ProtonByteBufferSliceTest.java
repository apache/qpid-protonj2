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
 * Tests for the ProtonByteBufferSlice class
 */
public class ProtonByteBufferSliceTest {

    //----- Test Slice creation ----------------------------------------------//

    @Test
    public void testCreateEmptySlice() {
        ProtonBuffer buffer = new ProtonByteBuffer();

        assertEquals(0, buffer.getReadableBytes());
        assertEquals(ProtonByteBuffer.DEFAULT_CAPACITY, buffer.capacity());
        assertEquals(ProtonByteBuffer.DEFAULT_MAXIMUM_CAPACITY, buffer.maxCapacity());

        assertTrue(buffer.hasArray());
        assertNotNull(buffer.getArray());
        assertEquals(0, buffer.getArrayOffset());

        ProtonBuffer slice = buffer.slice();

        assertEquals(0, slice.getReadableBytes());
        assertEquals(0, slice.capacity());
        assertEquals(0, slice.maxCapacity());

        assertTrue(slice.hasArray());
        assertNotNull(slice.getArray());
        assertEquals(0, slice.getArrayOffset());
    }

    @Test
    public void testCreateSlice() {
        ProtonBuffer buffer = new ProtonByteBuffer();

        buffer.writeBytes(new byte[] {0, 1, 2, 3, 4, 5});
        buffer.setReadIndex(1);

        assertEquals(5, buffer.getReadableBytes());
        assertEquals(ProtonByteBuffer.DEFAULT_CAPACITY, buffer.capacity());
        assertEquals(ProtonByteBuffer.DEFAULT_MAXIMUM_CAPACITY, buffer.maxCapacity());

        assertTrue(buffer.hasArray());
        assertNotNull(buffer.getArray());
        assertEquals(0, buffer.getArrayOffset());

        ProtonBuffer slice = buffer.slice();

        assertEquals(5, slice.getReadableBytes());
        assertEquals(5, slice.capacity());
        assertEquals(5, slice.maxCapacity());

        assertTrue(slice.hasArray());
        assertNotNull(slice.getArray());
        assertEquals(1, slice.getArrayOffset());

        assertEquals(1, slice.readByte());
    }

    @Test
    public void testCreateSliceOfASlice() {
        ProtonBuffer buffer = new ProtonByteBuffer();

        buffer.writeBytes(new byte[] {0, 1, 2, 3, 4, 5});
        buffer.setReadIndex(1);

        assertEquals(5, buffer.getReadableBytes());
        assertEquals(ProtonByteBuffer.DEFAULT_CAPACITY, buffer.capacity());
        assertEquals(ProtonByteBuffer.DEFAULT_MAXIMUM_CAPACITY, buffer.maxCapacity());

        assertTrue(buffer.hasArray());
        assertNotNull(buffer.getArray());
        assertEquals(0, buffer.getArrayOffset());

        ProtonBuffer slice = buffer.slice();
        slice.readByte();

        ProtonBuffer sliceOfSlice = slice.slice();

        assertEquals(4, sliceOfSlice.getReadableBytes());
        assertEquals(4, sliceOfSlice.capacity());
        assertEquals(4, sliceOfSlice.maxCapacity());

        assertTrue(sliceOfSlice.hasArray());
        assertNotNull(sliceOfSlice.getArray());
        assertEquals(2, sliceOfSlice.getArrayOffset());

        assertEquals(2, sliceOfSlice.readByte());
    }

    @Test
    public void testCreateSliceByIndex() {
        ProtonBuffer buffer = new ProtonByteBuffer();

        buffer.writeBytes(new byte[] {0, 1, 2, 3, 4, 5});

        ProtonBuffer slice = buffer.slice(1, 4);

        assertEquals(4, slice.getReadableBytes());
        assertEquals(4, slice.capacity());
        assertEquals(4, slice.maxCapacity());

        assertTrue(slice.hasArray());
        assertNotNull(slice.getArray());
        assertEquals(1, slice.getArrayOffset());

        assertEquals(1, slice.readByte());
    }

    @Test
    public void testCreateSliceByIndexBoundsChecks() {
        ProtonBuffer buffer = new ProtonByteBuffer(6, 6);

        buffer.writeBytes(new byte[] {0, 1, 2, 3, 4, 5});

        try {
            buffer.slice(1, 6);
            fail("Should have thrown IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException iobe) {}

        try {
            buffer.slice(-1, 5);
            fail("Should have thrown IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException iobe) {}

        try {
            buffer.slice(1, -5);
            fail("Should have thrown IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException iobe) {}

        try {
            buffer.slice(-1, -5);
            fail("Should have thrown IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException iobe) {}
    }
}
