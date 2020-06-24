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
package org.apache.qpid.protonj2.buffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonByteBuffer;
import org.apache.qpid.protonj2.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.protonj2.buffer.util.ProtonTestByteBuffer;
import org.junit.Test;

/**
 * Test behavior of the built in ProtonByteBuffer implementation.
 */
public class ProtonByteBufferTest extends ProtonAbstractBufferTest {

    //----- Test Buffer creation ---------------------------------------------//

    @Test
    public void testDefaultConstructor() {
        ProtonBuffer buffer = new ProtonByteBuffer();

        assertEquals(0, buffer.getReadableBytes());
        assertEquals(ProtonByteBuffer.DEFAULT_CAPACITY, buffer.capacity());
        assertEquals(ProtonByteBuffer.DEFAULT_MAXIMUM_CAPACITY, buffer.maxCapacity());

        assertTrue(buffer.hasArray());
        assertNotNull(buffer.getArray());
        assertEquals(0, buffer.getArrayOffset());
    }

    @Test
    public void testConstructorCapacityAndMaxCapacityAllocatesArray() {
        int baseCapaity = ProtonByteBuffer.DEFAULT_CAPACITY + 10;
        ProtonBuffer buffer = new ProtonByteBuffer(baseCapaity, baseCapaity + 100);

        assertEquals(0, buffer.getReadableBytes());
        assertEquals(baseCapaity, buffer.capacity());
        assertEquals(baseCapaity + 100, buffer.maxCapacity());

        assertTrue(buffer.hasArray());
        assertNotNull(buffer.getArray());
        assertEquals(0, buffer.getArrayOffset());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorCapacityExceptions() {
        new ProtonByteBuffer(-1);
    }

    @Test
    public void testConstructorCapacityMaxCapacity() {
        ProtonBuffer buffer = new ProtonByteBuffer(
            ProtonByteBuffer.DEFAULT_CAPACITY + 10, ProtonByteBuffer.DEFAULT_MAXIMUM_CAPACITY - 100);

        assertEquals(0, buffer.getReadableBytes());
        assertEquals(ProtonByteBuffer.DEFAULT_CAPACITY + 10, buffer.capacity());
        assertEquals(ProtonByteBuffer.DEFAULT_MAXIMUM_CAPACITY - 100, buffer.maxCapacity());

        assertTrue(buffer.hasArray());
        assertNotNull(buffer.getArray());
        assertEquals(0, buffer.getArrayOffset());
    }

    @Test
    public void testConstructorCapacityMaxCapacityExceptions() {

        try {
            new ProtonByteBuffer(-1, ProtonByteBuffer.DEFAULT_MAXIMUM_CAPACITY);
            fail("Should have thrown an IllegalArgumentException");
        } catch (IllegalArgumentException iae) {}

        try {
            new ProtonByteBuffer(ProtonByteBuffer.DEFAULT_CAPACITY, -1);
            fail("Should have thrown an IllegalArgumentException");
        } catch (IllegalArgumentException iae) {}

        try {
            new ProtonByteBuffer(100, 10);
            fail("Should have thrown an IllegalArgumentException");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testConstructorByteArray() {
        byte[] source = new byte[ProtonByteBuffer.DEFAULT_CAPACITY + 10];

        ProtonBuffer buffer = new ProtonByteBuffer(source);

        assertEquals(source.length, buffer.getReadableBytes());
        assertEquals(ProtonByteBuffer.DEFAULT_CAPACITY + 10, buffer.capacity());
        assertEquals(ProtonByteBuffer.DEFAULT_MAXIMUM_CAPACITY, buffer.maxCapacity());

        assertTrue(buffer.hasArray());
        assertSame(source, buffer.getArray());
        assertEquals(0, buffer.getArrayOffset());
    }

    @Test
    public void testConstructorByteArrayThrowsWhenNull() {
        try {
            new ProtonByteBuffer(null);
            fail("Should throw NullPointerException");
        } catch (NullPointerException npe) {}

        try {
            new ProtonTestByteBuffer(null, 1);
            fail("Should throw NullPointerException");
        } catch (NullPointerException npe) {}

        try {
            new ProtonTestByteBuffer(null, 1, 1);
            fail("Should throw NullPointerException");
        } catch (NullPointerException npe) {}
    }

    //----- Tests for altering buffer capacity -------------------------------//

    @Test
    public void testIncreaseCapacityReallocatesArray() {
        byte[] source = new byte[100];

        ProtonBuffer buffer = new ProtonByteBuffer(source);
        assertEquals(100, buffer.capacity());
        assertTrue(buffer.hasArray());
        assertSame(source, buffer.getArray());

        buffer.capacity(200);
        assertEquals(200, buffer.capacity());
        assertTrue(buffer.hasArray());
        assertNotSame(source, buffer.getArray());

        source = buffer.getArray();

        buffer.capacity(200);
        assertEquals(200, buffer.capacity());
        assertTrue(buffer.hasArray());
        assertSame(source, buffer.getArray());
    }

    @Test
    public void testDecreaseCapacityReallocatesArray() {
        byte[] source = new byte[100];

        ProtonBuffer buffer = new ProtonByteBuffer(source);
        assertEquals(100, buffer.capacity());
        assertEquals(100, buffer.getWriteIndex());
        assertTrue(buffer.hasArray());
        assertSame(source, buffer.getArray());

        buffer.capacity(50);
        assertEquals(50, buffer.capacity());
        assertTrue(buffer.hasArray());
        assertNotSame(source, buffer.getArray());

        // Buffer is truncated but we never read anything so read index stays at front.
        assertEquals(0, buffer.getReadIndex());
        assertEquals(50, buffer.getWriteIndex());
    }

    @Test
    public void testDecreaseCapacityWithReadIndexIndexBeyondNewValueReallocatesArray() {
        byte[] source = new byte[100];

        ProtonBuffer buffer = new ProtonByteBuffer(source);
        assertEquals(100, buffer.capacity());
        assertTrue(buffer.hasArray());
        assertSame(source, buffer.getArray());

        buffer.setReadIndex(60);

        buffer.capacity(50);
        assertEquals(50, buffer.capacity());
        assertTrue(buffer.hasArray());
        assertNotSame(source, buffer.getArray());

        // Buffer should be truncated and read index moves back to end
        assertEquals(50, buffer.getReadIndex());
        assertEquals(50, buffer.getWriteIndex());
    }

    @Test
    public void testDecreaseCapacityWithWriteIndexWithinNewValueReallocatesArray() {
        byte[] source = new byte[100];

        ProtonBuffer buffer = new ProtonByteBuffer(source);
        assertEquals(100, buffer.capacity());
        assertTrue(buffer.hasArray());
        assertSame(source, buffer.getArray());

        buffer.setIndex(10, 30);

        buffer.capacity(50);
        assertEquals(50, buffer.capacity());
        assertTrue(buffer.hasArray());
        assertNotSame(source, buffer.getArray());

        // Buffer should be truncated but index values remain unchanged
        assertEquals(10, buffer.getReadIndex());
        assertEquals(30, buffer.getWriteIndex());
    }

    @Test
    public void testCapacityIncreasesWhenWritesExceedCurrentReallocatesArray() {
        ProtonBuffer buffer = new ProtonByteBuffer(10);

        assertTrue(buffer.hasArray());

        assertEquals(10, buffer.capacity());
        assertEquals(10, buffer.getArray().length);
        assertEquals(Integer.MAX_VALUE, buffer.maxCapacity());

        for (int i = 1; i <= 9; ++i) {
            buffer.writeByte(i);
        }

        assertEquals(10, buffer.capacity());

        buffer.writeByte(10);

        assertEquals(10, buffer.capacity());
        assertEquals(10, buffer.getArray().length);

        buffer.writeByte(11);

        assertTrue(buffer.capacity() > 10);
        assertTrue(buffer.getArray().length > 10);

        assertEquals(11, buffer.getReadableBytes());

        for (int i = 1; i < 12; ++i) {
            assertEquals(i, buffer.readByte());
        }
    }

    //----- Tests for Copy operations ----------------------------------------//

    @Test
    public void testCopyEmptyBufferCopiesBackingArray() {
        ProtonBuffer buffer = new ProtonByteBuffer(10);
        ProtonBuffer copy = buffer.copy();

        assertEquals(buffer.getReadableBytes(), copy.getReadableBytes());

        assertTrue(copy.hasArray());
        assertNotNull(copy.getArray());

        assertNotSame(buffer.getArray(), copy.getArray());
    }

    @Test
    public void testCopyBufferResultsInMatchingBackingArrays() {
        ProtonBuffer buffer = new ProtonByteBuffer(10);

        buffer.writeByte(1);
        buffer.writeByte(2);
        buffer.writeByte(3);
        buffer.writeByte(4);
        buffer.writeByte(5);

        ProtonBuffer copy = buffer.copy();

        assertEquals(buffer.getReadableBytes(), copy.getReadableBytes());

        assertTrue(copy.hasArray());
        assertNotNull(copy.getArray());

        assertNotSame(buffer.getArray(), copy.getArray());

        for(int i = 0; i < 5; ++i) {
            assertEquals(buffer.getArray()[i], copy.getArray()[i]);
        }
    }

    //----- Tests for Buffer duplication -------------------------------------//

    @Test
    public void testDuplicateEmptyBufferRetainsBackingArrayAccess() {
        ProtonBuffer buffer = new ProtonByteBuffer(10);
        ProtonBuffer duplicate = buffer.duplicate();

        assertEquals(buffer.capacity(), duplicate.capacity());
        assertEquals(buffer.getReadableBytes(), duplicate.getReadableBytes());

        assertSame(buffer.getArray(), duplicate.getArray());
        assertEquals(0, buffer.getArrayOffset());
    }

    //----- Tests for conversion to ByteBuffer -------------------------------//

    @Test
    public void testToByteBufferWithDataPresentRetainsBackingArray() {
        ProtonBuffer buffer = new ProtonByteBuffer(10);

        buffer.writeByte(1);
        buffer.writeByte(2);
        buffer.writeByte(3);
        buffer.writeByte(4);
        buffer.writeByte(5);

        ByteBuffer byteBuffer = buffer.toByteBuffer();

        assertEquals(buffer.getReadableBytes(), byteBuffer.limit());

        assertTrue(byteBuffer.hasArray());
        assertNotNull(byteBuffer.array());

        assertSame(buffer.getArray(), byteBuffer.array());
    }

    @Test
    public void testToByteBufferWhenNoDataRetainsBackingArray() {
        ProtonBuffer buffer = new ProtonByteBuffer();
        ByteBuffer byteBuffer = buffer.toByteBuffer();

        assertEquals(buffer.getReadableBytes(), byteBuffer.limit());

        assertTrue(byteBuffer.hasArray());
        assertNotNull(byteBuffer.array());
        assertSame(buffer.getArray(), byteBuffer.array());
    }

    //----- Tests for string conversion --------------------------------------//

    @Test
    public void testToStringFromUTF8WithNonArrayBackedBuffer() throws Exception {
        String sourceString = "Test-String-1";

        ProtonTestByteBuffer buffer = new ProtonTestByteBuffer(false);
        buffer.writeBytes(sourceString.getBytes(StandardCharsets.UTF_8));

        assertFalse(buffer.hasArray());

        String decoded = buffer.toString(StandardCharsets.UTF_8);

        assertEquals(sourceString, decoded);
    }

    //----- Buffer creation implementation required by super-class

    @Override
    protected boolean canAllocateDirectBackedBuffers() {
        return false;
    }

    @Override
    protected ProtonBuffer allocateBuffer(int initialCapacity) {
        return new ProtonByteBuffer(initialCapacity);
    }

    @Override
    protected ProtonBuffer allocateDirectBuffer(int initialCapacity) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected ProtonBuffer allocateBuffer(int initialCapacity, int maxCapacity) {
        return new ProtonByteBuffer(initialCapacity, maxCapacity);
    }

    @Override
    protected ProtonBuffer allocateDirectBuffer(int initialCapacity, int maxCapacity) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected ProtonBuffer wrapBuffer(byte[] array) {
        return ProtonByteBufferAllocator.DEFAULT.wrap(array);
    }
}
