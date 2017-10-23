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
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;

import org.junit.Test;

/**
 * Test behavior of the built in ProtonByteBuffer implementation.
 */
public class ProtonByteBufferTest {

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
    public void testConstructorCapacityAndMaxCapacity() {
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

    //----- Tests for altering buffer properties -----------------------------//

    @Test
    public void testSetReadIndexWithNegative() {
        ProtonBuffer buffer = new ProtonByteBuffer();
        try {
            buffer.setReadIndex(-1);
            fail("Should not accept negative values");
        } catch (IndexOutOfBoundsException e) {}
    }

    @Test
    public void testSetReadIndexGreaterThanCapacity() {
        ProtonBuffer buffer = new ProtonByteBuffer();
        try {
            buffer.setReadIndex(buffer.capacity() + buffer.capacity());
            fail("Should not accept values bigger than capacity");
        } catch (IndexOutOfBoundsException e) {}
    }

    @Test
    public void testSetWriteIndexWithNegative() {
        ProtonBuffer buffer = new ProtonByteBuffer();
        try {
            buffer.setWriteIndex(-1);
            fail("Should not accept negative values");
        } catch (IndexOutOfBoundsException e) {}
    }

    @Test
    public void testSetWriteIndexGreaterThanCapacity() {
        ProtonBuffer buffer = new ProtonByteBuffer();
        try {
            buffer.setWriteIndex(buffer.capacity() + buffer.capacity());
            fail("Should not accept values bigger than capacity");
        } catch (IndexOutOfBoundsException e) {}
    }

    @Test
    public void testSeIndexWithNegativeReadIndex() {
        ProtonBuffer buffer = new ProtonByteBuffer();
        try {
            buffer.setIndex(-1, 0);
            fail("Should not accept negative values");
        } catch (IndexOutOfBoundsException e) {}
    }

    @Test
    public void testSeIndexWithNegativeWriteIndex() {
        ProtonBuffer buffer = new ProtonByteBuffer();
        try {
            buffer.setIndex(0, -1);
            fail("Should not accept negative values");
        } catch (IndexOutOfBoundsException e) {}
    }

    @Test
    public void testSeIndexWithReadIndexBiggerThanWrite() {
        ProtonBuffer buffer = new ProtonByteBuffer();
        try {
            buffer.setIndex(50, 40);
            fail("Should not accept bigger read index values");
        } catch (IndexOutOfBoundsException e) {}
    }

    @Test
    public void testSeIndexWithWriteIndexBiggerThanCapacity() {
        ProtonBuffer buffer = new ProtonByteBuffer();
        try {
            buffer.setIndex(0, buffer.capacity() + 1);
            fail("Should not accept write index bigger than capacity");
        } catch (IndexOutOfBoundsException e) {}
    }

    //----- Tests for altering buffer capacity -------------------------------//

    @Test
    public void testIncreaseCapacity() {
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
    public void testDecreaseCapacity() {
        byte[] source = new byte[100];

        ProtonBuffer buffer = new ProtonByteBuffer(source);
        assertEquals(100, buffer.capacity());
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
    public void testDecreaseCapacityWithReadIndexIndexBeyondNewValue() {
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
    public void testDecreaseCapacityWithWriteIndexWithinNewValue() {
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
    public void testCapacityIncreasesWhenWritesExceedCurrent() {
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

    //----- Write Method Tests -----------------------------------------------//

    @Test
    public void testWriteByte() {
        ProtonBuffer buffer = new ProtonByteBuffer();

        assertEquals(0, buffer.getWriteIndex());
        assertEquals(0, buffer.getReadIndex());

        buffer.writeByte((byte) 56);

        assertEquals(1, buffer.getWriteIndex());
        assertEquals(0, buffer.getReadIndex());

        assertEquals(56, buffer.readByte());

        assertEquals(1, buffer.getWriteIndex());
        assertEquals(1, buffer.getReadIndex());

        assertEquals(0, buffer.getReadableBytes());
    }

    @Test
    public void testWriteBoolean() {
        ProtonBuffer buffer = new ProtonByteBuffer();

        assertEquals(0, buffer.getWriteIndex());
        assertEquals(0, buffer.getReadIndex());

        buffer.writeBoolean(true);
        buffer.writeBoolean(false);

        assertEquals(2, buffer.getWriteIndex());
        assertEquals(0, buffer.getReadIndex());

        assertEquals(true, buffer.readBoolean());

        assertEquals(2, buffer.getWriteIndex());
        assertEquals(1, buffer.getReadIndex());

        assertEquals(1, buffer.getReadableBytes());
        assertEquals(false, buffer.readBoolean());
        assertEquals(2, buffer.getWriteIndex());
        assertEquals(2, buffer.getReadIndex());
    }

    @Test
    public void testWriteShort() {
        ProtonBuffer buffer = new ProtonByteBuffer();

        assertEquals(0, buffer.getWriteIndex());
        assertEquals(0, buffer.getReadIndex());

        buffer.writeShort((short) 42);

        assertEquals(2, buffer.getWriteIndex());
        assertEquals(0, buffer.getReadIndex());

        assertEquals(42, buffer.readShort());

        assertEquals(2, buffer.getWriteIndex());
        assertEquals(2, buffer.getReadIndex());

        assertEquals(0, buffer.getReadableBytes());
    }

    @Test
    public void testWriteInt() {
        ProtonBuffer buffer = new ProtonByteBuffer();

        assertEquals(0, buffer.getWriteIndex());
        assertEquals(0, buffer.getReadIndex());

        buffer.writeInt(72);

        assertEquals(4, buffer.getWriteIndex());
        assertEquals(0, buffer.getReadIndex());

        assertEquals(72, buffer.readInt());

        assertEquals(4, buffer.getWriteIndex());
        assertEquals(4, buffer.getReadIndex());

        assertEquals(0, buffer.getReadableBytes());
    }

    @Test
    public void testWriteLong() {
        ProtonBuffer buffer = new ProtonByteBuffer();

        assertEquals(0, buffer.getWriteIndex());
        assertEquals(0, buffer.getReadIndex());

        buffer.writeLong(500l);

        assertEquals(8, buffer.getWriteIndex());
        assertEquals(0, buffer.getReadIndex());

        assertEquals(500l, buffer.readLong());

        assertEquals(8, buffer.getWriteIndex());
        assertEquals(8, buffer.getReadIndex());

        assertEquals(0, buffer.getReadableBytes());
    }

    @Test
    public void testWriteFloat() {
        ProtonBuffer buffer = new ProtonByteBuffer();

        assertEquals(0, buffer.getWriteIndex());
        assertEquals(0, buffer.getReadIndex());

        buffer.writeFloat(35.5f);

        assertEquals(4, buffer.getWriteIndex());
        assertEquals(0, buffer.getReadIndex());

        assertEquals(35.5f, buffer.readFloat(), 0.4f);

        assertEquals(4, buffer.getWriteIndex());
        assertEquals(4, buffer.getReadIndex());

        assertEquals(0, buffer.getReadableBytes());
    }

    @Test
    public void testWriteDouble() {
        ProtonBuffer buffer = new ProtonByteBuffer();

        assertEquals(0, buffer.getWriteIndex());
        assertEquals(0, buffer.getReadIndex());

        buffer.writeDouble(1.66);

        assertEquals(8, buffer.getWriteIndex());
        assertEquals(0, buffer.getReadIndex());

        assertEquals(1.66, buffer.readDouble(), 0.1);

        assertEquals(8, buffer.getWriteIndex());
        assertEquals(8, buffer.getReadIndex());

        assertEquals(0, buffer.getReadableBytes());
    }

    //----- Tests for read operations ----------------------------------------//

    @Test
    public void testReadByte() {
        byte[] source = new byte[] { 0, 1, 2, 3, 4, 5 };
        ProtonBuffer buffer = new ProtonByteBuffer(source);

        assertEquals(source.length, buffer.getReadableBytes());

        for (int i = 0; i < source.length; ++i) {
            assertEquals(source[i], buffer.readByte());
        }

        try {
            buffer.readByte();
            fail("Should not be able to read beyond current capacity");
        } catch (IndexOutOfBoundsException ex) {}
    }

    @Test
    public void testReadBoolean() {
        byte[] source = new byte[] { 0, 1, 0, 1, 0, 1 };
        ProtonBuffer buffer = new ProtonByteBuffer(source);

        assertEquals(source.length, buffer.getReadableBytes());

        for (int i = 0; i < source.length; ++i) {
            if ((i % 2) == 0) {
                assertFalse(buffer.readBoolean());
            } else {
                assertTrue(buffer.readBoolean());
            }
        }

        try {
            buffer.readBoolean();
            fail("Should not be able to read beyond current capacity");
        } catch (IndexOutOfBoundsException ex) {}
    }

    @Test
    public void testReadShort() {
        ProtonBuffer buffer = new ProtonByteBuffer();

        buffer.writeShort((short) 2);
        buffer.writeShort((short) 20);
        buffer.writeShort((short) 200);

        assertEquals(2, buffer.readShort());
        assertEquals(20, buffer.readShort());
        assertEquals(200, buffer.readShort());

        try {
            buffer.readShort();
            fail("Should not be able to read beyond current capacity");
        } catch (IndexOutOfBoundsException ex) {}
    }

    @Test
    public void testReadInt() {
        ProtonBuffer buffer = new ProtonByteBuffer();

        buffer.writeInt(2);
        buffer.writeInt(20);
        buffer.writeInt(200);

        assertEquals(2, buffer.readInt());
        assertEquals(20, buffer.readInt());
        assertEquals(200, buffer.readInt());

        try {
            buffer.readInt();
            fail("Should not be able to read beyond current capacity");
        } catch (IndexOutOfBoundsException ex) {}
    }

    @Test
    public void testReadLong() {
        ProtonBuffer buffer = new ProtonByteBuffer();

        buffer.writeLong(2l);
        buffer.writeLong(20l);
        buffer.writeLong(200l);

        assertEquals(2l, buffer.readLong());
        assertEquals(20l, buffer.readLong());
        assertEquals(200l, buffer.readLong());

        try {
            buffer.readLong();
            fail("Should not be able to read beyond current capacity");
        } catch (IndexOutOfBoundsException ex) {}
    }

    @Test
    public void testReadFloat() {
        ProtonBuffer buffer = new ProtonByteBuffer();

        buffer.writeFloat(1.111f);
        buffer.writeFloat(2.222f);
        buffer.writeFloat(3.333f);

        assertEquals(1.111f, buffer.readFloat(), 0.111f);
        assertEquals(2.222f, buffer.readFloat(), 0.222f);
        assertEquals(3.333f, buffer.readFloat(), 0.333f);

        try {
            buffer.readFloat();
            fail("Should not be able to read beyond current capacity");
        } catch (IndexOutOfBoundsException ex) {}
    }

    @Test
    public void testReadDouble() {
        ProtonBuffer buffer = new ProtonByteBuffer();

        buffer.writeDouble(1.111);
        buffer.writeDouble(2.222);
        buffer.writeDouble(3.333);

        assertEquals(1.111, buffer.readDouble(), 0.111);
        assertEquals(2.222, buffer.readDouble(), 0.222);
        assertEquals(3.333, buffer.readDouble(), 0.333);

        try {
            buffer.readDouble();
            fail("Should not be able to read beyond current capacity");
        } catch (IndexOutOfBoundsException ex) {}
    }

    //----- Tests for Copy operations ----------------------------------------//

    @Test
    public void testCopyEmptyBuffer() {
        ProtonBuffer buffer = new ProtonByteBuffer(10);
        ProtonBuffer copy = buffer.copy();

        assertEquals(buffer.getReadableBytes(), copy.getReadableBytes());

        assertTrue(copy.hasArray());
        assertNotNull(copy.getArray());

        assertNotSame(buffer.getArray(), copy.getArray());
    }

    @Test
    public void testCopyBuffer() {
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
    public void testDuplicateEmptyBuffer() {
        ProtonBuffer buffer = new ProtonByteBuffer(10);
        ProtonBuffer duplicate = buffer.duplicate();

        assertEquals(buffer.capacity(), duplicate.capacity());
        assertEquals(buffer.getReadableBytes(), duplicate.getReadableBytes());

        assertSame(buffer.getArray(), duplicate.getArray());
        assertEquals(0, buffer.getArrayOffset());
    }

    //----- Tests for conversion to ByteBuffer -------------------------------//

    @Test
    public void testToByteBufferWithDataPresent() {
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
    public void testToByteBufferWhenNoData() {
        ProtonBuffer buffer = new ProtonByteBuffer();
        ByteBuffer byteBuffer = buffer.toByteBuffer();

        assertEquals(buffer.getReadableBytes(), byteBuffer.limit());

        assertTrue(byteBuffer.hasArray());
        assertNotNull(byteBuffer.array());
        assertSame(buffer.getArray(), byteBuffer.array());
    }
}
