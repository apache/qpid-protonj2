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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.apache.qpid.proton4j.buffer.util.ProtonTestByteBuffer;
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
    public void testSetIndexWithNegativeReadIndex() {
        ProtonBuffer buffer = new ProtonByteBuffer();
        try {
            buffer.setIndex(-1, 0);
            fail("Should not accept negative values");
        } catch (IndexOutOfBoundsException e) {}
    }

    @Test
    public void testSetIndexWithNegativeWriteIndex() {
        ProtonBuffer buffer = new ProtonByteBuffer();
        try {
            buffer.setIndex(0, -1);
            fail("Should not accept negative values");
        } catch (IndexOutOfBoundsException e) {}
    }

    @Test
    public void testSetIndexWithReadIndexBiggerThanWrite() {
        ProtonBuffer buffer = new ProtonByteBuffer();
        try {
            buffer.setIndex(50, 40);
            fail("Should not accept bigger read index values");
        } catch (IndexOutOfBoundsException e) {}
    }

    @Test
    public void testSetIndexWithWriteIndexBiggerThanCapacity() {
        ProtonBuffer buffer = new ProtonByteBuffer();
        try {
            buffer.setIndex(0, buffer.capacity() + 1);
            fail("Should not accept write index bigger than capacity");
        } catch (IndexOutOfBoundsException e) {}
    }

    @Test
    public void testIsReadable() {
        ProtonBuffer buffer = new ProtonByteBuffer();
        assertFalse(buffer.isReadable());
        buffer.writeBoolean(false);
        assertTrue(buffer.isReadable());
    }

    @Test
    public void testIsReadableWithAmount() {
        ProtonBuffer buffer = new ProtonByteBuffer();
        assertFalse(buffer.isReadable(1));
        buffer.writeBoolean(false);
        assertTrue(buffer.isReadable(1));
        assertFalse(buffer.isReadable(2));
    }

    @Test
    public void testIsWriteable() {
        ProtonBuffer buffer = new ProtonByteBuffer();
        assertTrue(buffer.isWritable());
        buffer.setWriteIndex(buffer.capacity());
        assertFalse(buffer.isWritable());
    }

    @Test
    public void testIsWriteableWithAmount() {
        ProtonBuffer buffer = new ProtonByteBuffer();
        assertTrue(buffer.isWritable());
        buffer.setWriteIndex(buffer.capacity() - 1);
        assertTrue(buffer.isWritable(1));
        assertFalse(buffer.isWritable(2));
    }

    @Test
    public void testClear() {
        ProtonBuffer buffer = new ProtonByteBuffer();
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
        ProtonBuffer buffer = new ProtonByteBuffer();
        buffer.setWriteIndex(buffer.capacity() / 2);
        assertEquals(0, buffer.getReadIndex());
        buffer.skipBytes(buffer.capacity() / 2);
        assertEquals(buffer.capacity() / 2, buffer.getReadIndex());
    }

    @Test
    public void testSkipBytesBeyondReable() {
        ProtonBuffer buffer = new ProtonByteBuffer();
        buffer.setWriteIndex(buffer.capacity() / 2);
        assertEquals(0, buffer.getReadIndex());

        try {
            buffer.skipBytes(buffer.getReadableBytes() + 50);
            fail("Should not be able to skip beyond write index");
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

    //----- Write Bytes Tests ------------------------------------------------//

    @Test
    public void testWriteBytes() {
        byte[] payload = new byte[] { 0, 1, 2, 3, 4, 5 };

        ProtonBuffer buffer = new ProtonByteBuffer();

        assertEquals(0, buffer.getWriteIndex());
        assertEquals(0, buffer.getReadIndex());

        buffer.writeBytes(payload);

        assertEquals(payload.length, buffer.getWriteIndex());
        assertEquals(0, buffer.getReadIndex());

        for (int i = 0; i < payload.length; ++i) {
            assertEquals(payload[i], buffer.readByte());
        }

        assertEquals(payload.length, buffer.getReadIndex());
    }

    @Test
    public void testWriteBytesWithEmptyArray() {
        ProtonBuffer buffer = new ProtonByteBuffer();

        assertEquals(0, buffer.getWriteIndex());
        assertEquals(0, buffer.getReadIndex());

        buffer.writeBytes(new byte[0]);

        assertEquals(0, buffer.getWriteIndex());
        assertEquals(0, buffer.getReadIndex());
    }

    @Test
    public void testWriteBytesNPEWhenNullGiven() {
        ProtonBuffer buffer = new ProtonByteBuffer();
        try {
            buffer.writeBytes((byte[]) null);
            fail();
        } catch (NullPointerException ex) {}

        try {
            buffer.writeBytes((byte[]) null, 0);
            fail();
        } catch (NullPointerException ex) {}

        try {
            buffer.writeBytes((byte[]) null, 0, 0);
            fail();
        } catch (NullPointerException ex) {}
    }

    @Test
    public void testWriteBytesWithLength() {
        byte[] payload = new byte[] { 0, 1, 2, 3, 4, 5 };

        ProtonBuffer buffer = new ProtonByteBuffer();

        assertEquals(0, buffer.getWriteIndex());
        assertEquals(0, buffer.getReadIndex());

        buffer.writeBytes(payload, payload.length);

        assertEquals(payload.length, buffer.getWriteIndex());
        assertEquals(0, buffer.getReadIndex());

        for (int i = 0; i < payload.length; ++i) {
            assertEquals(payload[i], buffer.readByte());
        }

        assertEquals(payload.length, buffer.getReadIndex());
    }

    @Test
    public void testWriteBytesWithLengthToBig() {
        byte[] payload = new byte[] { 0, 1, 2, 3, 4, 5 };

        ProtonBuffer buffer = new ProtonByteBuffer();

        assertEquals(0, buffer.getWriteIndex());
        assertEquals(0, buffer.getReadIndex());

        try {
            buffer.writeBytes(payload, payload.length + 1);
            fail("Should not write when length given is to large.");
        } catch (IndexOutOfBoundsException ex) {}
    }

    @Test
    public void testWriteBytesWithNegativeLength() {
        byte[] payload = new byte[] { 0, 1, 2, 3, 4, 5 };

        ProtonBuffer buffer = new ProtonByteBuffer();

        assertEquals(0, buffer.getWriteIndex());
        assertEquals(0, buffer.getReadIndex());

        try {
            buffer.writeBytes(payload, -1);
            fail("Should not write when length given is negative.");
        } catch (IllegalArgumentException ex) {}
    }

    @Test
    public void testWriteBytesWithLengthAndOffset() {
        byte[] payload = new byte[] { 0, 1, 2, 3, 4, 5 };

        ProtonBuffer buffer = new ProtonByteBuffer();

        assertEquals(0, buffer.getWriteIndex());
        assertEquals(0, buffer.getReadIndex());

        buffer.writeBytes(payload, 0, payload.length);

        assertEquals(payload.length, buffer.getWriteIndex());
        assertEquals(0, buffer.getReadIndex());

        for (int i = 0; i < payload.length; ++i) {
            assertEquals(payload[i], buffer.readByte());
        }

        assertEquals(payload.length, buffer.getReadIndex());
    }

    @Test
    public void testWriteBytesWithLengthAndOffsetIncorrect() {
        byte[] payload = new byte[] { 0, 1, 2, 3, 4, 5 };

        ProtonBuffer buffer = new ProtonByteBuffer();

        assertEquals(0, buffer.getWriteIndex());
        assertEquals(0, buffer.getReadIndex());

        try {
            buffer.writeBytes(payload, 0, payload.length + 1);
            fail("Should not write when length given is to large.");
        } catch (IndexOutOfBoundsException ex) {}

        try {
            buffer.writeBytes(payload, -1, payload.length);
            fail("Should not write when offset given is negative.");
        } catch (IndexOutOfBoundsException ex) {}

        try {
            buffer.writeBytes(payload, 0, -1);
            fail("Should not write when length given is negative.");
        } catch (IllegalArgumentException ex) {}

        try {
            buffer.writeBytes(payload, payload.length + 1, 1);
            fail("Should not write when offset given is to large.");
        } catch (IndexOutOfBoundsException ex) {}
    }

    @Test
    public void testWriteBytesFromProtonBuffer() {
        ProtonBuffer source = new ProtonByteBuffer(new byte[] { 0, 1, 2, 3, 4, 5 });
        ProtonBuffer buffer = new ProtonByteBuffer();

        assertEquals(0, buffer.getWriteIndex());
        assertEquals(0, buffer.getReadIndex());

        buffer.writeBytes(source);

        assertEquals(0, source.getReadableBytes());
        assertEquals(source.getWriteIndex(), buffer.getWriteIndex());
        assertEquals(0, buffer.getReadIndex());

        for (int i = 0; i < source.getReadableBytes(); ++i) {
            assertEquals(source.getByte(i), buffer.readByte());
        }

        assertEquals(source.getReadableBytes(), buffer.getReadIndex());
    }

    @Test
    public void testWriteBytesFromProtonBufferWithLength() {
        ProtonBuffer source = new ProtonByteBuffer(new byte[] { 0, 1, 2, 3, 4, 5 });
        ProtonBuffer buffer = new ProtonByteBuffer();

        assertEquals(0, buffer.getWriteIndex());
        assertEquals(0, buffer.getReadIndex());

        try {
            buffer.writeBytes(source, source.getReadableBytes() + 1);
            fail("Should not write when length given is to large.");
        } catch (IndexOutOfBoundsException ex) {}

        buffer.writeBytes(source, source.getReadableBytes());

        assertEquals(0, source.getReadableBytes());
        assertEquals(source.getWriteIndex(), buffer.getWriteIndex());
        assertEquals(0, buffer.getReadIndex());

        for (int i = 0; i < source.getReadableBytes(); ++i) {
            assertEquals(source.getByte(i), buffer.readByte());
        }

        assertEquals(source.getReadableBytes(), buffer.getReadIndex());
    }

    @Test
    public void testWriteBytesFromProtonBufferWithLengthAndOffset() {
        ProtonBuffer source = new ProtonByteBuffer(new byte[] { 0, 1, 2, 3, 4, 5 });
        ProtonBuffer buffer = new ProtonByteBuffer();

        assertEquals(0, buffer.getWriteIndex());
        assertEquals(0, buffer.getReadIndex());

        buffer.writeBytes(source, 0, source.getReadableBytes());

        assertEquals(source.getReadableBytes(), buffer.getWriteIndex());
        assertEquals(0, buffer.getReadIndex());

        for (int i = 0; i < source.getReadableBytes(); ++i) {
            assertEquals(source.getByte(i), buffer.readByte());
        }

        assertEquals(source.getReadableBytes(), buffer.getReadIndex());
    }

    @Test
    public void testWriteBytesFromByteBuffer() {
        ByteBuffer source = ByteBuffer.wrap(new byte[] { 0, 1, 2, 3, 4, 5 });
        ProtonBuffer buffer = new ProtonByteBuffer();

        assertEquals(0, buffer.getWriteIndex());
        assertEquals(0, buffer.getReadIndex());

        buffer.writeBytes(source);

        assertEquals(0, source.remaining());
        assertEquals(source.position(), buffer.getWriteIndex());
        assertEquals(0, buffer.getReadIndex());

        for (int i = 0; i < source.capacity(); ++i) {
            assertEquals(source.get(i), buffer.readByte());
        }
    }

    //----- Write Primitives Tests -------------------------------------------//

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

    //----- Tests for get operations -----------------------------------------//

    @Test
    public void testGetByte() {
        byte[] source = new byte[] { 0, 1, 2, 3, 4, 5 };
        ProtonBuffer buffer = new ProtonByteBuffer(source);

        assertEquals(source.length, buffer.getReadableBytes());

        for (int i = 0; i < source.length; ++i) {
            assertEquals(source[i], buffer.getByte(i));
        }

        try {
            buffer.readByte();
        } catch (IndexOutOfBoundsException ex) {
            fail("Should be able to read from the buffer");
        }
    }

    @Test
    public void testGetUnsignedByte() {
        byte[] source = new byte[] { 0, 1, 2, 3, 4, 5 };
        ProtonBuffer buffer = new ProtonByteBuffer(source);

        assertEquals(source.length, buffer.getReadableBytes());

        for (int i = 0; i < source.length; ++i) {
            assertEquals(source[i], buffer.getUnsignedByte(i));
        }

        try {
            buffer.readByte();
        } catch (IndexOutOfBoundsException ex) {
            fail("Should be able to read from the buffer");
        }
    }

    @Test
    public void testGetBoolean() {
        byte[] source = new byte[] { 0, 1, 0, 1, 0, 1 };
        ProtonBuffer buffer = new ProtonByteBuffer(source);

        assertEquals(source.length, buffer.getReadableBytes());

        for (int i = 0; i < source.length; ++i) {
            if ((i % 2) == 0) {
                assertFalse(buffer.getBoolean(i));
            } else {
                assertTrue(buffer.getBoolean(i));
            }
        }

        try {
            buffer.readBoolean();
        } catch (IndexOutOfBoundsException ex) {
            fail("Should be able to read from the buffer");
        }
    }

    @Test
    public void testGetShort() {
        byte[] source = new byte[] { 0, 0, 0, 1, 0, 2, 0, 3, 0, 4 };
        ProtonBuffer buffer = new ProtonByteBuffer(source);

        assertEquals(source.length, buffer.getReadableBytes());

        for (int i = 0; i < source.length; i += 2) {
            assertEquals(source[i + 1], buffer.getShort(i));
        }

        try {
            buffer.readShort();
        } catch (IndexOutOfBoundsException ex) {
            fail("Should be able to read from the buffer");
        }
    }

    @Test
    public void testGetUnsignedShort() {
        byte[] source = new byte[] { 0, 0, 0, 1, 0, 2, 0, 3, 0, 4 };
        ProtonBuffer buffer = new ProtonByteBuffer(source);

        assertEquals(source.length, buffer.getReadableBytes());

        for (int i = 0; i < source.length; i += 2) {
            assertEquals(source[i + 1], buffer.getUnsignedShort(i));
        }

        try {
            buffer.readShort();
        } catch (IndexOutOfBoundsException ex) {
            fail("Should be able to read from the buffer");
        }
    }

    @Test
    public void testGetInt() {
        byte[] source = new byte[] { 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 4 };
        ProtonBuffer buffer = new ProtonByteBuffer(source);

        assertEquals(source.length, buffer.getReadableBytes());

        for (int i = 0; i < source.length; i += 4) {
            assertEquals(source[i + 3], buffer.getInt(i));
        }

        try {
            buffer.readInt();
        } catch (IndexOutOfBoundsException ex) {
            fail("Should be able to read from the buffer");
        }
    }

    @Test
    public void testGetUnsignedInt() {
        byte[] source = new byte[] { 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 4 };
        ProtonBuffer buffer = new ProtonByteBuffer(source);

        assertEquals(source.length, buffer.getReadableBytes());

        for (int i = 0; i < source.length; i += 4) {
            assertEquals(source[i + 3], buffer.getUnsignedInt(i));
        }

        try {
            buffer.readInt();
        } catch (IndexOutOfBoundsException ex) {
            fail("Should be able to read from the buffer");
        }
    }

    @Test
    public void testGetLong() {
        byte[] source = new byte[] { 0, 0, 0, 0, 0, 0, 0, 0,
                                     0, 0, 0, 0, 0, 0, 0, 1,
                                     0, 0, 0, 0, 0, 0, 0, 2,
                                     0, 0, 0, 0, 0, 0, 0, 3,
                                     0, 0, 0, 0, 0, 0, 0, 4 };
        ProtonBuffer buffer = new ProtonByteBuffer(source);

        assertEquals(source.length, buffer.getReadableBytes());

        for (int i = 0; i < source.length; i += 8) {
            assertEquals(source[i + 7], buffer.getLong(i));
        }

        try {
            buffer.readLong();
        } catch (IndexOutOfBoundsException ex) {
            fail("Should be able to read from the buffer");
        }
    }

    @Test
    public void testGetFloat() {
        ProtonBuffer buffer = new ProtonByteBuffer();
        buffer.writeInt(Float.floatToIntBits(1.1f));
        buffer.writeInt(Float.floatToIntBits(2.2f));
        buffer.writeInt(Float.floatToIntBits(42.3f));

        assertEquals(Integer.BYTES * 3, buffer.getReadableBytes());

        assertEquals(1.1f, buffer.getFloat(0), 0.1);
        assertEquals(2.2f, buffer.getFloat(4), 0.1);
        assertEquals(42.3f, buffer.getFloat(8), 0.1);

        try {
            buffer.readFloat();
        } catch (IndexOutOfBoundsException ex) {
            fail("Should be able to read from the buffer");
        }
    }

    @Test
    public void testGetDouble() {
        ProtonBuffer buffer = new ProtonByteBuffer();
        buffer.writeLong(Double.doubleToLongBits(1.1));
        buffer.writeLong(Double.doubleToLongBits(2.2));
        buffer.writeLong(Double.doubleToLongBits(42.3));

        assertEquals(Long.BYTES * 3, buffer.getReadableBytes());

        assertEquals(1.1, buffer.getDouble(0), 0.1);
        assertEquals(2.2, buffer.getDouble(8), 0.1);
        assertEquals(42.3, buffer.getDouble(16), 0.1);

        try {
            buffer.readDouble();
        } catch (IndexOutOfBoundsException ex) {
            fail("Should be able to read from the buffer");
        }
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

    //----- Tests for string conversion --------------------------------------//

    @Test
    public void testToStringFromUTF8() throws Exception {
        String sourceString = "Test-String-1";

        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.wrap(sourceString.getBytes(StandardCharsets.UTF_8));

        String decoded = buffer.toString(StandardCharsets.UTF_8);

        assertEquals(sourceString, decoded);
    }

    @Test
    public void testToStringFromUTF8WithNonArrayBackedBuffer() throws Exception {
        String sourceString = "Test-String-1";

        ProtonTestByteBuffer buffer = new ProtonTestByteBuffer(false);
        buffer.writeBytes(sourceString.getBytes(StandardCharsets.UTF_8));

        assertFalse(buffer.hasArray());

        String decoded = buffer.toString(StandardCharsets.UTF_8);

        assertEquals(sourceString, decoded);
    }

    //----- Tests for index marking ------------------------------------------//

    @Test
    public void testMarkAndResetReadIndex() {
        ProtonBuffer buffer = new ProtonByteBuffer();

        buffer.writeByte(0).writeByte(1);
        buffer.markReadIndex();

        assertEquals(0, buffer.readByte());
        assertEquals(1, buffer.readByte());

        buffer.resetReadIndex();

        assertEquals(0, buffer.readByte());
        assertEquals(1, buffer.readByte());
    }

    @Test
    public void testResetReadIndexWhenInvalid() {
        ProtonBuffer buffer = new ProtonByteBuffer();

        buffer.writeByte(0).writeByte(1);
        buffer.readByte();
        buffer.readByte();
        buffer.markReadIndex();
        buffer.setIndex(0, 1);

        try {
            buffer.resetReadIndex();
            fail("Should not be able to reset to invalid mark");
        } catch (IndexOutOfBoundsException iobe) {}
    }

    @Test
    public void testMarkAndResetWriteIndex() {
        ProtonBuffer buffer = new ProtonByteBuffer();

        buffer.markWriteIndex();
        buffer.writeByte(0).writeByte(1);
        buffer.resetWriteIndex();
        buffer.writeByte(2).writeByte(3);

        assertEquals(2, buffer.readByte());
        assertEquals(3, buffer.readByte());
    }

    @Test
    public void testResetWriteIndexWhenInvalid() {
        ProtonBuffer buffer = new ProtonByteBuffer();

        buffer.markWriteIndex();
        buffer.writeByte(0).writeByte(1);
        buffer.readByte();
        buffer.readByte();

        try {
            buffer.resetWriteIndex();
            fail("Should not be able to reset to invalid mark");
        } catch (IndexOutOfBoundsException iobe) {}
    }

    //----- Tests for equality and comparison --------------------------------//

    @Test
    public void testEqualsSelf() {
        byte[] payload = new byte[] { 0, 1, 2, 3, 4 };
        ProtonBuffer buffer = new ProtonByteBuffer(payload);
        assertTrue(buffer.equals(buffer));
    }

    @SuppressWarnings("unlikely-arg-type")
    @Test
    public void testEqualsFailsForOtherBufferType() {
        byte[] payload = new byte[] { 0, 1, 2, 3, 4 };
        ProtonBuffer buffer = new ProtonByteBuffer(payload);
        ByteBuffer byteBuffer = ByteBuffer.wrap(payload);

        assertFalse(buffer.equals(byteBuffer));
    }

    @Test
    public void testEqualsWithSameContents() {
        byte[] payload = new byte[] { 0, 1, 2, 3, 4 };
        ProtonBuffer buffer1 = new ProtonByteBuffer(payload);
        ProtonBuffer buffer2 = new ProtonByteBuffer(payload);

        assertTrue(buffer1.equals(buffer2));
        assertTrue(buffer2.equals(buffer1));
    }

    @Test
    public void testEqualsWithSameContentDifferenceArrays() {
        byte[] payload1 = new byte[] { 0, 1, 2, 3, 4 };
        byte[] payload2 = new byte[] { 0, 1, 2, 3, 4 };
        ProtonBuffer buffer1 = new ProtonByteBuffer(payload1);
        ProtonBuffer buffer2 = new ProtonByteBuffer(payload2);

        assertTrue(buffer1.equals(buffer2));
        assertTrue(buffer2.equals(buffer1));
    }

    @Test
    public void testEqualsWithDiffereingContent() {
        byte[] payload1 = new byte[] { 1, 2, 3, 4, 5 };
        byte[] payload2 = new byte[] { 0, 1, 2, 3, 4 };
        ProtonBuffer buffer1 = new ProtonByteBuffer(payload1);
        ProtonBuffer buffer2 = new ProtonByteBuffer(payload2);

        assertFalse(buffer1.equals(buffer2));
        assertFalse(buffer2.equals(buffer1));
    }

    @Test
    public void testEqualsWithDifferingReadableBytes() {
        byte[] payload1 = new byte[] { 0, 1, 2, 3, 4 };
        byte[] payload2 = new byte[] { 0, 1, 2, 3, 4 };
        ProtonBuffer buffer1 = new ProtonByteBuffer(payload1);
        ProtonBuffer buffer2 = new ProtonByteBuffer(payload2);

        buffer1.readByte();

        assertFalse(buffer1.equals(buffer2));
        assertFalse(buffer2.equals(buffer1));
    }

    @Test
    public void testHashCode() {
        byte[] payload = new byte[] { 0, 1, 2, 3, 4 };
        ProtonBuffer buffer = new ProtonByteBuffer(payload);
        assertNotEquals(0, buffer.hashCode());
        assertNotEquals(buffer.hashCode(), System.identityHashCode(buffer));
    }

    @Test
    public void testCompareToSameContents() {
        byte[] payload1 = new byte[] { 0, 1, 2, 3, 4 };
        byte[] payload2 = new byte[] { 0, 1, 2, 3, 4 };
        ProtonBuffer buffer1 = new ProtonByteBuffer(payload1);
        ProtonBuffer buffer2 = new ProtonByteBuffer(payload2);

        assertEquals(0, buffer1.compareTo(buffer1));
        assertEquals(0, buffer1.compareTo(buffer2));
        assertEquals(0, buffer2.compareTo(buffer1));
    }

    @Test
    public void testCompareToDifferentContents() {
        byte[] payload1 = new byte[] { 1, 2, 3, 4, 5 };
        byte[] payload2 = new byte[] { 0, 1, 2, 3, 4 };
        ProtonBuffer buffer1 = new ProtonByteBuffer(payload1);
        ProtonBuffer buffer2 = new ProtonByteBuffer(payload2);

        assertEquals(1, buffer1.compareTo(buffer2));
        assertEquals(-1, buffer2.compareTo(buffer1));
    }

    //----- Tests for readBytes variants -------------------------------------//

    @Test
    public void testReadBytesSingleArray() {
        byte[] payload = new byte[] { 0, 1, 2, 3, 4 };
        byte[] target = new byte[payload.length];

        ProtonBuffer buffer = new ProtonByteBuffer(payload);

        buffer.readBytes(target);

        assertEquals(0, buffer.getReadableBytes());

        for (int i = 0; i < payload.length; ++i) {
            assertEquals(payload[i], buffer.getByte(i));
        }

        try {
            buffer.readBytes(target);
            fail("should have thrown IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException iobe) {
        }
    }

    @Test
    public void testReadBytesArrayAndLength() {
        byte[] payload = new byte[] { 0, 1, 2, 3, 4 };
        byte[] target = new byte[payload.length];

        ProtonBuffer buffer = new ProtonByteBuffer(payload);

        try {
            buffer.readBytes(target, -1);
            fail("should have thrown IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
        }

        buffer.readBytes(target, target.length);

        assertEquals(0, buffer.getReadableBytes());

        for (int i = 0; i < payload.length; ++i) {
            assertEquals(payload[i], buffer.getByte(i));
        }

        try {
            buffer.readBytes(target);
            fail("should have thrown IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException iobe) {
        }
    }

    @Test
    public void testReadBytesArrayOffsetAndLength() {
        byte[] payload = new byte[] { 0, 1, 2, 3, 4 };
        byte[] target = new byte[payload.length];

        ProtonBuffer buffer = new ProtonByteBuffer(payload);

        try {
            buffer.readBytes(target, 0, -1);
            fail("should have thrown IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
        }

        buffer.readBytes(target, 0, target.length);

        assertEquals(0, buffer.getReadableBytes());

        for (int i = 0; i < payload.length; ++i) {
            assertEquals(payload[i], buffer.getByte(i));
        }

        try {
            buffer.readBytes(target);
            fail("should have thrown IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException iobe) {
        }
    }

    @Test
    public void testReadBytesSingleProtonBuffer() {
        byte[] payload = new byte[] { 0, 1, 2, 3, 4 };

        ProtonBuffer buffer = new ProtonByteBuffer(payload);
        ProtonBuffer target = new ProtonByteBuffer(5, 5);

        buffer.readBytes(target);

        assertEquals(0, buffer.getReadableBytes());

        for (int i = 0; i < payload.length; ++i) {
            assertEquals(payload[i], buffer.getByte(i));
        }

        target.setWriteIndex(0);

        try {
            buffer.readBytes(target);
            fail("should have thrown IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException iobe) {
        }
    }

    @Test
    public void testReadBytesProtonBufferAndLength() {
        byte[] payload = new byte[] { 0, 1, 2, 3, 4 };

        ProtonBuffer buffer = new ProtonByteBuffer(payload);
        ProtonBuffer target = new ProtonByteBuffer(5, 5);

        try {
            buffer.readBytes(target, -1);
            fail("should have thrown IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
        }

        try {
            buffer.readBytes(target, 1024);
            fail("should have thrown IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException iae) {
        }

        buffer.readBytes(target, payload.length);

        assertEquals(0, buffer.getReadableBytes());

        for (int i = 0; i < payload.length; ++i) {
            assertEquals(payload[i], buffer.getByte(i));
        }

        target.setWriteIndex(0);

        try {
            buffer.readBytes(target);
            fail("should have thrown IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException iobe) {
        }
    }

    @Test
    public void testReadBytesProtonBufferOffsetAndLength() {
        byte[] payload = new byte[] { 0, 1, 2, 3, 4 };

        ProtonBuffer buffer = new ProtonByteBuffer(payload);
        ProtonBuffer target = new ProtonByteBuffer(5, 5);

        try {
            buffer.readBytes(target, 0, -1);
            fail("should have thrown IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
        }

        try {
            buffer.readBytes(target, -1, -1);
            fail("should have thrown IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
        }

        buffer.readBytes(target, 0, payload.length);

        assertEquals(0, buffer.getReadableBytes());

        for (int i = 0; i < payload.length; ++i) {
            assertEquals(payload[i], buffer.getByte(i));
        }

        target.setWriteIndex(0);

        try {
            buffer.readBytes(target);
            fail("should have thrown IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException iobe) {
        }
    }

    @Test
    public void testReadBytesIntoByteBuffer() {
        byte[] payload = new byte[] { 0, 1, 2, 3, 4 };

        ProtonBuffer buffer = new ProtonByteBuffer(payload);
        ByteBuffer target = ByteBuffer.allocate(5);

        buffer.readBytes(target);

        assertEquals(0, buffer.getReadableBytes());

        for (int i = 0; i < payload.length; ++i) {
            assertEquals(payload[i], buffer.getByte(i));
        }

        target.clear();

        try {
            buffer.readBytes(target);
            fail("should have thrown IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException iobe) {
        }
    }

    //----- Test for set by index --------------------------------------------//

    @Test
    public void testSetByteAtIndex() {
        ProtonBuffer buffer = new ProtonByteBuffer(5, 5);

        for (int i = 0; i < buffer.capacity(); ++i) {
            buffer.setByte(i, i);
        }

        for (int i = 0; i < buffer.capacity(); ++i) {
            assertEquals(i, buffer.getByte(i));
        }

        try {
            buffer.setByte(-1, 0);
            fail("should throw an IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException ioe) {}

        try {
            buffer.setByte(buffer.capacity(), 0);
            fail("should throw an IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException ioe) {}
    }

    @Test
    public void testSetBooleanAtIndex() {
        ProtonBuffer buffer = new ProtonByteBuffer(5, 5);

        for (int i = 0; i < buffer.capacity(); ++i) {
            if ((i % 2) == 0) {
                buffer.setBoolean(i, false);
            } else {
                buffer.setBoolean(i, true);
            }
        }

        for (int i = 0; i < buffer.capacity(); ++i) {
            if ((i % 2) == 0) {
                assertFalse(buffer.getBoolean(i));
            } else {
                assertTrue(buffer.getBoolean(i));
            }
        }

        try {
            buffer.setBoolean(-1, true);
            fail("should throw an IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException ioe) {}

        try {
            buffer.setBoolean(buffer.capacity(), false);
            fail("should throw an IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException ioe) {}
    }

    @Test
    public void testSetShortAtIndex() {
        ProtonBuffer buffer = new ProtonByteBuffer(10, 10);

        for (short i = 0; i < buffer.capacity() / 2; i += 2) {
            buffer.setShort(i, i);
        }

        for (short i = 0; i < buffer.capacity() / 2; i += 2) {
            assertEquals(i, buffer.getShort(i));
        }

        try {
            buffer.setShort(-1, 0);
            fail("should throw an IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException ioe) {}

        try {
            buffer.setShort(buffer.capacity(), 0);
            fail("should throw an IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException ioe) {}
    }

    @Test
    public void testSetIntAtIndex() {
        ProtonBuffer buffer = new ProtonByteBuffer(20, 20);

        for (int i = 0; i < buffer.capacity() / 4; i += 4) {
            buffer.setInt(i, i);
        }

        for (int i = 0; i < buffer.capacity() / 4; i += 4) {
            assertEquals(i, buffer.getInt(i));
        }

        try {
            buffer.setInt(-1, 0);
            fail("should throw an IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException ioe) {}

        try {
            buffer.setInt(buffer.capacity(), 0);
            fail("should throw an IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException ioe) {}
    }

    @Test
    public void testSetLongAtIndex() {
        ProtonBuffer buffer = new ProtonByteBuffer(40, 40);

        for (long i = 0; i < buffer.capacity() / 8; i += 8) {
            buffer.setLong((int) i, i);
        }

        for (long i = 0; i < buffer.capacity() / 8; i += 8) {
            assertEquals(i, buffer.getLong((int) i));
        }

        try {
            buffer.setInt(-1, 0);
            fail("should throw an IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException ioe) {}

        try {
            buffer.setInt(buffer.capacity(), 0);
            fail("should throw an IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException ioe) {}
    }

    @Test
    public void testSetFloatAtIndex() {
        ProtonBuffer buffer = new ProtonByteBuffer(8, 8);

        buffer.setFloat(0, 1.5f);
        buffer.setFloat(4, 45.2f);

        assertEquals(1.5f, buffer.getFloat(0), 0.1);
        assertEquals(45.2f, buffer.getFloat(4), 0.1);

        try {
            buffer.setFloat(-1, 0);
            fail("should throw an IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException ioe) {}

        try {
            buffer.setFloat(buffer.capacity(), 0);
            fail("should throw an IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException ioe) {}
    }

    @Test
    public void testSetDoubleAtIndex() {
        ProtonBuffer buffer = new ProtonByteBuffer(16, 16);

        buffer.setDouble(0, 1.5);
        buffer.setDouble(8, 45.2);

        assertEquals(1.5, buffer.getDouble(0), 0.1);
        assertEquals(45.2, buffer.getDouble(8), 0.1);

        try {
            buffer.setDouble(-1, 0);
            fail("should throw an IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException ioe) {}

        try {
            buffer.setDouble(buffer.capacity(), 0);
            fail("should throw an IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException ioe) {}
    }

    @Test
    public void testSetCharAtIndex() {
        ProtonBuffer buffer = new ProtonByteBuffer(8, 8);

        buffer.setChar(0, 65);
        buffer.setChar(4, 66);

        assertEquals('A', buffer.getChar(0));
        assertEquals('B', buffer.getChar(4));

        try {
            buffer.setDouble(-1, 0);
            fail("should throw an IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException ioe) {}

        try {
            buffer.setDouble(buffer.capacity(), 0);
            fail("should throw an IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException ioe) {}
    }
}
