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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Random;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Abstract test base for testing common expected behaviors of ProtonBuffer implementations
 * of ProtonBuffer.
 */
public abstract class ProtonAbstractBufferTest {

    public static final int LARGE_CAPACITY = 4096; // Must be even for these tests
    public static final int BLOCK_SIZE = 128;
    public static final int DEFAULT_CAPACITY = 64;

    protected long seed;
    protected Random random;

    @BeforeEach
    public void setUp() {
        seed = System.currentTimeMillis();
        random = new Random();
        random.setSeed(seed);
    }

    //----- Test Buffer creation ---------------------------------------------//

    @Test
    public void testConstructWithDifferingCapacityAndMaxCapacity() {
        assumeTrue(canBufferCapacityBeChanged());

        final int baseCapaity = DEFAULT_CAPACITY + 10;

        ProtonBuffer buffer = allocateBuffer(baseCapaity, baseCapaity + 100);

        assertEquals(0, buffer.getReadableBytes());
        assertEquals(baseCapaity, buffer.capacity());
        assertEquals(baseCapaity + 100, buffer.maxCapacity());
    }

    @Test
    public void testBufferRespectsMaxCapacityAfterGrowingToFit() {
        assumeTrue(canBufferCapacityBeChanged());

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

    @Test
    public void testBufferRespectsMaxCapacityLimitNoGrowthScenario() {
        ProtonBuffer buffer = allocateBuffer(10, 10);

        assertEquals(0, buffer.getReadableBytes());
        assertEquals(10, buffer.capacity());
        assertEquals(10, buffer.maxCapacity());

        // Writes to capacity work, but exceeding that should fail.
        for (int i = 0; i < 10; ++i) {
            buffer.writeByte(i);
        }

        try {
            buffer.writeByte(10);
            fail("Should not be able to write more than the max capacity bytes");
        } catch (IndexOutOfBoundsException iobe) {}
    }

    //----- Tests for altering buffer capacity -------------------------------//

    @Test
    public void testCapacityEnforceMaxCapacity() {
        assumeTrue(canBufferCapacityBeChanged());

        ProtonBuffer buffer = allocateBuffer(3, 13);
        assertEquals(13, buffer.maxCapacity());
        assertEquals(3, buffer.capacity());

        assertThrows(IllegalArgumentException.class, () -> buffer.capacity(14));
    }

    @Test
    public void testCapacityNegative() {
        assumeTrue(canBufferCapacityBeChanged());

        ProtonBuffer buffer = allocateBuffer(3, 13);
        assertEquals(13, buffer.maxCapacity());
        assertEquals(3, buffer.capacity());

        assertThrows(IllegalArgumentException.class, () -> buffer.capacity(-1));
    }

    @Test
    public void testCapacityDecrease() {
        assumeTrue(canBufferCapacityBeChanged());

        ProtonBuffer buffer = allocateBuffer(3, 13);
        assertEquals(13, buffer.maxCapacity());
        assertEquals(3, buffer.capacity());
        buffer.capacity(2);
        assertEquals(2, buffer.capacity());
        assertEquals(13, buffer.maxCapacity());
    }

    @Test
    public void testCapacityIncrease() {
        assumeTrue(canBufferCapacityBeChanged());

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
    public void testMaxWritableBytes() {
        ProtonBuffer buffer = allocateBuffer(DEFAULT_CAPACITY, DEFAULT_CAPACITY);
        assertTrue(buffer.isWritable());
        assertEquals(DEFAULT_CAPACITY, buffer.getMaxWritableBytes());
        buffer.setWriteIndex(buffer.capacity() - 1);
        assertTrue(buffer.isWritable(1));
        assertFalse(buffer.isWritable(2));
        assertEquals(1, buffer.getMaxWritableBytes());
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

    //----- Tests for altering buffer capacity -------------------------------//

    @Test
    public void testIncreaseCapacity() {
        assumeTrue(canBufferCapacityBeChanged());

        byte[] source = new byte[100];

        ProtonBuffer buffer = allocateBuffer(100).writeBytes(source);
        assertEquals(100, buffer.capacity());
        assertEquals(0, buffer.getReadIndex());
        assertEquals(100, buffer.getWriteIndex());

        buffer.capacity(200);
        assertEquals(200, buffer.capacity());

        buffer.capacity(200);
        assertEquals(200, buffer.capacity());

        assertEquals(0, buffer.getReadIndex());
        assertEquals(100, buffer.getWriteIndex());
    }

    @Test
    public void testDecreaseCapacity() {
        assumeTrue(canBufferCapacityBeChanged());

        byte[] source = new byte[100];

        ProtonBuffer buffer = allocateBuffer(100).writeBytes(source);
        assertEquals(100, buffer.capacity());
        assertEquals(100, buffer.getWriteIndex());

        buffer.capacity(50);
        assertEquals(50, buffer.capacity());

        // Buffer is truncated but we never read anything so read index stays at front.
        assertEquals(0, buffer.getReadIndex());
        assertEquals(50, buffer.getWriteIndex());
    }

    @Test
    public void testDecreaseCapacityValidatesSize() {
        assumeTrue(canBufferCapacityBeChanged());

        ProtonBuffer buffer = allocateBuffer(100);
        assertEquals(100, buffer.capacity());

        try {
            buffer.capacity(-50);
            fail("Should throw IllegalArgumentException");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testDecreaseCapacityWithReadIndexIndexBeyondNewValue() {
        assumeTrue(canBufferCapacityBeChanged());

        byte[] source = new byte[100];

        ProtonBuffer buffer = allocateBuffer(100).writeBytes(source);
        assertEquals(100, buffer.capacity());

        buffer.setReadIndex(60);

        buffer.capacity(50);
        assertEquals(50, buffer.capacity());

        // Buffer should be truncated and read index moves back to end
        assertEquals(50, buffer.getReadIndex());
        assertEquals(50, buffer.getWriteIndex());
    }

    @Test
    public void testDecreaseCapacityWithWriteIndexWithinNewValue() {
        assumeTrue(canBufferCapacityBeChanged());

        byte[] source = new byte[100];

        ProtonBuffer buffer = allocateBuffer(100).writeBytes(source);
        assertEquals(100, buffer.capacity());

        buffer.setIndex(10, 30);
        buffer.capacity(50);
        assertEquals(50, buffer.capacity());

        // Buffer should be truncated but index values remain unchanged
        assertEquals(10, buffer.getReadIndex());
        assertEquals(30, buffer.getWriteIndex());
    }

    @Test
    public void testCapacityIncreasesWhenWritesExceedCurrent() {
        assumeTrue(canBufferCapacityBeChanged());

        ProtonBuffer buffer = allocateBuffer(10);

        assertTrue(buffer.hasArray());

        assertEquals(10, buffer.capacity());
        assertEquals(Integer.MAX_VALUE, buffer.maxCapacity());

        for (int i = 1; i <= 9; ++i) {
            buffer.writeByte(i);
        }

        assertEquals(10, buffer.capacity());

        buffer.writeByte(10);

        assertEquals(10, buffer.capacity());

        buffer.writeByte(11);

        assertTrue(buffer.capacity() > 10);

        assertEquals(11, buffer.getReadableBytes());

        for (int i = 1; i < 12; ++i) {
            assertEquals(i, buffer.readByte());
        }
    }

    //----- Write Bytes Tests ------------------------------------------------//

    @Test
    public void testWriteBytes() {
        byte[] payload = new byte[] { 0, 1, 2, 3, 4, 5 };

        ProtonBuffer buffer = allocateDefaultBuffer();

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
        ProtonBuffer buffer = allocateDefaultBuffer();

        assertEquals(0, buffer.getWriteIndex());
        assertEquals(0, buffer.getReadIndex());

        buffer.writeBytes(new byte[0]);

        assertEquals(0, buffer.getWriteIndex());
        assertEquals(0, buffer.getReadIndex());
    }

    @Test
    public void testWriteBytesNPEWhenNullGiven() {
        ProtonBuffer buffer = allocateDefaultBuffer();

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

        ProtonBuffer buffer = allocateDefaultBuffer();

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

        ProtonBuffer buffer = allocateDefaultBuffer();

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

        ProtonBuffer buffer = allocateDefaultBuffer();

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

        ProtonBuffer buffer = allocateDefaultBuffer();

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

        ProtonBuffer buffer = allocateDefaultBuffer();

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
        ProtonBuffer buffer = allocateDefaultBuffer();

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
        ProtonBuffer buffer = allocateDefaultBuffer();

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
        ProtonBuffer buffer = allocateDefaultBuffer();

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
        ProtonBuffer buffer = allocateDefaultBuffer();

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
        ProtonBuffer buffer = allocateDefaultBuffer();

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
        ProtonBuffer buffer = allocateDefaultBuffer();

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
        ProtonBuffer buffer = allocateDefaultBuffer();

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
        ProtonBuffer buffer = allocateDefaultBuffer();

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
        ProtonBuffer buffer = allocateDefaultBuffer();

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
        ProtonBuffer buffer = allocateDefaultBuffer();

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
        ProtonBuffer buffer = allocateDefaultBuffer();

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
        ProtonBuffer buffer = wrapBuffer(source);

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
        ProtonBuffer buffer = wrapBuffer(source);

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
        ProtonBuffer buffer = allocateDefaultBuffer();

        buffer.writeShort((short) 2);
        buffer.writeShort((short) 20);
        buffer.writeShort((short) 200);
        buffer.writeShort((short) 256);
        buffer.writeShort((short) 512);
        buffer.writeShort((short) 1025);
        buffer.writeShort((short) 32767);
        buffer.writeShort((short) -1);
        buffer.writeShort((short) -8757);

        assertEquals(2, buffer.readShort());
        assertEquals(20, buffer.readShort());
        assertEquals(200, buffer.readShort());
        assertEquals(256, buffer.readShort());
        assertEquals(512, buffer.readShort());
        assertEquals(1025, buffer.readShort());
        assertEquals(32767, buffer.readShort());
        assertEquals(-1, buffer.readShort());
        assertEquals(-8757, buffer.readShort());

        try {
            buffer.readShort();
            fail("Should not be able to read beyond current capacity");
        } catch (IndexOutOfBoundsException ex) {}
    }

    @Test
    public void testReadInt() {
        ProtonBuffer buffer = allocateDefaultBuffer();

        buffer.writeInt((short) 2);
        buffer.writeInt((short) 20);
        buffer.writeInt((short) 200);
        buffer.writeInt((short) 256);
        buffer.writeInt((short) 512);
        buffer.writeInt((short) 1025);
        buffer.writeInt((short) 32767);
        buffer.writeInt((short) -1);
        buffer.writeInt((short) -8757);

        assertEquals(2, buffer.readInt());
        assertEquals(20, buffer.readInt());
        assertEquals(200, buffer.readInt());
        assertEquals(256, buffer.readInt());
        assertEquals(512, buffer.readInt());
        assertEquals(1025, buffer.readInt());
        assertEquals(32767, buffer.readInt());
        assertEquals(-1, buffer.readInt());
        assertEquals(-8757, buffer.readInt());

        try {
            buffer.readInt();
            fail("Should not be able to read beyond current capacity");
        } catch (IndexOutOfBoundsException ex) {}
    }

    @Test
    public void testReadLong() {
        // This is not a capacity increase handling test so allocate with enough capacity for this test.
        ProtonBuffer buffer = allocateBuffer(DEFAULT_CAPACITY * 2);

        buffer.writeLong((short) 2);
        buffer.writeLong((short) 20);
        buffer.writeLong((short) 200);
        buffer.writeLong((short) 256);
        buffer.writeLong((short) 512);
        buffer.writeLong((short) 1025);
        buffer.writeLong((short) 32767);
        buffer.writeLong((short) -1);
        buffer.writeLong((short) -8757);

        assertEquals(2, buffer.readLong());
        assertEquals(20, buffer.readLong());
        assertEquals(200, buffer.readLong());
        assertEquals(256, buffer.readLong());
        assertEquals(512, buffer.readLong());
        assertEquals(1025, buffer.readLong());
        assertEquals(32767, buffer.readLong());
        assertEquals(-1, buffer.readLong());
        assertEquals(-8757, buffer.readLong());

        try {
            buffer.readLong();
            fail("Should not be able to read beyond current readable bytes");
        } catch (IndexOutOfBoundsException ex) {}
    }

    @Test
    public void testReadFloat() {
        ProtonBuffer buffer = allocateDefaultBuffer();

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
        ProtonBuffer buffer = allocateDefaultBuffer();

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
        ProtonBuffer buffer = wrapBuffer(source);

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
        ProtonBuffer buffer = wrapBuffer(source);

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
        ProtonBuffer buffer = wrapBuffer(source);

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
        ProtonBuffer buffer = wrapBuffer(source);

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
        ProtonBuffer buffer = wrapBuffer(source);

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
        ProtonBuffer buffer = wrapBuffer(source);

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
        ProtonBuffer buffer = wrapBuffer(source);

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
        ProtonBuffer buffer = wrapBuffer(source);

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
        ProtonBuffer buffer = allocateDefaultBuffer();
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
        ProtonBuffer buffer = allocateDefaultBuffer();
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
        ProtonBuffer buffer = allocateBuffer(10);
        ProtonBuffer copy = buffer.copy();

        assertEquals(buffer.getReadableBytes(), copy.getReadableBytes());

        if (buffer.hasArray()) {
            assertTrue(copy.hasArray());
            assertNotNull(copy.getArray());
            assertNotSame(buffer.getArray(), copy.getArray());
        }
    }

    @Test
    public void testCopyBuffer() {
        ProtonBuffer buffer = allocateBuffer(10);

        buffer.writeByte(1);
        buffer.writeByte(2);
        buffer.writeByte(3);
        buffer.writeByte(4);
        buffer.writeByte(5);

        ProtonBuffer copy = buffer.copy();

        assertEquals(buffer.getReadableBytes(), copy.getReadableBytes());

        for (int i = 0; i < 5; ++i) {
            assertEquals(buffer.getByte(i), copy.getByte(i));
        }
    }

    @Test
    public void testCopy() {
        ProtonBuffer buffer = allocateBuffer(LARGE_CAPACITY);

        for (int i = 0; i < buffer.capacity(); i ++) {
            byte value = (byte) random.nextInt();
            buffer.setByte(i, value);
        }

        final int readerIndex = LARGE_CAPACITY / 3;
        final int writerIndex = LARGE_CAPACITY * 2 / 3;
        buffer.setIndex(readerIndex, writerIndex);

        // Make sure all properties are copied.
        ProtonBuffer copy = buffer.copy();
        assertEquals(0, copy.getReadIndex());
        assertEquals(buffer.getReadableBytes(), copy.getWriteIndex());
        assertEquals(buffer.getReadableBytes(), copy.capacity());
        for (int i = 0; i < copy.capacity(); i ++) {
            assertEquals(buffer.getByte(i + readerIndex), copy.getByte(i));
        }

        // Make sure the buffer content is independent from each other.
        buffer.setByte(readerIndex, (byte) (buffer.getByte(readerIndex) + 1));
        assertTrue(buffer.getByte(readerIndex) != copy.getByte(0));
        copy.setByte(1, (byte) (copy.getByte(1) + 1));
        assertTrue(buffer.getByte(readerIndex + 1) != copy.getByte(1));
    }

    @Test
    public void testSequentialRandomFilledBufferIndexedCopy() {
        doTestSequentialRandomFilledBufferIndexedCopy(false);
    }

    @Test
    public void testSequentialRandomFilledBufferIndexedCopyDirectBackedBuffer() {
        assumeTrue(canAllocateDirectBackedBuffers());
        doTestSequentialRandomFilledBufferIndexedCopy(true);
    }

    private void doTestSequentialRandomFilledBufferIndexedCopy(boolean direct) {
        final ProtonBuffer buffer;
        if (direct) {
            buffer = allocateDirectBuffer(LARGE_CAPACITY);
        } else {
            buffer = allocateBuffer(LARGE_CAPACITY);
        }

        byte[] value = new byte[BLOCK_SIZE];
        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            random.nextBytes(value);
            buffer.setBytes(i, value);
        }

        random.setSeed(seed);
        byte[] expectedValueContent = new byte[BLOCK_SIZE];
        ProtonBuffer expectedValue = new ProtonByteBuffer(expectedValueContent);
        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            random.nextBytes(expectedValueContent);
            ProtonBuffer copy = buffer.copy(i, BLOCK_SIZE);
            for (int j = 0; j < BLOCK_SIZE; j ++) {
                assertEquals(expectedValue.getByte(j), copy.getByte(j));
            }
        }
    }

    //----- Tests for Buffer duplication -------------------------------------//

    @Test
    public void testDuplicateEmptyBuffer() {
        ProtonBuffer buffer = new ProtonByteBuffer(10);
        ProtonBuffer duplicate = buffer.duplicate();

        assertNotSame(buffer, duplicate);
        assertEquals(buffer.capacity(), duplicate.capacity());
        assertEquals(buffer.getReadableBytes(), duplicate.getReadableBytes());

        if (buffer.hasArray()) {
            assertSame(buffer.getArray(), duplicate.getArray());
            assertEquals(0, buffer.getArrayOffset());
        }
    }

    @Test
    public void testDuplicate() {
        ProtonBuffer buffer = allocateBuffer(LARGE_CAPACITY);

        for (int i = 0; i < buffer.capacity(); i ++) {
            byte value = (byte) random.nextInt();
            buffer.setByte(i, value);
        }

        final int readerIndex = LARGE_CAPACITY / 3;
        final int writerIndex = LARGE_CAPACITY * 2 / 3;
        buffer.setIndex(readerIndex, writerIndex);

        // Make sure all properties are copied.
        ProtonBuffer duplicate = buffer.duplicate();
        assertEquals(buffer.getReadableBytes(), duplicate.getReadableBytes());
        assertEquals(0, buffer.compareTo(duplicate));

        // Make sure the buffer content is shared.
        buffer.setByte(readerIndex, (byte) (buffer.getByte(readerIndex) + 1));
        assertEquals(buffer.getByte(readerIndex), duplicate.getByte(duplicate.getReadIndex()));
        duplicate.setByte(duplicate.getReadIndex(), (byte) (duplicate.getByte(duplicate.getReadIndex()) + 1));
        assertEquals(buffer.getByte(readerIndex), duplicate.getByte(duplicate.getReadIndex()));
    }

    //----- Tests for Buffer slicing -----------------------------------------//

    @Test
    public void testSliceIndex() throws Exception {
        ProtonBuffer buffer = allocateBuffer(LARGE_CAPACITY);

        assertEquals(0, buffer.slice(0, buffer.capacity()).getReadIndex());
        assertEquals(0, buffer.slice(0, buffer.capacity() - 1).getReadIndex());
        assertEquals(0, buffer.slice(1, buffer.capacity() - 1).getReadIndex());
        assertEquals(0, buffer.slice(1, buffer.capacity() - 2).getReadIndex());

        assertEquals(buffer.capacity(), buffer.slice(0, buffer.capacity()).getWriteIndex());
        assertEquals(buffer.capacity() - 1, buffer.slice(0, buffer.capacity() - 1).getWriteIndex());
        assertEquals(buffer.capacity() - 1, buffer.slice(1, buffer.capacity() - 1).getWriteIndex());
        assertEquals(buffer.capacity() - 2, buffer.slice(1, buffer.capacity() - 2).getWriteIndex());
    }

    @Test
    public void testAdvancingSlice() {
        ProtonBuffer buffer = allocateBuffer(LARGE_CAPACITY);

        buffer.setWriteIndex(0);
        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            byte[] value = new byte[BLOCK_SIZE];
            random.nextBytes(value);
            assertEquals(0, buffer.getReadIndex());
            assertEquals(i, buffer.getWriteIndex());
            buffer.writeBytes(value);
        }

        random.setSeed(seed);
        byte[] expectedValue = new byte[BLOCK_SIZE];
        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            random.nextBytes(expectedValue);
            assertEquals(i, buffer.getReadIndex());
            assertEquals(LARGE_CAPACITY, buffer.getWriteIndex());
            ProtonBuffer actualValue = buffer.slice().setWriteIndex(BLOCK_SIZE);
            buffer.setReadIndex(BLOCK_SIZE + buffer.getReadIndex());
            assertEquals(wrapBuffer(expectedValue), actualValue);

            // Make sure if it is a sliced buffer.
            actualValue.setByte(0, (byte) (actualValue.getByte(0) + 1));
            assertEquals(buffer.getByte(i), actualValue.getByte(0));
        }
    }

    @Test
    public void testSequentialSlice1() {
        ProtonBuffer buffer = allocateBuffer(LARGE_CAPACITY);

        buffer.setWriteIndex(0);
        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            byte[] value = new byte[BLOCK_SIZE];
            random.nextBytes(value);
            assertEquals(0, buffer.getReadIndex());
            assertEquals(i, buffer.getWriteIndex());
            buffer.writeBytes(value);
        }

        random.setSeed(seed);
        byte[] expectedValue = new byte[BLOCK_SIZE];
        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            random.nextBytes(expectedValue);
            assertEquals(i, buffer.getReadIndex());
            assertEquals(LARGE_CAPACITY, buffer.getWriteIndex());
            ProtonBuffer actualValue = buffer.slice(buffer.getReadIndex(), BLOCK_SIZE);
            buffer.setReadIndex(BLOCK_SIZE + buffer.getReadIndex());
            assertEquals(new ProtonByteBuffer(expectedValue), actualValue);

            // Make sure if it is a sliced buffer.
            actualValue.setByte(0, (byte) (actualValue.getByte(0) + 1));
            assertEquals(buffer.getByte(i), actualValue.getByte(0));
        }
    }

    //----- Tests for conversion to ByteBuffer -------------------------------//

    @Test
    public void testToByteBufferWithDataPresent() {
        ProtonBuffer buffer = allocateBuffer(10);

        buffer.writeByte(1);
        buffer.writeByte(2);
        buffer.writeByte(3);
        buffer.writeByte(4);
        buffer.writeByte(5);

        ByteBuffer byteBuffer = buffer.toByteBuffer();

        assertEquals(buffer.getReadableBytes(), byteBuffer.limit());

        if (buffer.hasArray()) {
            assertTrue(byteBuffer.hasArray());
            assertNotNull(byteBuffer.array());
            assertSame(buffer.getArray(), byteBuffer.array());
        }
    }

    @Test
    public void testToByteBufferWhenNoData() {
        ProtonBuffer buffer = allocateDefaultBuffer();
        ByteBuffer byteBuffer = buffer.toByteBuffer();

        assertEquals(buffer.getReadableBytes(), byteBuffer.limit());

        if (buffer.hasArray()) {
            assertTrue(byteBuffer.hasArray());
            assertNotNull(byteBuffer.array());
            assertSame(buffer.getArray(), byteBuffer.array());
        }
    }

    @Test
    public void testNioBufferNoArgVariant() {
        ProtonBuffer buffer = allocateBuffer(LARGE_CAPACITY);

        byte[] value = new byte[buffer.capacity()];
        random.nextBytes(value);
        buffer.clear();
        buffer.writeBytes(value);

        assertRemainingEquals(ByteBuffer.wrap(value), buffer.toByteBuffer());
    }

    @Test
    public void testToByteBufferWithRange() {
        ProtonBuffer buffer = allocateBuffer(LARGE_CAPACITY);

        byte[] value = new byte[buffer.capacity()];
        random.nextBytes(value);
        buffer.clear();
        buffer.writeBytes(value);

        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            assertRemainingEquals(ByteBuffer.wrap(value, i, BLOCK_SIZE), buffer.toByteBuffer(i, BLOCK_SIZE));
        }
    }

    //----- Tests for string conversion --------------------------------------//

    @Test
    public void testToStringFromUTF8() throws Exception {
        String sourceString = "Test-String-1";

        ProtonBuffer buffer = wrapBuffer(sourceString.getBytes(StandardCharsets.UTF_8));

        String decoded = buffer.toString(StandardCharsets.UTF_8);

        assertEquals(sourceString, decoded);
    }

    //----- Tests for index marking ------------------------------------------//

    @Test
    public void testMarkAndResetReadIndex() {
        ProtonBuffer buffer = allocateDefaultBuffer();

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
        ProtonBuffer buffer = allocateDefaultBuffer();

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
        ProtonBuffer buffer = allocateDefaultBuffer();

        buffer.markWriteIndex();
        buffer.writeByte(0).writeByte(1);
        buffer.resetWriteIndex();
        buffer.writeByte(2).writeByte(3);

        assertEquals(2, buffer.readByte());
        assertEquals(3, buffer.readByte());
    }

    @Test
    public void testResetWriteIndexWhenInvalid() {
        ProtonBuffer buffer = allocateDefaultBuffer();

        buffer.markWriteIndex();
        buffer.writeByte(0).writeByte(1);
        buffer.readByte();
        buffer.readByte();

        try {
            buffer.resetWriteIndex();
            fail("Should not be able to reset to invalid mark");
        } catch (IndexOutOfBoundsException iobe) {}
    }

    @Test
    public void testReaderIndexLargerThanWriterIndex() {
        String content1 = "hello";
        String content2 = "world";
        int length = content1.length() + content2.length();

        ProtonBuffer buffer = allocateBuffer(length);
        buffer.setIndex(0, 0);
        buffer.writeBytes(content1.getBytes(StandardCharsets.UTF_8));
        buffer.markWriteIndex();
        buffer.skipBytes(content1.length());
        buffer.writeBytes(content2.getBytes(StandardCharsets.UTF_8));
        buffer.skipBytes(content2.length());
        assertTrue(buffer.getReadIndex() <= buffer.getWriteIndex());

        assertThrows(IndexOutOfBoundsException.class, () -> buffer.resetWriteIndex());
    }

    //----- Tests for equality and comparison --------------------------------//

    @Test
    public void testEqualsSelf() {
        byte[] payload = new byte[] { 0, 1, 2, 3, 4 };
        ProtonBuffer buffer = wrapBuffer(payload);
        assertTrue(buffer.equals(buffer));
    }

    @SuppressWarnings("unlikely-arg-type")
    @Test
    public void testEqualsFailsForOtherBufferType() {
        byte[] payload = new byte[] { 0, 1, 2, 3, 4 };
        ProtonBuffer buffer = wrapBuffer(payload);
        ByteBuffer byteBuffer = ByteBuffer.wrap(payload);

        assertFalse(buffer.equals(byteBuffer));
    }

    @Test
    public void testEqualsWithSameContents() {
        byte[] payload = new byte[] { 0, 1, 2, 3, 4 };
        ProtonBuffer buffer1 = wrapBuffer(payload);
        ProtonBuffer buffer2 = wrapBuffer(payload);

        assertTrue(buffer1.equals(buffer2));
        assertTrue(buffer2.equals(buffer1));
    }

    @Test
    public void testEqualsWithSameContentDifferenceArrays() {
        byte[] payload1 = new byte[] { 0, 1, 2, 3, 4 };
        byte[] payload2 = new byte[] { 0, 1, 2, 3, 4 };
        ProtonBuffer buffer1 = wrapBuffer(payload1);
        ProtonBuffer buffer2 = wrapBuffer(payload2);

        assertTrue(buffer1.equals(buffer2));
        assertTrue(buffer2.equals(buffer1));
    }

    @Test
    public void testEqualsWithDiffereingContent() {
        byte[] payload1 = new byte[] { 1, 2, 3, 4, 5 };
        byte[] payload2 = new byte[] { 0, 1, 2, 3, 4 };
        ProtonBuffer buffer1 = wrapBuffer(payload1);
        ProtonBuffer buffer2 = wrapBuffer(payload2);

        assertFalse(buffer1.equals(buffer2));
        assertFalse(buffer2.equals(buffer1));
    }

    @Test
    public void testEqualsWithDifferingReadableBytes() {
        byte[] payload1 = new byte[] { 0, 1, 2, 3, 4 };
        byte[] payload2 = new byte[] { 0, 1, 2, 3, 4 };
        ProtonBuffer buffer1 = wrapBuffer(payload1);
        ProtonBuffer buffer2 = wrapBuffer(payload2);

        buffer1.readByte();

        assertFalse(buffer1.equals(buffer2));
        assertFalse(buffer2.equals(buffer1));
    }

    @Test
    public void testHashCode() {
        byte[] payload = new byte[] { 0, 1, 2, 3, 4 };
        ProtonBuffer buffer = wrapBuffer(payload);
        assertNotEquals(0, buffer.hashCode());
        assertNotEquals(buffer.hashCode(), System.identityHashCode(buffer));
    }

    @Test
    public void testCompareToSameContents() {
        byte[] payload1 = new byte[] { 0, 1, 2, 3, 4 };
        byte[] payload2 = new byte[] { 0, 1, 2, 3, 4 };
        ProtonBuffer buffer1 = wrapBuffer(payload1);
        ProtonBuffer buffer2 = wrapBuffer(payload2);

        assertEquals(0, buffer1.compareTo(buffer1));
        assertEquals(0, buffer1.compareTo(buffer2));
        assertEquals(0, buffer2.compareTo(buffer1));
    }

    @Test
    public void testCompareToDifferentContents() {
        byte[] payload1 = new byte[] { 1, 2, 3, 4, 5 };
        byte[] payload2 = new byte[] { 0, 1, 2, 3, 4 };
        ProtonBuffer buffer1 = wrapBuffer(payload1);
        ProtonBuffer buffer2 = wrapBuffer(payload2);

        assertEquals(1, buffer1.compareTo(buffer2));
        assertEquals(-1, buffer2.compareTo(buffer1));
    }

    @Test
    public void testComparableInterfaceNotViolatedWithLongWrites() {
        ProtonBuffer buffer1 = allocateBuffer(LARGE_CAPACITY);
        ProtonBuffer buffer2 = allocateBuffer(LARGE_CAPACITY);

        buffer1.setWriteIndex(buffer1.getReadIndex());
        buffer1.writeLong(0);

        buffer2.setWriteIndex(buffer2.getReadIndex());
        buffer2.writeLong(0xF0000000L);

        assertTrue(buffer1.compareTo(buffer2) < 0);
        assertTrue(buffer2.compareTo(buffer1) > 0);
    }

    @Test
    public void testCompareToContract() {
        ProtonBuffer buffer = allocateBuffer(LARGE_CAPACITY);

        try {
            buffer.compareTo(null);
            fail();
        } catch (NullPointerException e) {
            // Expected
        }

        // Fill the random stuff
        byte[] value = new byte[32];
        random.nextBytes(value);
        // Prevent overflow / underflow
        if (value[0] == 0) {
            value[0]++;
        } else if (value[0] == -1) {
            value[0]--;
        }

        buffer.setIndex(0, value.length);
        buffer.setBytes(0, value);

        assertEquals(0, buffer.compareTo(new ProtonByteBuffer(value)));

        value[0]++;
        assertTrue(buffer.compareTo(new ProtonByteBuffer(value)) < 0);
        value[0] -= 2;
        assertTrue(buffer.compareTo(new ProtonByteBuffer(value)) > 0);
        value[0]++;

        assertTrue(buffer.compareTo(new ProtonByteBuffer(value, 0, 31)) > 0);
        assertTrue(buffer.slice(0, 31).compareTo(new ProtonByteBuffer(value)) < 0);
    }

    //----- Tests for readBytes variants -------------------------------------//

    @Test
    public void testReadBytesSingleArray() {
        byte[] payload = new byte[] { 0, 1, 2, 3, 4 };
        byte[] target = new byte[payload.length];

        ProtonBuffer buffer = wrapBuffer(payload);

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

        ProtonBuffer buffer = wrapBuffer(payload);

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

        ProtonBuffer buffer = wrapBuffer(payload);

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

        ProtonBuffer buffer = wrapBuffer(payload);
        ProtonBuffer target = allocateBuffer(5, 5);

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

        ProtonBuffer buffer = wrapBuffer(payload);
        ProtonBuffer target = allocateBuffer(5, 5);

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

        ProtonBuffer buffer = wrapBuffer(payload);
        ProtonBuffer target = allocateBuffer(5, 5);

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

        ProtonBuffer buffer = wrapBuffer(payload);
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
        ProtonBuffer buffer = allocateBuffer(5, 5);

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
        ProtonBuffer buffer = allocateBuffer(5, 5);

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
        ProtonBuffer buffer = allocateBuffer(10, 10);

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
        ProtonBuffer buffer = allocateBuffer(20, 20);

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
        ProtonBuffer buffer = allocateBuffer(40, 40);

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
        ProtonBuffer buffer = allocateBuffer(8, 8);

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
        ProtonBuffer buffer = allocateBuffer(16, 16);

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
        ProtonBuffer buffer = allocateBuffer(8, 8);

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

    //----- Test for setBytes methods ----------------------------------------//

    @Test
    public void testSetBytesUsingArray() {
        ProtonBuffer buffer = allocateBuffer(8, 8);

        byte[] other = new byte[] { 1, 2 };

        buffer.setWriteIndex(buffer.capacity());
        buffer.setBytes(0, other);

        assertEquals(1, buffer.readByte());
        assertEquals(2, buffer.readByte());
    }

    @Test
    public void testSetBytesUsingBufferAtIndex() {
        ProtonBuffer buffer = allocateBuffer(8, 8);
        ProtonBuffer other = allocateBuffer(8, 8);

        buffer.setWriteIndex(buffer.capacity());

        other.writeByte(1);
        other.writeByte(2);

        buffer.setBytes(0, other);

        assertEquals(1, buffer.readByte());
        assertEquals(2, buffer.readByte());

        assertTrue(other.getReadableBytes() == 0);
        assertFalse(other.isReadable());
    }

    @Test
    public void testSetBytesUsingBufferAtIndexWithLength() {
        ProtonBuffer buffer = allocateBuffer(8, 8);
        ProtonBuffer other = allocateBuffer(8, 8);

        buffer.setWriteIndex(buffer.capacity());

        other.writeByte(1);
        other.writeByte(2);

        buffer.setBytes(0, other, 2);

        assertEquals(1, buffer.readByte());
        assertEquals(2, buffer.readByte());

        assertTrue(other.getReadableBytes() == 0);
        assertFalse(other.isReadable());
    }

    @Test
    public void testSetBytesUsingBufferAtIndexHandleBoundsError() {
        ProtonBuffer buffer = allocateBuffer(8, 8);
        ProtonBuffer other = allocateBuffer(8, 8);

        buffer.setWriteIndex(buffer.capacity());

        other.writeByte(0);
        other.writeByte(1);

        try {
            buffer.setBytes(0, null, 0);
            fail("Should thrown NullPointerException");
        } catch (NullPointerException npe) {}

        try {
            buffer.setBytes(-1, other, 1);
            fail("Should thrown IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException iobe) {}

        try {
            buffer.setBytes(0, other, -1);
            fail("Should thrown IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException iobe) {}

        try {
            buffer.setBytes(-1, other, -1);
            fail("Should thrown IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException iobe) {}

        try {
            buffer.setBytes(10, other, 1);
            fail("Should thrown IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException iobe) {}

        try {
            buffer.setBytes(0, other, 10);
            fail("Should thrown IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException iobe) {}

        try {
            buffer.setBytes(0, other, 3);
            fail("Should thrown IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException iobe) {}

        try {
            buffer.setBytes(buffer.getWriteIndex() - 1, other, 2);
            fail("Should thrown IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException iobe) {}
    }

    @Test
    public void testSetBytesFromLargerBufferIntoSmallerBuffer() {
        ProtonBuffer buffer = allocateBuffer(2, 2);
        ProtonBuffer other = allocateBuffer(3, 3);

        other.writeByte(1);
        other.writeByte(2);
        other.writeByte(3);

        try {
            buffer.setBytes(0, other);
        } catch (IndexOutOfBoundsException iobe) {}

        buffer.setBytes(0, other, 2);

        assertFalse(buffer.isReadable());

        buffer.setWriteIndex(2);

        assertEquals(buffer.readByte(), 1);
        assertEquals(buffer.readByte(), 2);

        assertEquals(2, other.getReadIndex());
    }

    //----- Test for getBytes methods ----------------------------------------//

    @Test
    public void testGetBytesUsingBufferAtIndexHandleBoundsError() {
        ProtonBuffer buffer = allocateBuffer(8, 8);
        ProtonBuffer other = allocateBuffer(8, 8);

        buffer.setWriteIndex(buffer.capacity());

        other.writeByte(0);
        other.writeByte(1);

        try {
            buffer.getBytes(0, null, 0);
            fail("Should thrown NullPointerException");
        } catch (NullPointerException npe) {}

        try {
            buffer.getBytes(-1, other, 1);
            fail("Should thrown IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException iobe) {}

        try {
            buffer.getBytes(0, other, -1);
            fail("Should thrown IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException iobe) {}

        try {
            buffer.getBytes(-1, other, -1);
            fail("Should thrown IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException iobe) {}

        try {
            buffer.getBytes(10, other, 1);
            fail("Should thrown IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException iobe) {}

        try {
            buffer.getBytes(0, other, 10);
            fail("Should thrown IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException iobe) {}

        try {
            buffer.getBytes(buffer.getWriteIndex() - 1, other, 2);
            fail("Should thrown IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException iobe) {}
    }

    @Test
    public void testGetBytesUsingArray() {
        ProtonBuffer buffer = allocateBuffer(8, 8);

        byte[] data = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 };
        byte[] target = new byte[data.length];

        buffer.writeBytes(data);
        buffer.getBytes(0, target);

        for (int i = 0; i < data.length; ++i) {
            assertEquals(buffer.getByte(i), target[i]);
        }
    }

    @Test
    public void testGetBytesUsingArrayOffsetAndLength() {
        ProtonBuffer buffer = allocateBuffer(8, 8);

        byte[] data = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 };
        byte[] target = new byte[data.length];

        buffer.writeBytes(data);
        buffer.getBytes(0, target, 0, target.length);

        for (int i = 0; i < data.length; ++i) {
            assertEquals(buffer.getByte(i), target[i]);
        }

        byte[] target2 = new byte[data.length * 2];
        buffer.getBytes(0, target2, target.length, target.length);

        for (int i = 0; i < data.length; ++i) {
            assertEquals(buffer.getByte(i), target2[i + target.length]);
        }
    }

    @Test
    public void testGetBytesUsingBuffer() {
        ProtonBuffer buffer = allocateBuffer(8, 8);
        ProtonBuffer target = allocateBuffer(8, 8);

        byte[] data = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 };

        buffer.writeBytes(data);
        buffer.getBytes(0, target);

        assertTrue(target.isReadable());
        assertEquals(8, target.getReadableBytes());

        for (int i = 0; i < data.length; ++i) {
            assertEquals(buffer.getByte(i), target.getByte(i));
        }
    }

    @Test
    public void testGetBytesFromLargerBufferIntoSmallerBuffer() {
        ProtonBuffer buffer = allocateBuffer(2, 2);
        ProtonBuffer other = allocateBuffer(3, 3);

        other.writeByte(1);
        other.writeByte(2);
        other.writeByte(3);

        other.getBytes(0, buffer);

        assertTrue(buffer.isReadable());

        assertEquals(other.readByte(), 1);
        assertEquals(other.readByte(), 2);
        assertEquals(other.readByte(), 3);

        assertEquals(3, other.getReadIndex());

        assertEquals(buffer.readByte(), 1);
        assertEquals(buffer.readByte(), 2);
    }

    //----- Miscellaneous Stress Tests

    @Test
    public void testRandomByteAccess() {
        ProtonBuffer buffer = allocateBuffer(LARGE_CAPACITY);

        for (int i = 0; i < buffer.capacity(); i ++) {
            byte value = (byte) random.nextInt();
            buffer.setByte(i, value);
        }

        random.setSeed(seed);
        for (int i = 0; i < buffer.capacity(); i ++) {
            byte value = (byte) random.nextInt();
            assertEquals(value, buffer.getByte(i));
        }
    }

    @Test
    public void testRandomShortAccess() {
        ProtonBuffer buffer = allocateBuffer(LARGE_CAPACITY);

        for (int i = 0; i < buffer.capacity() - 1; i += 2) {
            short value = (short) random.nextInt();
            buffer.setShort(i, value);
        }

        random.setSeed(seed);
        for (int i = 0; i < buffer.capacity() - 1; i += 2) {
            short value = (short) random.nextInt();
            assertEquals(value, buffer.getShort(i));
        }
    }

    @Test
    public void testShortConsistentWithByteBuffer() {
        ProtonBuffer buffer = allocateBuffer(LARGE_CAPACITY);

        for (int i = 0; i < 64; ++i) {
            ByteBuffer javaBuffer = ByteBuffer.allocate(buffer.capacity());

            short expected = (short) (random.nextInt() & 0xFFFF);
            javaBuffer.putShort(expected);

            final int bufferIndex = buffer.capacity() - 2;
            buffer.setShort(bufferIndex, expected);
            javaBuffer.flip();

            short javaActual = javaBuffer.getShort();
            assertEquals(expected, javaActual);
            assertEquals(javaActual, buffer.getShort(bufferIndex));
        }
    }

    @Test
    public void testRandomIntAccess() {
        ProtonBuffer buffer = allocateBuffer(LARGE_CAPACITY);

        for (int i = 0; i < buffer.capacity() - 3; i += 4) {
            int value = random.nextInt();
            buffer.setInt(i, value);
        }

        random.setSeed(seed);
        for (int i = 0; i < buffer.capacity() - 3; i += 4) {
            int value = random.nextInt();
            assertEquals(value, buffer.getInt(i));
        }
    }

    @Test
    public void testIntConsistentWithByteBuffer() {
        ProtonBuffer buffer = allocateBuffer(LARGE_CAPACITY);

        for (int i = 0; i < 64; ++i) {
            ByteBuffer javaBuffer = ByteBuffer.allocate(buffer.capacity());
            int expected = random.nextInt();
            javaBuffer.putInt(expected);

            final int bufferIndex = buffer.capacity() - 4;
            buffer.setInt(bufferIndex, expected);
            javaBuffer.flip();

            int javaActual = javaBuffer.getInt();
            assertEquals(expected, javaActual);
            assertEquals(javaActual, buffer.getInt(bufferIndex));
        }
    }

    @Test
    public void testRandomLongAccess() {
        ProtonBuffer buffer = allocateBuffer(LARGE_CAPACITY);

        for (int i = 0; i < buffer.capacity() - 7; i += 8) {
            long value = random.nextLong();
            buffer.setLong(i, value);
        }

        random.setSeed(seed);
        for (int i = 0; i < buffer.capacity() - 7; i += 8) {
            long value = random.nextLong();
            assertEquals(value, buffer.getLong(i));
        }
    }

    @Test
    public void testLongConsistentWithByteBuffer() {
        ProtonBuffer buffer = allocateBuffer(LARGE_CAPACITY);

        for (int i = 0; i < 64; ++i) {
            ByteBuffer javaBuffer = ByteBuffer.allocate(buffer.capacity());

            long expected = random.nextLong();
            javaBuffer.putLong(expected);

            final int bufferIndex = buffer.capacity() - 8;
            buffer.setLong(bufferIndex, expected);
            javaBuffer.flip();

            long javaActual = javaBuffer.getLong();
            assertEquals(expected, javaActual);
            assertEquals(javaActual, buffer.getLong(bufferIndex));
        }
    }

    @Test
    public void testRandomFloatAccess() {
        ProtonBuffer buffer = allocateBuffer(LARGE_CAPACITY);

        for (int i = 0; i < buffer.capacity() - 7; i += 8) {
            float value = random.nextFloat();
            buffer.setFloat(i, value);
        }

        random.setSeed(seed);
        for (int i = 0; i < buffer.capacity() - 7; i += 8) {
            float expected = random.nextFloat();
            float actual = buffer.getFloat(i);
            assertEquals(expected, actual, 0.01);
        }
    }

    @Test
    public void testRandomDoubleAccess() {
        ProtonBuffer buffer = allocateBuffer(LARGE_CAPACITY);

        for (int i = 0; i < buffer.capacity() - 7; i += 8) {
            double value = random.nextDouble();
            buffer.setDouble(i, value);
        }

        random.setSeed(seed);
        for (int i = 0; i < buffer.capacity() - 7; i += 8) {
            double expected = random.nextDouble();
            double actual = buffer.getDouble(i);
            assertEquals(expected, actual, 0.01);
        }
    }

    @Test
    public void testSequentialByteAccess() {
        ProtonBuffer buffer = allocateBuffer(LARGE_CAPACITY);

        buffer.setWriteIndex(0);
        for (int i = 0; i < buffer.capacity(); i ++) {
            byte value = (byte) random.nextInt();
            assertEquals(i, buffer.getWriteIndex());
            assertTrue(buffer.isWritable());
            buffer.writeByte(value);
        }

        assertEquals(0, buffer.getReadIndex());
        assertEquals(buffer.capacity(), buffer.getWriteIndex());
        assertFalse(buffer.isWritable());

        random.setSeed(seed);
        for (int i = 0; i < buffer.capacity(); i ++) {
            byte value = (byte) random.nextInt();
            assertEquals(i, buffer.getReadIndex());
            assertTrue(buffer.isReadable());
            assertEquals(value, buffer.readByte());
        }

        assertEquals(buffer.capacity(), buffer.getReadIndex());
        assertEquals(buffer.capacity(), buffer.getWriteIndex());
        assertFalse(buffer.isReadable());
        assertFalse(buffer.isWritable());
    }

    @Test
    public void testSequentialShortAccess() {
        ProtonBuffer buffer = allocateBuffer(LARGE_CAPACITY);

        buffer.setWriteIndex(0);
        for (int i = 0; i < buffer.capacity(); i += 2) {
            short value = (short) random.nextInt();
            assertEquals(i, buffer.getWriteIndex());
            assertTrue(buffer.isWritable());
            buffer.writeShort(value);
        }

        assertEquals(0, buffer.getReadIndex());
        assertEquals(buffer.capacity(), buffer.getWriteIndex());
        assertFalse(buffer.isWritable());

        random.setSeed(seed);
        for (int i = 0; i < buffer.capacity(); i += 2) {
            short value = (short) random.nextInt();
            assertEquals(i, buffer.getReadIndex());
            assertTrue(buffer.isReadable());
            assertEquals(value, buffer.readShort());
        }

        assertEquals(buffer.capacity(), buffer.getReadIndex());
        assertEquals(buffer.capacity(), buffer.getWriteIndex());
        assertFalse(buffer.isReadable());
        assertFalse(buffer.isWritable());
    }

    @Test
    public void testSequentialIntAccess() {
        ProtonBuffer buffer = allocateBuffer(LARGE_CAPACITY);

        buffer.setWriteIndex(0);
        for (int i = 0; i < buffer.capacity(); i += 4) {
            int value = random.nextInt();
            assertEquals(i, buffer.getWriteIndex());
            assertTrue(buffer.isWritable());
            buffer.writeInt(value);
        }

        assertEquals(0, buffer.getReadIndex());
        assertEquals(buffer.capacity(), buffer.getWriteIndex());
        assertFalse(buffer.isWritable());

        random.setSeed(seed);
        for (int i = 0; i < buffer.capacity(); i += 4) {
            int value = random.nextInt();
            assertEquals(i, buffer.getReadIndex());
            assertTrue(buffer.isReadable());
            assertEquals(value, buffer.readInt());
        }

        assertEquals(buffer.capacity(), buffer.getReadIndex());
        assertEquals(buffer.capacity(), buffer.getWriteIndex());
        assertFalse(buffer.isReadable());
        assertFalse(buffer.isWritable());
    }

    @Test
    public void testSequentialLongAccess() {
        ProtonBuffer buffer = allocateBuffer(LARGE_CAPACITY);

        buffer.setWriteIndex(0);
        for (int i = 0; i < buffer.capacity(); i += 8) {
            long value = random.nextLong();
            assertEquals(i, buffer.getWriteIndex());
            assertTrue(buffer.isWritable());
            buffer.writeLong(value);
        }

        assertEquals(0, buffer.getReadIndex());
        assertEquals(buffer.capacity(), buffer.getWriteIndex());
        assertFalse(buffer.isWritable());

        random.setSeed(seed);
        for (int i = 0; i < buffer.capacity(); i += 8) {
            long value = random.nextLong();
            assertEquals(i, buffer.getReadIndex());
            assertTrue(buffer.isReadable());
            assertEquals(value, buffer.readLong());
        }

        assertEquals(buffer.capacity(), buffer.getReadIndex());
        assertEquals(buffer.capacity(), buffer.getWriteIndex());
        assertFalse(buffer.isReadable());
        assertFalse(buffer.isWritable());
    }

    @Test
    public void testByteArrayTransfer() {
        testByteArrayTransfer(false);
    }

    @Test
    public void testByteArrayTransferDirectBackedBuffer() {
        assumeTrue(canAllocateDirectBackedBuffers());
        testByteArrayTransfer(true);
    }

    private void testByteArrayTransfer(boolean direct) {
        final ProtonBuffer buffer;

        if (direct) {
            buffer = allocateDirectBuffer(LARGE_CAPACITY);
        } else {
            buffer = allocateBuffer(LARGE_CAPACITY);
        }

        byte[] value = new byte[BLOCK_SIZE * 2];
        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            random.nextBytes(value);
            buffer.setBytes(i, value, random.nextInt(BLOCK_SIZE), BLOCK_SIZE);
        }

        random.setSeed(seed);
        byte[] expectedValue = new byte[BLOCK_SIZE * 2];
        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            random.nextBytes(expectedValue);
            int valueOffset = random.nextInt(BLOCK_SIZE);
            buffer.getBytes(i, value, valueOffset, BLOCK_SIZE);
            for (int j = valueOffset; j < valueOffset + BLOCK_SIZE; j ++) {
                assertEquals(expectedValue[j], value[j]);
            }
        }
    }

    @Test
    public void testRandomByteArrayTransfer1() {
        doTestRandomByteArrayTransfer1(false);
    }

    @Test
    public void testRandomByteArrayTransfer1DirectBackedBuffer() {
        assumeTrue(canAllocateDirectBackedBuffers());
        doTestRandomByteArrayTransfer1(true);
    }

    private void doTestRandomByteArrayTransfer1(boolean direct) {
        final ProtonBuffer buffer;
        if (direct) {
            buffer = allocateDirectBuffer(LARGE_CAPACITY);
        } else {
            buffer = allocateBuffer(LARGE_CAPACITY);
        }

        byte[] value = new byte[BLOCK_SIZE];
        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            random.nextBytes(value);
            buffer.setBytes(i, value);
        }

        random.setSeed(seed);
        byte[] expectedValueContent = new byte[BLOCK_SIZE];
        ProtonBuffer expectedValue = new ProtonByteBuffer(expectedValueContent);
        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            random.nextBytes(expectedValueContent);
            buffer.getBytes(i, value);
            for (int j = 0; j < BLOCK_SIZE; j ++) {
                assertEquals(expectedValue.getByte(j), value[j]);
            }
        }
    }

    @Test
    public void testRandomByteArrayTransfer2() {
        dotestRandomByteArrayTransfer2(false);
    }

    @Test
    public void testRandomByteArrayTransfer2DirectBackedBuffer() {
        assumeTrue(canAllocateDirectBackedBuffers());
        dotestRandomByteArrayTransfer2(true);
    }

    private void dotestRandomByteArrayTransfer2(boolean direct) {
        final ProtonBuffer buffer;
        if (direct) {
            buffer = allocateDirectBuffer(LARGE_CAPACITY);
        } else {
            buffer = allocateBuffer(LARGE_CAPACITY);
        }

        byte[] value = new byte[BLOCK_SIZE * 2];
        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            random.nextBytes(value);
            buffer.setBytes(i, value, random.nextInt(BLOCK_SIZE), BLOCK_SIZE);
        }

        random.setSeed(seed);
        byte[] expectedValueContent = new byte[BLOCK_SIZE * 2];
        ProtonBuffer expectedValue = new ProtonByteBuffer(expectedValueContent);
        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            random.nextBytes(expectedValueContent);
            int valueOffset = random.nextInt(BLOCK_SIZE);
            buffer.getBytes(i, value, valueOffset, BLOCK_SIZE);
            for (int j = valueOffset; j < valueOffset + BLOCK_SIZE; j ++) {
                assertEquals(expectedValue.getByte(j), value[j]);
            }
        }
    }

    @Test
    public void testRandomProtonBufferTransfer1() {
        doTestRandomProtonBufferTransfer1(false, false);
    }

    @Test
    public void testRandomProtonBufferTransfer1DirectSource() {
        doTestRandomProtonBufferTransfer1(true, false);
    }

    @Test
    public void testRandomProtonBufferTransfer1DirectTarget() {
        assumeTrue(canAllocateDirectBackedBuffers());
        doTestRandomProtonBufferTransfer1(false, true);
    }

    @Test
    public void testRandomProtonBufferTransfer1DirectSourceAndTarget() {
        assumeTrue(canAllocateDirectBackedBuffers());
        doTestRandomProtonBufferTransfer1(true, true);
    }

    private void doTestRandomProtonBufferTransfer1(boolean directSource, boolean directTarget) {
        final ProtonBuffer buffer;
        if (directTarget) {
            buffer = allocateDirectBuffer(LARGE_CAPACITY);
        } else {
            buffer = allocateBuffer(LARGE_CAPACITY);
        }

        byte[] valueContent = new byte[BLOCK_SIZE];
        final ProtonBuffer value;
        if (directSource) {
            value = new ProtonNioByteBuffer(ByteBuffer.allocateDirect(BLOCK_SIZE));
        } else {
            value = new ProtonByteBuffer(BLOCK_SIZE, BLOCK_SIZE);
        }

        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            random.nextBytes(valueContent);
            value.clear();
            value.writeBytes(valueContent);
            buffer.setBytes(i, value);
            assertEquals(BLOCK_SIZE, value.getReadIndex());
            assertEquals(BLOCK_SIZE, value.getWriteIndex());
        }

        random.setSeed(seed);
        byte[] expectedValueContent = new byte[BLOCK_SIZE];
        ProtonBuffer expectedValue = new ProtonByteBuffer(expectedValueContent);
        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            random.nextBytes(expectedValueContent);
            value.clear();
            buffer.getBytes(i, value);
            assertEquals(0, value.getReadIndex());
            assertEquals(BLOCK_SIZE, value.getWriteIndex());
            for (int j = 0; j < BLOCK_SIZE; j ++) {
                assertEquals(expectedValue.getByte(j), value.getByte(j));
            }
        }
    }

    @Test
    public void testRandomProtonBufferTransfer2() {
        doTestRandomProtonBufferTransfer2(false, false);
    }

    @Test
    public void testRandomProtonBufferTransfer2DirectSource() {
        doTestRandomProtonBufferTransfer2(true, false);
    }

    @Test
    public void testRandomProtonBufferTransfer2DirectTarget() {
        assumeTrue(canAllocateDirectBackedBuffers());
        doTestRandomProtonBufferTransfer2(false, true);
    }

    @Test
    public void testRandomProtonBufferTransfer2DirectSourceAndTarget() {
        assumeTrue(canAllocateDirectBackedBuffers());
        doTestRandomProtonBufferTransfer2(true, true);
    }

    private void doTestRandomProtonBufferTransfer2(boolean directSource, boolean directTarget) {
        final ProtonBuffer buffer;
        if (directTarget) {
            buffer = allocateDirectBuffer(LARGE_CAPACITY);
        } else {
            buffer = allocateBuffer(LARGE_CAPACITY);
        }

        final int SIZE = BLOCK_SIZE * 2;
        byte[] valueContent = new byte[SIZE];
        final ProtonBuffer value;
        if (directSource) {
            value = new ProtonNioByteBuffer(ByteBuffer.allocateDirect(SIZE));
        } else {
            value = new ProtonByteBuffer(SIZE, SIZE);
        }

        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            random.nextBytes(valueContent);
            value.clear();
            value.writeBytes(valueContent);
            buffer.setBytes(i, value, random.nextInt(BLOCK_SIZE), BLOCK_SIZE);
        }

        random.setSeed(seed);
        byte[] expectedValueContent = new byte[SIZE];
        ProtonBuffer expectedValue = new ProtonByteBuffer(expectedValueContent);
        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            random.nextBytes(expectedValueContent);
            int valueOffset = random.nextInt(BLOCK_SIZE);
            buffer.getBytes(i, value, valueOffset, BLOCK_SIZE);
            for (int j = valueOffset; j < valueOffset + BLOCK_SIZE; j ++) {
                assertEquals(expectedValue.getByte(j), value.getByte(j));
            }
        }
    }

    @Test
    public void testRandomByteBufferTransfer() {
        doTestRandomByteBufferTransfer(false, false);
    }

    @Test
    public void testRandomDirectByteBufferTransfer() {
        doTestRandomByteBufferTransfer(true, false);
    }

    @Test
    public void testRandomByteBufferTransferToDirectProtonBuffer() {
        assumeTrue(canAllocateDirectBackedBuffers());
        doTestRandomByteBufferTransfer(false, true);
    }

    @Test
    public void testRandomDirectByteBufferTransferToDirectProtonBuffer() {
        assumeTrue(canAllocateDirectBackedBuffers());
        doTestRandomByteBufferTransfer(true, true);
    }

    private void doTestRandomByteBufferTransfer(boolean directSource, boolean directTarget) {
        final ProtonBuffer buffer;
        if (directTarget) {
            buffer = allocateDirectBuffer(LARGE_CAPACITY);
        } else {
            buffer = allocateBuffer(LARGE_CAPACITY);
        }

        final int SIZE = BLOCK_SIZE * 2;
        final byte[] valueContent = new byte[SIZE];
        final ByteBuffer value;
        if (directSource) {
            value = ByteBuffer.allocateDirect(BLOCK_SIZE * 2);
        } else {
            value = ByteBuffer.allocate(BLOCK_SIZE * 2);
        }

        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            random.nextBytes(valueContent);
            value.clear();
            value.put(valueContent);
            value.clear().position(random.nextInt(BLOCK_SIZE));
            value.limit(value.position() + BLOCK_SIZE);
            buffer.setBytes(i, value);
        }

        random.setSeed(seed);
        ByteBuffer expectedValue = ByteBuffer.allocate(BLOCK_SIZE * 2);
        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            random.nextBytes(expectedValue.array());
            int valueOffset = random.nextInt(BLOCK_SIZE);
            value.clear().position(valueOffset).limit(valueOffset + BLOCK_SIZE);
            buffer.getBytes(i, value);
            assertEquals(valueOffset + BLOCK_SIZE, value.position());
            for (int j = valueOffset; j < valueOffset + BLOCK_SIZE; j ++) {
                assertEquals(expectedValue.get(j), value.get(j));
            }
        }
    }

    @Test
    public void testSequentialByteArrayTransfer1() {
        dotestSequentialByteArrayTransfer1(false);
    }

    @Test
    public void testSequentialByteArrayTransfer1DirectBackedBuffer() {
        assumeTrue(canAllocateDirectBackedBuffers());
        dotestSequentialByteArrayTransfer1(true);
    }

    private void dotestSequentialByteArrayTransfer1(boolean direct) {
        final ProtonBuffer buffer;
        if (direct) {
            buffer = allocateDirectBuffer(LARGE_CAPACITY);
        } else {
            buffer = allocateBuffer(LARGE_CAPACITY);
        }

        byte[] value = new byte[BLOCK_SIZE];
        buffer.setWriteIndex(0);
        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            random.nextBytes(value);
            assertEquals(0, buffer.getReadIndex());
            assertEquals(i, buffer.getWriteIndex());
            buffer.writeBytes(value);
        }

        random.setSeed(seed);
        byte[] expectedValue = new byte[BLOCK_SIZE];
        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            random.nextBytes(expectedValue);
            assertEquals(i, buffer.getReadIndex());
            assertEquals(LARGE_CAPACITY, buffer.getWriteIndex());
            buffer.readBytes(value);
            for (int j = 0; j < BLOCK_SIZE; j ++) {
                assertEquals(expectedValue[j], value[j]);
            }
        }
    }

    @Test
    public void testSequentialByteArrayTransfer2() {
        doTestSequentialByteArrayTransfer2(false);
    }

    @Test
    public void testSequentialByteArrayTransfer2DirectBackedBuffer() {
        assumeTrue(canAllocateDirectBackedBuffers());
        doTestSequentialByteArrayTransfer2(true);
    }

    private void doTestSequentialByteArrayTransfer2(boolean direct) {
        final ProtonBuffer buffer;
        if (direct) {
            buffer = allocateDirectBuffer(LARGE_CAPACITY);
        } else {
            buffer = allocateBuffer(LARGE_CAPACITY);
        }

        byte[] value = new byte[BLOCK_SIZE * 2];
        buffer.setWriteIndex(0);
        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            random.nextBytes(value);
            assertEquals(0, buffer.getReadIndex());
            assertEquals(i, buffer.getWriteIndex());
            int readerIndex = random.nextInt(BLOCK_SIZE);
            buffer.writeBytes(value, readerIndex, BLOCK_SIZE);
        }

        random.setSeed(seed);
        byte[] expectedValue = new byte[BLOCK_SIZE * 2];
        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            random.nextBytes(expectedValue);
            int valueOffset = random.nextInt(BLOCK_SIZE);
            assertEquals(i, buffer.getReadIndex());
            assertEquals(LARGE_CAPACITY, buffer.getWriteIndex());
            buffer.readBytes(value, valueOffset, BLOCK_SIZE);
            for (int j = valueOffset; j < valueOffset + BLOCK_SIZE; j ++) {
                assertEquals(expectedValue[j], value[j]);
            }
        }
    }

    @Test
    public void testSequentialProtonBufferTransfer1() {
        doTestSequentialProtonBufferTransfer1(false, false);
    }

    @Test
    public void testSequentialProtonBufferTransfer1DirectSource() {
        doTestSequentialProtonBufferTransfer1(true, false);
    }

    @Test
    public void testSequentialProtonBufferTransfer1DirectTargetBuffer() {
        assumeTrue(canAllocateDirectBackedBuffers());
        doTestSequentialProtonBufferTransfer1(false, true);
    }

    @Test
    public void testSequentialProtonBufferTransfer1DirectSourceAndTargetBuffers() {
        assumeTrue(canAllocateDirectBackedBuffers());
        doTestSequentialProtonBufferTransfer1(true, true);
    }

    private void doTestSequentialProtonBufferTransfer1(boolean directSource, boolean directTarget) {
        final ProtonBuffer buffer;
        if (directTarget) {
            buffer = allocateDirectBuffer(LARGE_CAPACITY);
        } else {
            buffer = allocateBuffer(LARGE_CAPACITY);
        }

        final int SIZE = BLOCK_SIZE * 2;
        final byte[] valueContent = new byte[SIZE];
        final ProtonBuffer value;
        if (directSource) {
            value = new ProtonNioByteBuffer(ByteBuffer.allocateDirect(SIZE));
        } else {
            value = new ProtonByteBuffer(SIZE, SIZE);
        }

        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            random.nextBytes(valueContent);
            value.clear().writeBytes(valueContent);
            assertEquals(0, buffer.getReadIndex());
            assertEquals(i, buffer.getWriteIndex());
            buffer.writeBytes(value, random.nextInt(BLOCK_SIZE), BLOCK_SIZE);
            assertEquals(0, value.getReadIndex());
            assertEquals(valueContent.length, value.getWriteIndex());
        }

        random.setSeed(seed);
        byte[] expectedValueContent = new byte[BLOCK_SIZE * 2];
        ProtonBuffer expectedValue = new ProtonByteBuffer(expectedValueContent);
        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            random.nextBytes(expectedValueContent);
            int valueOffset = random.nextInt(BLOCK_SIZE);
            assertEquals(i, buffer.getReadIndex());
            assertEquals(LARGE_CAPACITY, buffer.getWriteIndex());
            buffer.readBytes(value, valueOffset, BLOCK_SIZE);
            for (int j = valueOffset; j < valueOffset + BLOCK_SIZE; j ++) {
                assertEquals(expectedValue.getByte(j), value.getByte(j));
            }
            assertEquals(0, value.getReadIndex());
            assertEquals(valueContent.length, value.getWriteIndex());
        }
    }

    @Test
    public void testSequentialProtonBufferTransfer2() {
        doTestSequentialProtonBufferTransfer2(false, false);
    }

    @Test
    public void testSequentialProtonBufferTransfer2DirectSourceBuffer() {
        doTestSequentialProtonBufferTransfer2(true, false);
    }

    @Test
    public void testSequentialProtonBufferTransfer2DirectTargetBuffer() {
        assumeTrue(canAllocateDirectBackedBuffers());
        doTestSequentialProtonBufferTransfer2(false, true);
    }

    @Test
    public void testSequentialProtonBufferTransfer2DirectSourceAndTargetBuffer() {
        assumeTrue(canAllocateDirectBackedBuffers());
        doTestSequentialProtonBufferTransfer2(true, true);
    }

    private void doTestSequentialProtonBufferTransfer2(boolean directSource, boolean directTarget) {
        final ProtonBuffer buffer;
        if (directTarget) {
            buffer = allocateDirectBuffer(LARGE_CAPACITY);
        } else {
            buffer = allocateBuffer(LARGE_CAPACITY);
        }

        final int SIZE = BLOCK_SIZE * 2;
        final byte[] valueContent = new byte[SIZE];
        final ProtonBuffer value;
        if (directSource) {
            value = new ProtonNioByteBuffer(ByteBuffer.allocateDirect(SIZE));
        } else {
            value = new ProtonByteBuffer(SIZE, SIZE);
        }

        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            random.nextBytes(valueContent);
            value.clear().writeBytes(valueContent);
            assertEquals(0, buffer.getReadIndex());
            assertEquals(i, buffer.getWriteIndex());
            int readerIndex = random.nextInt(BLOCK_SIZE);
            value.setReadIndex(readerIndex);
            value.setWriteIndex(readerIndex + BLOCK_SIZE);
            buffer.writeBytes(value);
            assertEquals(readerIndex + BLOCK_SIZE, value.getWriteIndex());
            assertEquals(value.getWriteIndex(), value.getReadIndex());
        }

        random.setSeed(seed);
        byte[] expectedValueContent = new byte[BLOCK_SIZE * 2];
        ProtonBuffer expectedValue = new ProtonByteBuffer(expectedValueContent);
        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            random.nextBytes(expectedValueContent);
            int valueOffset = random.nextInt(BLOCK_SIZE);
            assertEquals(i, buffer.getReadIndex());
            assertEquals(LARGE_CAPACITY, buffer.getWriteIndex());
            value.setReadIndex(valueOffset);
            value.setWriteIndex(valueOffset);
            buffer.readBytes(value, BLOCK_SIZE);
            for (int j = valueOffset; j < valueOffset + BLOCK_SIZE; j ++) {
                assertEquals(expectedValue.getByte(j), value.getByte(j));
            }
            assertEquals(valueOffset, value.getReadIndex());
            assertEquals(valueOffset + BLOCK_SIZE, value.getWriteIndex());
        }
    }

    @Test
    public void testSequentialProtonBufferTransfer3() {
        doTestSequentialProtonBufferTransfer3(false, false);
    }

    @Test
    public void testSequentialProtonBufferTransfer3DirectSource() {
        doTestSequentialProtonBufferTransfer3(true, false);
    }

    @Test
    public void testSequentialProtonBufferTransfer3DirectTargetBuffer() {
        assumeTrue(canAllocateDirectBackedBuffers());
        doTestSequentialProtonBufferTransfer3(false, true);
    }

    @Test
    public void testSequentialProtonBufferTransfer3DirectSourceAndTargetBuffers() {
        assumeTrue(canAllocateDirectBackedBuffers());
        doTestSequentialProtonBufferTransfer3(true, true);
    }

    private void doTestSequentialProtonBufferTransfer3(boolean directSource, boolean directTarget) {
        final ProtonBuffer buffer;
        if (directTarget) {
            buffer = allocateDirectBuffer(LARGE_CAPACITY);
        } else {
            buffer = allocateBuffer(LARGE_CAPACITY);
        }

        final byte[] valueContent = new byte[BLOCK_SIZE];
        final ProtonBuffer value;
        if (directSource) {
            value = new ProtonNioByteBuffer(ByteBuffer.allocateDirect(BLOCK_SIZE));
        } else {
            value = new ProtonByteBuffer(BLOCK_SIZE, BLOCK_SIZE);
        }

        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            random.nextBytes(valueContent);
            value.clear().writeBytes(valueContent);
            assertEquals(0, value.getReadIndex());
            assertEquals(BLOCK_SIZE, value.getWriteIndex());
            buffer.setWriteIndex(buffer.getWriteIndex() + BLOCK_SIZE);
            buffer.setBytes(i, value, BLOCK_SIZE);
            assertEquals(BLOCK_SIZE, value.getReadIndex());
            assertEquals(BLOCK_SIZE, value.getWriteIndex());
        }

        random.setSeed(seed);
        byte[] expectedValueContent = new byte[BLOCK_SIZE];
        ProtonBuffer expectedValue = new ProtonByteBuffer(expectedValueContent);
        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            random.nextBytes(expectedValueContent);
            value.clear();
            assertEquals(0, value.getReadIndex());
            assertEquals(0, value.getWriteIndex());
            buffer.getBytes(i, value, BLOCK_SIZE);
            assertEquals(0, value.getReadIndex());
            assertEquals(BLOCK_SIZE, value.getWriteIndex());
            for (int j = 0; j < BLOCK_SIZE; j++) {
                assertEquals(expectedValue.getByte(j), value.getByte(j));
            }
        }
    }

    //----- Tests need to define these allocation methods

    /**
     * @return true if the buffer type under test support capacity alterations.
     */
    protected boolean canBufferCapacityBeChanged() {
        return true;
    }

    /**
     * @return true if the buffer implementation can allocate a version with a direct buffer backed implementation.
     */
    protected abstract boolean canAllocateDirectBackedBuffers();

    /**
     * @return a ProtonBuffer allocated with defaults for capacity and max-capacity.
     */
    protected ProtonBuffer allocateDefaultBuffer() {
        return allocateBuffer(DEFAULT_CAPACITY);
    }

    /**
     * @return a ProtonBuffer allocated with defaults for capacity and max-capacity.
     */
    protected ProtonBuffer allocateDefaultDirectBuffer() {
        return allocateDirectBuffer(DEFAULT_CAPACITY);
    }

    /**
     * @param initialCapacity the initial capacity to assign the returned buffer
     *
     * @return a ProtonBuffer allocated with the given capacity and a default max-capacity.
     */
    protected abstract ProtonBuffer allocateBuffer(int initialCapacity);

    /**
     * @param initialCapacity the initial capacity to assign the returned buffer
     *
     * @return a ProtonBuffer allocated with the given capacity and a default max-capacity.
     */
    protected abstract ProtonBuffer allocateDirectBuffer(int initialCapacity);

    /**
     * @param initialCapacity the initial capacity to assign the returned buffer
     * @param maxCapacity the maximum capacity the buffer is allowed to grow to
     *
     * @return a ProtonBuffer allocated with the given capacity and the given max-capacity.
     */
    protected abstract ProtonBuffer allocateBuffer(int initialCapacity, int maxCapacity);

    /**
     * @param initialCapacity the initial capacity to assign the returned buffer
     * @param maxCapacity the maximum capacity the buffer is allowed to grow to
     *
     * @return a ProtonBuffer allocated with the given capacity and the given max-capacity.
     */
    protected abstract ProtonBuffer allocateDirectBuffer(int initialCapacity, int maxCapacity);

    /**
     * @param array the byte array to wrap with the given buffer under test.
     *
     * @return a ProtonBuffer that wraps the given buffer.
     */
    protected abstract ProtonBuffer wrapBuffer(byte[] array);

    //----- Test support methods

    public static void assertRemainingEquals(ByteBuffer expected, ByteBuffer actual) {
        int remaining1 = expected.remaining();
        int remaining2 = actual.remaining();

        assertEquals(remaining1, remaining2);
        byte[] array1 = new byte[remaining1];
        byte[] array2 = new byte[remaining2];
        expected.get(array1);
        actual.get(array2);
        assertArrayEquals(array1, array2);
    }
}
