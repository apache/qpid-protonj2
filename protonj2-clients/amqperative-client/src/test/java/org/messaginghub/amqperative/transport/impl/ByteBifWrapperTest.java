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
package org.messaginghub.amqperative.transport.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.util.Random;

import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBuffer;
import org.junit.Before;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * Test the buffer wrapper around Netty ByteBuf instances
 */
public class ByteBifWrapperTest {

    private static final int CAPACITY = 4096; // Must be even
    private static final int BLOCK_SIZE = 128;

    public static final byte[] EMPTY_BYTES = {};

    private long seed;
    private Random random;

    @Before
    public void setUp() {
        seed = System.currentTimeMillis();
        random = new Random(seed);
    }

    @Test
    public void testReaderIndexBoundaryCheck4() {
        ByteBuf buffer = Unpooled.buffer(CAPACITY);
        ByteBufWrapper wrapper = new ByteBufWrapper(buffer);

        wrapper.setWriteIndex(0);
        wrapper.setReadIndex(0);
        wrapper.setWriteIndex(buffer.capacity());
        wrapper.setReadIndex(buffer.capacity());
    }

    @Test
    public void testCreateWrapper() {
        ByteBuf buffer = Unpooled.buffer();
        ByteBufWrapper wrapper = new ByteBufWrapper(buffer);

        assertEquals(buffer.capacity(), wrapper.capacity());
        assertEquals(buffer.readableBytes(), wrapper.getReadableBytes());
        assertEquals(buffer.writableBytes(), wrapper.getWritableBytes());
        assertEquals(buffer.readerIndex(), wrapper.getReadIndex());
        assertEquals(buffer.writerIndex(), wrapper.getWriteIndex());
    }

    @Test
    public void testReadByteFromWrapper() {
        ByteBuf buffer = Unpooled.buffer();

        for (int i = 0; i < 256; ++i) {
            buffer.writeByte(i);
        }

        ByteBufWrapper wrapper = new ByteBufWrapper(buffer);

        assertEquals(buffer.capacity(), wrapper.capacity());
        assertEquals(buffer.readableBytes(), wrapper.getReadableBytes());
        assertEquals(buffer.writableBytes(), wrapper.getWritableBytes());
        assertEquals(buffer.readerIndex(), wrapper.getReadIndex());
        assertEquals(buffer.writerIndex(), wrapper.getWriteIndex());

        for (int i = 0; i < 256; ++i) {
            assertEquals((byte) i, wrapper.readByte());
        }
    }

    @Test
    public void testReadShortFromWrapper() {
        ByteBuf buffer = Unpooled.buffer();

        for (int i = 0; i < 256; ++i) {
            buffer.writeShort(i);
        }

        ByteBufWrapper wrapper = new ByteBufWrapper(buffer);

        assertEquals(buffer.capacity(), wrapper.capacity());
        assertEquals(buffer.readableBytes(), wrapper.getReadableBytes());
        assertEquals(buffer.writableBytes(), wrapper.getWritableBytes());
        assertEquals(buffer.readerIndex(), wrapper.getReadIndex());
        assertEquals(buffer.writerIndex(), wrapper.getWriteIndex());

        for (int i = 0; i < 256; ++i) {
            assertEquals((short) i, wrapper.readShort());
        }
    }

    @Test
    public void testReadIntFromWrapper() {
        ByteBuf buffer = Unpooled.buffer();

        for (int i = 0; i < 256; ++i) {
            buffer.writeInt(i);
        }

        ByteBufWrapper wrapper = new ByteBufWrapper(buffer);

        assertEquals(buffer.capacity(), wrapper.capacity());
        assertEquals(buffer.readableBytes(), wrapper.getReadableBytes());
        assertEquals(buffer.writableBytes(), wrapper.getWritableBytes());
        assertEquals(buffer.readerIndex(), wrapper.getReadIndex());
        assertEquals(buffer.writerIndex(), wrapper.getWriteIndex());

        for (int i = 0; i < 256; ++i) {
            assertEquals(i, wrapper.readInt());
        }
    }

    @Test
    public void testReadLongFromWrapper() {
        ByteBuf buffer = Unpooled.buffer();

        for (int i = 0; i < 256; ++i) {
            buffer.writeLong(i);
        }

        ByteBufWrapper wrapper = new ByteBufWrapper(buffer);

        assertEquals(buffer.capacity(), wrapper.capacity());
        assertEquals(buffer.readableBytes(), wrapper.getReadableBytes());
        assertEquals(buffer.writableBytes(), wrapper.getWritableBytes());
        assertEquals(buffer.readerIndex(), wrapper.getReadIndex());
        assertEquals(buffer.writerIndex(), wrapper.getWriteIndex());

        for (int i = 0; i < 256; ++i) {
            assertEquals(i, wrapper.readLong());
        }
    }

    @Test
    public void testComparableInterfaceNotViolatedWithLongWrites() {
        ByteBuf buffer1 = Unpooled.buffer(CAPACITY);
        ByteBuf buffer2 = Unpooled.buffer(CAPACITY);

        ByteBufWrapper wrapper1 = new ByteBufWrapper(buffer1);
        ByteBufWrapper wrapper2 = new ByteBufWrapper(buffer2);

        wrapper1.setWriteIndex(wrapper2.getReadIndex());
        wrapper1.writeLong(0);

        wrapper2.setWriteIndex(wrapper2.getReadIndex());
        wrapper2.writeLong(0xF0000000L);

        assertTrue(wrapper1.compareTo(wrapper2) < 0);
        assertTrue(wrapper2.compareTo(wrapper1) > 0);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testSetReadIndexBoundaryCheckForNegative() {
        ByteBuf buffer = Unpooled.buffer(CAPACITY);
        ByteBufWrapper wrapper = new ByteBufWrapper(buffer);

        try {
            wrapper.setWriteIndex(0);
        } catch (IndexOutOfBoundsException e) {
            fail("Should be able to set index to zero");
        }
        wrapper.setReadIndex(-1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testSetReadIndexBoundaryCheckForOverCapacityValue() {
        ByteBuf buffer = Unpooled.buffer(CAPACITY);
        ByteBufWrapper wrapper = new ByteBufWrapper(buffer);

        try {
            wrapper.setWriteIndex(buffer.capacity());
        } catch (IndexOutOfBoundsException e) {
            fail();
        }
        wrapper.setReadIndex(buffer.capacity() + 1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void setReadIndexBoundaryCheckValueBeyondWriteIndex() {
        ByteBuf buffer = Unpooled.buffer(CAPACITY);
        ByteBufWrapper wrapper = new ByteBufWrapper(buffer);

        try {
            wrapper.setWriteIndex(CAPACITY / 2);
        } catch (IndexOutOfBoundsException e) {
            fail();
        }
        wrapper.setReadIndex(CAPACITY * 3 / 2);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void setWriteIndexBoundaryCheckValueBeyondCapacity() {
        ByteBuf buffer = Unpooled.buffer(CAPACITY);
        ByteBufWrapper wrapper = new ByteBufWrapper(buffer);

        try {
            wrapper.setWriteIndex(CAPACITY);
            wrapper.setReadIndex(CAPACITY);
        } catch (IndexOutOfBoundsException e) {
            fail("Should be able to place indices at capacity");
        }
        wrapper.setWriteIndex(wrapper.capacity() + 1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void setWriteIndexBoundaryCheckWriteIndexBelowReadIndex() {
        ByteBuf buffer = Unpooled.buffer(CAPACITY);
        ByteBufWrapper wrapper = new ByteBufWrapper(buffer);

        try {
            wrapper.setWriteIndex(CAPACITY);
            wrapper.setReadIndex(CAPACITY / 2);
        } catch (IndexOutOfBoundsException e) {
            fail("Should be able to place indices at capacity and half capacity");
        }
        wrapper.setWriteIndex(CAPACITY / 4);
    }

    @Test
    public void testWriterIndexBoundaryCheckEmptyWriteDoesNotThrow() {
        ByteBuf buffer = Unpooled.buffer(CAPACITY);
        ByteBufWrapper wrapper = new ByteBufWrapper(buffer);

        wrapper.setWriteIndex(0);
        wrapper.setReadIndex(0);
        wrapper.setWriteIndex(CAPACITY);

        wrapper.writeBytes(ByteBuffer.wrap(EMPTY_BYTES));
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetBooleanBoundaryCheck1() {
        ByteBuf buffer = Unpooled.buffer(CAPACITY);
        ByteBufWrapper wrapper = new ByteBufWrapper(buffer);
        wrapper.getBoolean(-1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetBooleanBoundaryCheck2() {
        ByteBuf buffer = Unpooled.buffer(CAPACITY);
        ByteBufWrapper wrapper = new ByteBufWrapper(buffer);
        wrapper.getBoolean(wrapper.capacity());
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetByteBoundaryCheck1() {
        ByteBuf buffer = Unpooled.buffer(CAPACITY);
        ByteBufWrapper wrapper = new ByteBufWrapper(buffer);
        wrapper.getByte(-1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetByteBoundaryCheck2() {
        ByteBuf buffer = Unpooled.buffer(CAPACITY);
        ByteBufWrapper wrapper = new ByteBufWrapper(buffer);
        wrapper.getByte(wrapper.capacity());
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetShortBoundaryCheck1() {
        ByteBuf buffer = Unpooled.buffer(CAPACITY);
        ByteBufWrapper wrapper = new ByteBufWrapper(buffer);
        wrapper.getShort(-1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetShortBoundaryCheck2() {
        ByteBuf buffer = Unpooled.buffer(CAPACITY);
        ByteBufWrapper wrapper = new ByteBufWrapper(buffer);
        wrapper.getShort(wrapper.capacity() - 1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetIntBoundaryCheck1() {
        ByteBuf buffer = Unpooled.buffer(CAPACITY);
        ByteBufWrapper wrapper = new ByteBufWrapper(buffer);
        wrapper.getInt(-1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetIntBoundaryCheck2() {
        ByteBuf buffer = Unpooled.buffer(CAPACITY);
        ByteBufWrapper wrapper = new ByteBufWrapper(buffer);
        wrapper.getInt(wrapper.capacity() - 3);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetLongBoundaryCheck1() {
        ByteBuf buffer = Unpooled.buffer(CAPACITY);
        ByteBufWrapper wrapper = new ByteBufWrapper(buffer);
        wrapper.getLong(-1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetLongBoundaryCheck2() {
        ByteBuf buffer = Unpooled.buffer(CAPACITY);
        ByteBufWrapper wrapper = new ByteBufWrapper(buffer);
        wrapper.getLong(wrapper.capacity() - 7);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetByteArrayBoundaryCheck1() {
        ByteBuf buffer = Unpooled.buffer(CAPACITY);
        ByteBufWrapper wrapper = new ByteBufWrapper(buffer);
        wrapper.getBytes(-1, EMPTY_BYTES);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetByteArrayBoundaryCheck2() {
        ByteBuf buffer = Unpooled.buffer(CAPACITY);
        ByteBufWrapper wrapper = new ByteBufWrapper(buffer);
        wrapper.getBytes(-1, EMPTY_BYTES, 0, 0);
    }

    @Test
    public void testGetByteArrayBoundaryCheckWithNegativeOffset() {
        ByteBuf netty = Unpooled.buffer(CAPACITY);
        ByteBufWrapper wrapper = new ByteBufWrapper(netty);

        byte[] dst = new byte[4];
        wrapper.setInt(0, 0x01020304);
        try {
            wrapper.getBytes(0, dst, -1, 4);
            fail("Should not allow offset out of range.");
        } catch (IndexOutOfBoundsException e) {
            // Success
        }

        // No partial copy is expected.
        assertEquals(0, dst[0]);
        assertEquals(0, dst[1]);
        assertEquals(0, dst[2]);
        assertEquals(0, dst[3]);
    }

    @Test
    public void testGetByteArrayBoundaryCheckRangeOfWriteOutOfBounds() {
        ByteBuf netty = Unpooled.buffer(CAPACITY);
        ByteBufWrapper wrapper = new ByteBufWrapper(netty);

        byte[] dst = new byte[4];
        wrapper.setInt(0, 0x01020304);
        try {
            wrapper.getBytes(0, dst, 1, 4);
            fail("Should not allow get when range produces out of bounds write");
        } catch (IndexOutOfBoundsException e) {
            // Success
        }

        // No partial copy is expected.
        assertEquals(0, dst[0]);
        assertEquals(0, dst[1]);
        assertEquals(0, dst[2]);
        assertEquals(0, dst[3]);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetByteBufferBoundaryCheck() {
        ByteBuf netty = Unpooled.buffer(CAPACITY);
        ByteBufWrapper wrapper = new ByteBufWrapper(netty);
        wrapper.getBytes(-1, ByteBuffer.allocate(0));
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testCopyBoundaryCheck1() {
        ByteBuf netty = Unpooled.buffer(CAPACITY);
        ByteBufWrapper wrapper = new ByteBufWrapper(netty);
        wrapper.copy(-1, 0);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testCopyBoundaryCheck2() {
        ByteBuf netty = Unpooled.buffer(CAPACITY);
        ByteBufWrapper wrapper = new ByteBufWrapper(netty);
        wrapper.copy(0, wrapper.capacity() + 1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testCopyBoundaryCheck3() {
        ByteBuf netty = Unpooled.buffer(CAPACITY);
        ByteBufWrapper wrapper = new ByteBufWrapper(netty);
        wrapper.copy(wrapper.capacity() + 1, 0);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testCopyBoundaryCheck4() {
        ByteBuf netty = Unpooled.buffer(CAPACITY);
        ByteBufWrapper wrapper = new ByteBufWrapper(netty);
        wrapper.copy(wrapper.capacity(), 1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testSetIndexBoundaryCheck1() {
        ByteBuf netty = Unpooled.buffer(CAPACITY);
        ByteBufWrapper wrapper = new ByteBufWrapper(netty);
        wrapper.setIndex(-1, CAPACITY);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testSetIndexBoundaryCheck2() {
        ByteBuf netty = Unpooled.buffer(CAPACITY);
        ByteBufWrapper wrapper = new ByteBufWrapper(netty);
        wrapper.setIndex(CAPACITY / 2, CAPACITY / 4);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testSetIndexBoundaryCheck3() {
        ByteBuf netty = Unpooled.buffer(CAPACITY);
        ByteBufWrapper wrapper = new ByteBufWrapper(netty);
        wrapper.setIndex(0, CAPACITY + 1);
    }

    @Test
    public void testGetByteBufferStateAfterLimtedGet() {
        ByteBuffer dst = ByteBuffer.allocate(4);

        ByteBuf netty = Unpooled.buffer(CAPACITY);
        ByteBufWrapper wrapper = new ByteBufWrapper(netty);

        dst.position(1);
        dst.limit(3);

        wrapper.setByte(0, (byte) 1);
        wrapper.setByte(1, (byte) 2);
        wrapper.setByte(2, (byte) 3);
        wrapper.setByte(3, (byte) 4);
        wrapper.getBytes(1, dst);

        assertEquals(3, dst.position());
        assertEquals(3, dst.limit());

        dst.clear();
        assertEquals(0, dst.get(0));
        assertEquals(2, dst.get(1));
        assertEquals(3, dst.get(2));
        assertEquals(0, dst.get(3));
    }

    @Test
    public void testRandomByteAccess() {
        ByteBuf netty = Unpooled.buffer(CAPACITY);
        ByteBufWrapper wrapper = new ByteBufWrapper(netty);

        for (int i = 0; i < wrapper.capacity(); i ++) {
            byte value = (byte) random.nextInt();
            wrapper.setByte(i, value);
        }

        random.setSeed(seed);
        for (int i = 0; i < wrapper.capacity(); i ++) {
            byte value = (byte) random.nextInt();
            assertEquals(value, wrapper.getByte(i));
        }
    }

    @Test
    public void testRandomShortAccess() {
        ByteBuf netty = Unpooled.buffer(CAPACITY);
        ByteBufWrapper wrapper = new ByteBufWrapper(netty);

        for (int i = 0; i < wrapper.capacity() - 1; i += 2) {
            short value = (short) random.nextInt();
            wrapper.setShort(i, value);
        }

        random.setSeed(seed);
        for (int i = 0; i < wrapper.capacity() - 1; i += 2) {
            short value = (short) random.nextInt();
            assertEquals(value, wrapper.getShort(i));
        }
    }

    @Test
    public void testShortConsistentWithByteBuffer() {
        ByteBuf netty = Unpooled.buffer(CAPACITY);
        ByteBufWrapper wrapper = new ByteBufWrapper(netty);

        for (int i = 0; i < 64; ++i) {
            ByteBuffer javaBuffer = ByteBuffer.allocate(wrapper.capacity());

            short expected = (short) (random.nextInt() & 0xFFFF);
            javaBuffer.putShort(expected);

            final int bufferIndex = wrapper.capacity() - 2;
            wrapper.setShort(bufferIndex, expected);
            javaBuffer.flip();

            short javaActual = javaBuffer.getShort();
            assertEquals(expected, javaActual);
            assertEquals(javaActual, wrapper.getShort(bufferIndex));
        }
    }

    @Test
    public void testRandomIntAccess() {
        ByteBuf netty = Unpooled.buffer(CAPACITY);
        ByteBufWrapper wrapper = new ByteBufWrapper(netty);

        for (int i = 0; i < wrapper.capacity() - 3; i += 4) {
            int value = random.nextInt();
            wrapper.setInt(i, value);
        }

        random.setSeed(seed);
        for (int i = 0; i < wrapper.capacity() - 3; i += 4) {
            int value = random.nextInt();
            assertEquals(value, wrapper.getInt(i));
        }
    }

    @Test
    public void testIntConsistentWithByteBuffer() {
        ByteBuf netty = Unpooled.buffer(CAPACITY);
        ByteBufWrapper wrapper = new ByteBufWrapper(netty);

        for (int i = 0; i < 64; ++i) {
            ByteBuffer javaBuffer = ByteBuffer.allocate(wrapper.capacity());
            int expected = random.nextInt();
            javaBuffer.putInt(expected);

            final int bufferIndex = wrapper.capacity() - 4;
            wrapper.setInt(bufferIndex, expected);
            javaBuffer.flip();

            int javaActual = javaBuffer.getInt();
            assertEquals(expected, javaActual);
            assertEquals(javaActual, wrapper.getInt(bufferIndex));
        }
    }

    @Test
    public void testRandomLongAccess() {
        ByteBuf netty = Unpooled.buffer(CAPACITY);
        ByteBufWrapper wrapper = new ByteBufWrapper(netty);

        for (int i = 0; i < wrapper.capacity() - 7; i += 8) {
            long value = random.nextLong();
            wrapper.setLong(i, value);
        }

        random.setSeed(seed);
        for (int i = 0; i < wrapper.capacity() - 7; i += 8) {
            long value = random.nextLong();
            assertEquals(value, wrapper.getLong(i));
        }
    }

    @Test
    public void testLongConsistentWithByteBuffer() {
        ByteBuf netty = Unpooled.buffer(CAPACITY);
        ByteBufWrapper wrapper = new ByteBufWrapper(netty);

        for (int i = 0; i < 64; ++i) {
            ByteBuffer javaBuffer = ByteBuffer.allocate(wrapper.capacity());

            long expected = random.nextLong();
            javaBuffer.putLong(expected);

            final int bufferIndex = wrapper.capacity() - 8;
            wrapper.setLong(bufferIndex, expected);
            javaBuffer.flip();

            long javaActual = javaBuffer.getLong();
            assertEquals(expected, javaActual);
            assertEquals(javaActual, wrapper.getLong(bufferIndex));
        }
    }

    @Test
    public void testRandomFloatAccess() {
        ByteBuf netty = Unpooled.buffer(CAPACITY);
        ByteBufWrapper wrapper = new ByteBufWrapper(netty);

        for (int i = 0; i < wrapper.capacity() - 7; i += 8) {
            float value = random.nextFloat();
            wrapper.setFloat(i, value);
        }

        random.setSeed(seed);
        for (int i = 0; i < wrapper.capacity() - 7; i += 8) {
            float expected = random.nextFloat();
            float actual = wrapper.getFloat(i);
            assertEquals(expected, actual, 0.01);
        }
    }

    @Test
    public void testRandomDoubleAccess() {
        ByteBuf netty = Unpooled.buffer(CAPACITY);
        ByteBufWrapper wrapper = new ByteBufWrapper(netty);

        for (int i = 0; i < wrapper.capacity() - 7; i += 8) {
            double value = random.nextDouble();
            wrapper.setDouble(i, value);
        }

        random.setSeed(seed);
        for (int i = 0; i < wrapper.capacity() - 7; i += 8) {
            double expected = random.nextDouble();
            double actual = wrapper.getDouble(i);
            assertEquals(expected, actual, 0.01);
        }
    }

    @Test
    public void testSequentialByteAccess() {
        ByteBuf netty = Unpooled.buffer(CAPACITY);
        ByteBufWrapper buffer = new ByteBufWrapper(netty);

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
        ByteBuf netty = Unpooled.buffer(CAPACITY);
        ByteBufWrapper buffer = new ByteBufWrapper(netty);

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
        ByteBuf netty = Unpooled.buffer(CAPACITY);
        ByteBufWrapper buffer = new ByteBufWrapper(netty);

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
        ByteBuf netty = Unpooled.buffer(CAPACITY);
        ByteBufWrapper buffer = new ByteBufWrapper(netty);

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
        ByteBuf netty = Unpooled.buffer(CAPACITY);
        ByteBufWrapper buffer = new ByteBufWrapper(netty);

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
        ByteBuf netty = Unpooled.buffer(CAPACITY);
        ByteBufWrapper buffer = new ByteBufWrapper(netty);

        byte[] value = new byte[BLOCK_SIZE];
        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            random.nextBytes(value);
            buffer.setBytes(i, value);
        }

        random.setSeed(seed);
        byte[] expectedValueContent = new byte[BLOCK_SIZE];
        ByteBufWrapper expectedValue = new ByteBufWrapper(Unpooled.wrappedBuffer(expectedValueContent));
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
        ByteBuf netty = Unpooled.buffer(CAPACITY);
        ByteBufWrapper buffer = new ByteBufWrapper(netty);

        byte[] value = new byte[BLOCK_SIZE * 2];
        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            random.nextBytes(value);
            buffer.setBytes(i, value, random.nextInt(BLOCK_SIZE), BLOCK_SIZE);
        }

        random.setSeed(seed);
        byte[] expectedValueContent = new byte[BLOCK_SIZE * 2];
        ByteBufWrapper expectedValue = new ByteBufWrapper(Unpooled.wrappedBuffer(expectedValueContent));
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
        ByteBuf netty = Unpooled.buffer(CAPACITY);
        ByteBufWrapper buffer = new ByteBufWrapper(netty);

        byte[] valueContent = new byte[BLOCK_SIZE];
        ProtonBuffer value = new ProtonByteBuffer(valueContent);
        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            random.nextBytes(valueContent);
            value.setIndex(0, BLOCK_SIZE);
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
    public void testRandomHeapBufferTransfer2() {
        ByteBuf netty = Unpooled.buffer(CAPACITY);
        ByteBufWrapper buffer = new ByteBufWrapper(netty);

        byte[] valueContent = new byte[BLOCK_SIZE * 2];
        ProtonBuffer value = new ProtonByteBuffer(valueContent);
        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            random.nextBytes(valueContent);
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
                assertEquals(expectedValue.getByte(j), value.getByte(j));
            }
        }
    }
}
