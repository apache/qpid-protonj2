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
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.nio.ByteBuffer;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonByteBuffer;
import org.apache.qpid.protonj2.buffer.ProtonNettyByteBuffer;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * Test the buffer wrapper around Netty ByteBuf instances
 */
public class ProtonNettyByteBufferTest extends ProtonAbstractBufferTest {

    private static final int CAPACITY = 4096; // Must be even
    private static final int BLOCK_SIZE = 128;

    public static final byte[] EMPTY_BYTES = {};

    @Test
    public void testUnwrap() {
        ByteBuf buffer = Unpooled.buffer(CAPACITY);
        ProtonNettyByteBuffer wrapper = new ProtonNettyByteBuffer(buffer);

        assertSame(buffer, wrapper.unwrap());
        ProtonBuffer duplicate = wrapper.duplicate();
        assertTrue(duplicate instanceof ProtonNettyByteBuffer);
        assertNotSame(((ProtonNettyByteBuffer) duplicate).unwrap(), buffer);
    }

    @Test
    public void testReaderIndexBoundaryCheck4() {
        ByteBuf buffer = Unpooled.buffer(CAPACITY);
        ProtonNettyByteBuffer wrapper = new ProtonNettyByteBuffer(buffer);

        wrapper.setWriteIndex(0);
        wrapper.setReadIndex(0);
        wrapper.setWriteIndex(buffer.capacity());
        wrapper.setReadIndex(buffer.capacity());
    }

    @Test
    public void testCreateWrapper() {
        ByteBuf buffer = Unpooled.buffer();
        ProtonNettyByteBuffer wrapper = new ProtonNettyByteBuffer(buffer);

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

        ProtonNettyByteBuffer wrapper = new ProtonNettyByteBuffer(buffer);

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

        ProtonNettyByteBuffer wrapper = new ProtonNettyByteBuffer(buffer);

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

        ProtonNettyByteBuffer wrapper = new ProtonNettyByteBuffer(buffer);

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

        ProtonNettyByteBuffer wrapper = new ProtonNettyByteBuffer(buffer);

        assertEquals(buffer.capacity(), wrapper.capacity());
        assertEquals(buffer.readableBytes(), wrapper.getReadableBytes());
        assertEquals(buffer.writableBytes(), wrapper.getWritableBytes());
        assertEquals(buffer.readerIndex(), wrapper.getReadIndex());
        assertEquals(buffer.writerIndex(), wrapper.getWriteIndex());

        for (int i = 0; i < 256; ++i) {
            assertEquals(i, wrapper.readLong());
        }
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testSetReadIndexBoundaryCheckForNegative() {
        ByteBuf buffer = Unpooled.buffer(CAPACITY);
        ProtonNettyByteBuffer wrapper = new ProtonNettyByteBuffer(buffer);

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
        ProtonNettyByteBuffer wrapper = new ProtonNettyByteBuffer(buffer);

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
        ProtonNettyByteBuffer wrapper = new ProtonNettyByteBuffer(buffer);

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
        ProtonNettyByteBuffer wrapper = new ProtonNettyByteBuffer(buffer);

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
        ProtonNettyByteBuffer wrapper = new ProtonNettyByteBuffer(buffer);

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
        ProtonNettyByteBuffer wrapper = new ProtonNettyByteBuffer(buffer);

        wrapper.setWriteIndex(0);
        wrapper.setReadIndex(0);
        wrapper.setWriteIndex(CAPACITY);

        wrapper.writeBytes(ByteBuffer.wrap(EMPTY_BYTES));
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetBooleanBoundaryCheck1() {
        ByteBuf buffer = Unpooled.buffer(CAPACITY);
        ProtonNettyByteBuffer wrapper = new ProtonNettyByteBuffer(buffer);
        wrapper.getBoolean(-1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetBooleanBoundaryCheck2() {
        ByteBuf buffer = Unpooled.buffer(CAPACITY);
        ProtonNettyByteBuffer wrapper = new ProtonNettyByteBuffer(buffer);
        wrapper.getBoolean(wrapper.capacity());
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetByteBoundaryCheck1() {
        ByteBuf buffer = Unpooled.buffer(CAPACITY);
        ProtonNettyByteBuffer wrapper = new ProtonNettyByteBuffer(buffer);
        wrapper.getByte(-1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetByteBoundaryCheck2() {
        ByteBuf buffer = Unpooled.buffer(CAPACITY);
        ProtonNettyByteBuffer wrapper = new ProtonNettyByteBuffer(buffer);
        wrapper.getByte(wrapper.capacity());
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetShortBoundaryCheck1() {
        ByteBuf buffer = Unpooled.buffer(CAPACITY);
        ProtonNettyByteBuffer wrapper = new ProtonNettyByteBuffer(buffer);
        wrapper.getShort(-1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetShortBoundaryCheck2() {
        ByteBuf buffer = Unpooled.buffer(CAPACITY);
        ProtonNettyByteBuffer wrapper = new ProtonNettyByteBuffer(buffer);
        wrapper.getShort(wrapper.capacity() - 1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetIntBoundaryCheck1() {
        ByteBuf buffer = Unpooled.buffer(CAPACITY);
        ProtonNettyByteBuffer wrapper = new ProtonNettyByteBuffer(buffer);
        wrapper.getInt(-1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetIntBoundaryCheck2() {
        ByteBuf buffer = Unpooled.buffer(CAPACITY);
        ProtonNettyByteBuffer wrapper = new ProtonNettyByteBuffer(buffer);
        wrapper.getInt(wrapper.capacity() - 3);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetLongBoundaryCheck1() {
        ByteBuf buffer = Unpooled.buffer(CAPACITY);
        ProtonNettyByteBuffer wrapper = new ProtonNettyByteBuffer(buffer);
        wrapper.getLong(-1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetLongBoundaryCheck2() {
        ByteBuf buffer = Unpooled.buffer(CAPACITY);
        ProtonNettyByteBuffer wrapper = new ProtonNettyByteBuffer(buffer);
        wrapper.getLong(wrapper.capacity() - 7);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetByteArrayBoundaryCheck1() {
        ByteBuf buffer = Unpooled.buffer(CAPACITY);
        ProtonNettyByteBuffer wrapper = new ProtonNettyByteBuffer(buffer);
        wrapper.getBytes(-1, EMPTY_BYTES);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetByteArrayBoundaryCheck2() {
        ByteBuf buffer = Unpooled.buffer(CAPACITY);
        ProtonNettyByteBuffer wrapper = new ProtonNettyByteBuffer(buffer);
        wrapper.getBytes(-1, EMPTY_BYTES, 0, 0);
    }

    @Test
    public void testGetByteArrayBoundaryCheckWithNegativeOffset() {
        ByteBuf netty = Unpooled.buffer(CAPACITY);
        ProtonNettyByteBuffer wrapper = new ProtonNettyByteBuffer(netty);

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
        ProtonNettyByteBuffer wrapper = new ProtonNettyByteBuffer(netty);

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
        ProtonNettyByteBuffer wrapper = new ProtonNettyByteBuffer(netty);
        wrapper.getBytes(-1, ByteBuffer.allocate(0));
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testCopyBoundaryCheck1() {
        ByteBuf netty = Unpooled.buffer(CAPACITY);
        ProtonNettyByteBuffer wrapper = new ProtonNettyByteBuffer(netty);
        wrapper.copy(-1, 0);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testCopyBoundaryCheck2() {
        ByteBuf netty = Unpooled.buffer(CAPACITY);
        ProtonNettyByteBuffer wrapper = new ProtonNettyByteBuffer(netty);
        wrapper.copy(0, wrapper.capacity() + 1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testCopyBoundaryCheck3() {
        ByteBuf netty = Unpooled.buffer(CAPACITY);
        ProtonNettyByteBuffer wrapper = new ProtonNettyByteBuffer(netty);
        wrapper.copy(wrapper.capacity() + 1, 0);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testCopyBoundaryCheck4() {
        ByteBuf netty = Unpooled.buffer(CAPACITY);
        ProtonNettyByteBuffer wrapper = new ProtonNettyByteBuffer(netty);
        wrapper.copy(wrapper.capacity(), 1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testSetIndexBoundaryCheck1() {
        ByteBuf netty = Unpooled.buffer(CAPACITY);
        ProtonNettyByteBuffer wrapper = new ProtonNettyByteBuffer(netty);
        wrapper.setIndex(-1, CAPACITY);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testSetIndexBoundaryCheck2() {
        ByteBuf netty = Unpooled.buffer(CAPACITY);
        ProtonNettyByteBuffer wrapper = new ProtonNettyByteBuffer(netty);
        wrapper.setIndex(CAPACITY / 2, CAPACITY / 4);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testSetIndexBoundaryCheck3() {
        ByteBuf netty = Unpooled.buffer(CAPACITY);
        ProtonNettyByteBuffer wrapper = new ProtonNettyByteBuffer(netty);
        wrapper.setIndex(0, CAPACITY + 1);
    }

    @Test
    public void testGetByteBufferStateAfterLimtedGet() {
        ByteBuffer dst = ByteBuffer.allocate(4);

        ByteBuf netty = Unpooled.buffer(CAPACITY);
        ProtonNettyByteBuffer wrapper = new ProtonNettyByteBuffer(netty);

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
    public void testRandomProtonBufferTransfer3() {
        doTestRandomProtonBufferTransfer3(false);
    }

    @Test
    public void testRandomProtonBufferTransfer3DirectBackedBuffer() {
        doTestRandomProtonBufferTransfer3(true);
    }

    private void doTestRandomProtonBufferTransfer3(boolean direct) {
        final ProtonBuffer buffer;
        if (direct) {
            buffer = allocateDirectBuffer(LARGE_CAPACITY);
        } else {
            buffer = allocateBuffer(LARGE_CAPACITY);
        }

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

    @Test
    public void testRandomProtonBufferTransfer4() {
        doTestRandomProtonBufferTransfer4(false);
    }

    @Test
    public void testRandomProtonBufferTransfer4DirectBackedBuffer() {
        doTestRandomProtonBufferTransfer4(true);
    }

    private void doTestRandomProtonBufferTransfer4(boolean direct) {
        final ProtonBuffer buffer;
        if (direct) {
            buffer = allocateDirectBuffer(LARGE_CAPACITY);
        } else {
            buffer = allocateBuffer(LARGE_CAPACITY);
        }

        byte[] valueContent = new byte[BLOCK_SIZE * 2];
        ProtonNettyByteBuffer value = new ProtonNettyByteBuffer(Unpooled.wrappedBuffer(valueContent));
        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            random.nextBytes(valueContent);
            buffer.setBytes(i, value, random.nextInt(BLOCK_SIZE), BLOCK_SIZE);
        }

        random.setSeed(seed);
        byte[] expectedValueContent = new byte[BLOCK_SIZE * 2];
        ProtonNettyByteBuffer expectedValue = new ProtonNettyByteBuffer(Unpooled.wrappedBuffer(expectedValueContent));
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
    public void testRandomProtonNettyBufferTransfer2() {
        doTestRandomProtonBufferTransfer2(false, false);
    }

    @Test
    public void testRandomProtonNettyBufferTransfer2DirectSource() {
        doTestRandomProtonBufferTransfer2(true, false);
    }

    @Test
    public void testRandomProtonNettyBufferTransfer2DirectTarget() {
        assumeTrue(canAllocateDirectBackedBuffers());
        doTestRandomProtonBufferTransfer2(false, true);
    }

    @Test
    public void testRandomProtonNettyBufferTransfer2DirectSourceAndTarget() {
        assumeTrue(canAllocateDirectBackedBuffers());
        doTestRandomProtonBufferTransfer2(true, true);
    }

    /*
     * Tests getBytes with netty wrapper to netty wrapper with direct and non-direct variants
     */
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
            value = allocateDirectBuffer(SIZE);
        } else {
            value = allocateBuffer(SIZE);
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
    public void testSkipBytes1() {
        ByteBuf netty = Unpooled.buffer(CAPACITY);
        ProtonNettyByteBuffer buffer = new ProtonNettyByteBuffer(netty);

        buffer.setIndex(CAPACITY / 4, CAPACITY / 2);

        buffer.skipBytes(CAPACITY / 4);
        assertEquals(CAPACITY / 4 * 2, buffer.getReadIndex());

        try {
            buffer.skipBytes(CAPACITY / 4 + 1);
            fail();
        } catch (IndexOutOfBoundsException e) {
            // Expected
        }

        // Should remain unchanged.
        assertEquals(CAPACITY / 4 * 2, buffer.getReadIndex());
    }

    //----- Test API implemented for the abstract base class tests

    @Override
    protected boolean canAllocateDirectBackedBuffers() {
        return true;
    }

    @Override
    protected ProtonBuffer allocateBuffer(int initialCapacity) {
        return new ProtonNettyByteBuffer(Unpooled.buffer(initialCapacity));
    }

    @Override
    protected ProtonBuffer allocateDirectBuffer(int initialCapacity) {
        return new ProtonNettyByteBuffer(Unpooled.directBuffer(initialCapacity));
    }

    @Override
    protected ProtonBuffer allocateBuffer(int initialCapacity, int maxCapacity) {
        return new ProtonNettyByteBuffer(Unpooled.buffer(initialCapacity, maxCapacity));
    }

    @Override
    protected ProtonBuffer allocateDirectBuffer(int initialCapacity, int maxCapacity) {
        return new ProtonNettyByteBuffer(Unpooled.directBuffer(initialCapacity, maxCapacity));
    }

    @Override
    protected ProtonBuffer wrapBuffer(byte[] array) {
        return new ProtonNettyByteBuffer(Unpooled.wrappedBuffer(array));
    }
}
