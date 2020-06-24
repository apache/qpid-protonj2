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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonByteBuffer;
import org.apache.qpid.protonj2.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.protonj2.buffer.ProtonCompositeBuffer;
import org.apache.qpid.protonj2.buffer.ProtonNioByteBuffer;
import org.junit.Test;

/**
 * Test the Proton Composite Buffer class
 */
public class ProtonCompositeBufferTest extends ProtonAbstractBufferTest {

    @Test
    public void testCreateDefaultCompositeBuffer() {
        ProtonCompositeBuffer composite = new ProtonCompositeBuffer(Integer.MAX_VALUE);
        assertNotNull(composite);
        assertEquals(0, composite.capacity());
        assertEquals(Integer.MAX_VALUE, composite.maxCapacity());
    }

    //----- Read and Write Index handling tests

    @Test
    public void testManipulateReadIndexWithOneArrayAppended() {
        ProtonCompositeBuffer buffer = new ProtonCompositeBuffer(Integer.MAX_VALUE);

        buffer.append(ProtonByteBufferAllocator.DEFAULT.wrap(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 }));

        assertEquals(10, buffer.capacity());
        assertEquals(10, buffer.getWriteIndex());
        assertEquals(0, buffer.getReadIndex());

        buffer.setReadIndex(5);
        assertEquals(5, buffer.getReadIndex());

        buffer.setReadIndex(6);
        assertEquals(6, buffer.getReadIndex());

        buffer.setReadIndex(10);
        assertEquals(10, buffer.getReadIndex());

        try {
            buffer.setReadIndex(11);
            fail("Should throw a IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException e) {}

        buffer.markReadIndex();

        buffer.setReadIndex(0);
        assertEquals(0, buffer.getReadIndex());
    }

    @Test
    public void testPositionWithTwoArraysAppended() {
        ProtonCompositeBuffer buffer = new ProtonCompositeBuffer(Integer.MAX_VALUE);

        buffer.append(ProtonByteBufferAllocator.DEFAULT.wrap(new byte[] { 0, 1, 2, 3, 4 }))
              .append(ProtonByteBufferAllocator.DEFAULT.wrap(new byte[] { 5, 6, 7, 8, 9 }));

        assertEquals(10, buffer.capacity());
        assertEquals(10, buffer.getReadableBytes());

        buffer.setReadIndex(5);
        assertEquals(5, buffer.getReadIndex());

        buffer.setReadIndex(6);
        assertEquals(6, buffer.getReadIndex());

        buffer.setReadIndex(10);
        assertEquals(10, buffer.getReadIndex());

        try {
            buffer.setReadIndex(11);
            fail("Should throw a IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException e) {}

        buffer.setReadIndex(0);
        assertEquals(0, buffer.getReadIndex());
    }

    @Test
    public void testPositionEnforcesPreconditions() {
        ProtonCompositeBuffer buffer = new ProtonCompositeBuffer(Integer.MAX_VALUE);

        // test with nothing appended.
        try {
            buffer.setReadIndex(2);
            fail("Should throw a IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException e) {}

        try {
            buffer.setReadIndex(-1);
            fail("Should throw a IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException e) {}

        // Test with something appended
        buffer.append(ProtonByteBufferAllocator.DEFAULT.wrap(new byte[] { 127 }));

        try {
            buffer.setReadIndex(2);
            fail("Should throw a IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException e) {}

        try {
            buffer.setReadIndex(-1);
            fail("Should throw a IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException e) {}
    }

    //----- Test reading from composite of multiple buffers

    @Test
    public void testGetByteWithManyArraysWithOneElements() {
        ProtonCompositeBuffer buffer = new ProtonCompositeBuffer();

        buffer.append(new byte[] {0})
              .append(new byte[] {1})
              .append(new byte[] {2})
              .append(new byte[] {3})
              .append(new byte[] {4})
              .append(new byte[] {5})
              .append(new byte[] {6})
              .append(new byte[] {7})
              .append(new byte[] {8})
              .append(new byte[] {9});

        assertEquals(10, buffer.getReadableBytes());
        assertFalse(buffer.hasArray());
        assertTrue(buffer.isReadable());
        assertEquals(0, buffer.getReadIndex());

        for (int i = 0; i < 10; ++i) {
            assertEquals(i, buffer.readByte());
        }

        assertEquals(0, buffer.getReadableBytes());
        assertEquals(10, buffer.getReadIndex());
        assertEquals(10, buffer.getWriteIndex());

        try {
            buffer.readByte();
            fail("Should not be able to read past end");
        } catch (IndexOutOfBoundsException e) {}
    }

    @Test
    public void testGetByteWithManyArraysWithVariedElements() {
        ProtonCompositeBuffer buffer = new ProtonCompositeBuffer();

        buffer.append(new byte[] {0})
              .append(new byte[] {1, 2})
              .append(new byte[] {3, 4, 5})
              .append(new byte[] {6})
              .append(new byte[] {7, 8, 9});

        assertEquals(10, buffer.getReadableBytes());
        assertFalse(buffer.hasArray());
        assertTrue(buffer.isReadable());
        assertEquals(0, buffer.getReadIndex());
        assertEquals(10, buffer.getWriteIndex());

        for (int i = 0; i < 10; ++i) {
            assertEquals(i, buffer.readByte());
        }

        assertEquals(0, buffer.getReadableBytes());
        assertEquals(10, buffer.getReadIndex());

        try {
            buffer.readByte();
            fail("Should not be able to read past end");
        } catch (IndexOutOfBoundsException e) {}
    }

    @Test
    public void testGetShortByteWithNothingAppended() {
        ProtonCompositeBuffer buffer = new ProtonCompositeBuffer();

        try {
            buffer.readShort();
            fail("Should throw a IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException e) {}
    }

    @Test
    public void testGetShortWithTwoArraysContainingOneElement() {
        ProtonCompositeBuffer buffer = new ProtonCompositeBuffer();

        buffer.append(new byte[] {8}).append(new byte[] {0});

        assertEquals(2, buffer.getReadableBytes());
        assertTrue(buffer.isReadable());
        assertEquals(0, buffer.getReadIndex());

        assertEquals(2048, buffer.readShort());

        assertEquals(0, buffer.getReadableBytes());
        assertFalse(buffer.isReadable());
        assertEquals(2, buffer.getReadIndex());

        try {
            buffer.readShort();
            fail("Should not be able to read past end");
        } catch (IndexOutOfBoundsException e) {}
    }

    @Test
    public void testGetIntWithTwoArraysContainingOneElement() {
        ProtonCompositeBuffer buffer = new ProtonCompositeBuffer();

        buffer.append(new byte[] { 0 ,0 }).append(new byte[] { 8, 0 });

        assertEquals(4, buffer.getReadableBytes());
        assertTrue(buffer.isReadable());
        assertEquals(0, buffer.getReadIndex());

        assertEquals(2048, buffer.readInt());

        assertEquals(0, buffer.getReadableBytes());
        assertFalse(buffer.isReadable());
        assertEquals(4, buffer.getReadIndex());

        try {
            buffer.readInt();
            fail("Should not be able to read past end");
        } catch (IndexOutOfBoundsException e) {}
    }

    @Test
    public void testGetLongWithTwoArraysContainingOneElement() {
        ProtonCompositeBuffer buffer = new ProtonCompositeBuffer();

        buffer.append(new byte[] { 0 ,0, 0, 0 }).append(new byte[] { 0, 0, 8, 0 });

        assertEquals(8, buffer.getReadableBytes());
        assertTrue(buffer.isReadable());
        assertEquals(0, buffer.getReadIndex());

        assertEquals(2048, buffer.readLong());

        assertEquals(0, buffer.getReadableBytes());
        assertFalse(buffer.isReadable());
        assertEquals(8, buffer.getReadIndex());

        try {
            buffer.readLong();
            fail("Should not be able to read past end");
        } catch (IndexOutOfBoundsException e) {}
    }

    @Test
    public void testGetWritableBufferWithContentsInSeveralArrays() {
        ProtonCompositeBuffer buffer = new ProtonCompositeBuffer();

        byte[] data1 = new byte[] {0, 1, 2, 3, 4};
        byte[] data2 = new byte[] {5, 6, 7, 8, 9};
        byte[] data3 = new byte[] {10, 11, 12};

        int size = data1.length + data2.length + data3.length;

        buffer.append(data1).append(data2).append(data3);

        assertEquals(size, buffer.getWriteIndex());

        ProtonBuffer destination = ProtonByteBufferAllocator.DEFAULT.allocate(1, 1);

        for (int i = 0; i < size; i++) {
            assertEquals(buffer.getReadIndex(), i);
            ProtonBuffer self = buffer.readBytes(destination);
            assertEquals(destination.getByte(0), buffer.getByte(i));
            assertSame(self, buffer);
            destination.setWriteIndex(0);
        }

        try {
            buffer.readBytes(destination);
            fail("Should throw IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException e) {
        }

        try {
            buffer.readBytes((ProtonBuffer) null);
            fail("Should throw NullPointerException");
        } catch (NullPointerException e) {
        }
    }

    @Test
    public void testGetintWithContentsInMultipleArrays() {
        ProtonCompositeBuffer buffer = new ProtonCompositeBuffer();

        buffer.append(new byte[] {0, 1, 2, 3, 4}).append(new byte[] {5, 6, 7, 8, 9});

        for (int i = 0; i < buffer.capacity(); i++) {
            assertEquals(buffer.getReadIndex(), i);
            assertEquals(buffer.readByte(), buffer.getByte(i));
        }

        try {
            buffer.getByte(-1);
            fail("Should throw IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException e) {
        }

        try {
            buffer.getByte(buffer.getWriteIndex());
            fail("Should throw IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException e) {
        }
    }

    @Test
    public void testGetbyteArrayIntIntWithContentsInMultipleArrays() {
        ProtonCompositeBuffer buffer = new ProtonCompositeBuffer();

        byte[] data1 = new byte[] {0, 1, 2, 3, 4};
        byte[] data2 = new byte[] {5, 6, 7, 8, 9};

        final int dataLength = data1.length + data2.length;

        buffer.append(data1).append(data2);

        assertEquals(dataLength, buffer.getReadableBytes());

        byte array[] = new byte[buffer.getReadableBytes()];

        try {
            buffer.readBytes(new byte[dataLength + 1], 0, dataLength + 1);
            fail("Should throw IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException e) {
        }

        assertEquals(buffer.getReadIndex(), 0);

        try {
            buffer.readBytes(array, -1, array.length);
            fail("Should throw IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException e) {
        }

        buffer.readBytes(array, array.length, 0);

        try {
            buffer.readBytes(array, array.length + 1, 1);
            fail("Should throw IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException e) {
        }

        assertEquals(buffer.getReadIndex(), 0);

        try {
            buffer.readBytes(array, 2, -1);
            fail("Should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
        }

        try {
            buffer.readBytes(array, 2, array.length);
            fail("Should throw IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException e) {
        }

        try {
            buffer.readBytes((byte[])null, -1, 0);
            fail("Should throw NullPointerException");
        } catch (NullPointerException e) {
        }

        try {
            buffer.readBytes(array, 1, Integer.MAX_VALUE);
            fail("Should throw IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException e) {
        }

        try {
            buffer.readBytes(array, Integer.MAX_VALUE, 1);
            fail("Should throw IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException e) {
        }

        assertEquals(buffer.getReadIndex(), 0);

        ProtonBuffer self = buffer.readBytes(array, 0, array.length);
        assertEquals(buffer.getReadIndex(), buffer.capacity());
        assertContentEquals(buffer, array, 0, array.length);
        assertSame(self, buffer);
    }

    @Test
    public void testSetAndGetShortAcrossMultipleArrays() {
        ProtonCompositeBuffer buffer = new ProtonCompositeBuffer();

        final int NUM_ELEMENTS = 4;

        for (int i = 0; i < Short.BYTES * NUM_ELEMENTS; ++i) {
            buffer.append(new byte[] {0});
        }

        for (int i = 0, j = 1; i < buffer.getReadableBytes(); i += Short.BYTES, j++) {
            buffer.setShort(i, j);
        }

        assertEquals(Short.BYTES * NUM_ELEMENTS, buffer.getReadableBytes());
        assertFalse(buffer.hasArray());
        assertTrue(buffer.isReadable());
        assertEquals(0, buffer.getReadIndex());

        for (int i = 0, j = 1; i < buffer.getReadableBytes(); i += Short.BYTES, j++) {
            assertEquals(j, buffer.getShort(i));
        }

        assertEquals(Short.BYTES * NUM_ELEMENTS, buffer.getReadableBytes());
        assertEquals(0, buffer.getReadIndex());
        assertEquals(Short.BYTES * NUM_ELEMENTS, buffer.getReadableBytes());
    }

    @Test
    public void testSetAndGetIntegersAcrossMultipleArrays() {
        ProtonCompositeBuffer buffer = new ProtonCompositeBuffer();

        final int NUM_ELEMENTS = 4;

        for (int i = 0; i < Integer.BYTES * NUM_ELEMENTS; ++i) {
            buffer.append(new byte[] {0});
        }

        for (int i = 0, j = 1; i < buffer.getReadableBytes(); i += Integer.BYTES, j++) {
            buffer.setShort(i, j);
        }

        assertEquals(Integer.BYTES * NUM_ELEMENTS, buffer.getReadableBytes());
        assertFalse(buffer.hasArray());
        assertTrue(buffer.isReadable());
        assertEquals(0, buffer.getReadIndex());

        for (int i = 0, j = 1; i < buffer.getReadableBytes(); i += Integer.BYTES, j++) {
            assertEquals(j, buffer.getShort(i));
        }

        assertEquals(Integer.BYTES * NUM_ELEMENTS, buffer.getReadableBytes());
        assertEquals(0, buffer.getReadIndex());
        assertEquals(Integer.BYTES * NUM_ELEMENTS, buffer.getReadableBytes());
    }

    @Test
    public void testSetAndGetLongsAcrossMultipleArrays() {
        ProtonCompositeBuffer buffer = new ProtonCompositeBuffer();

        final int NUM_ELEMENTS = 4;

        for (int i = 0; i < Long.BYTES * NUM_ELEMENTS; ++i) {
            buffer.append(new byte[] {0});
        }

        for (int i = 0, j = 1; i < buffer.getReadableBytes(); i += Long.BYTES, j++) {
            buffer.setShort(i, j);
        }

        assertEquals(Long.BYTES * NUM_ELEMENTS, buffer.getReadableBytes());
        assertFalse(buffer.hasArray());
        assertTrue(buffer.isReadable());
        assertEquals(0, buffer.getReadIndex());

        for (int i = 0, j = 1; i < buffer.getReadableBytes(); i += Long.BYTES, j++) {
            assertEquals(j, buffer.getShort(i));
        }

        assertEquals(Long.BYTES * NUM_ELEMENTS, buffer.getReadableBytes());
        assertEquals(0, buffer.getReadIndex());
        assertEquals(Long.BYTES * NUM_ELEMENTS, buffer.getReadableBytes());
    }

    //----- Test array access method

    @Test
    public void testArrayWhenEmpty() {
        ProtonCompositeBuffer buffer = new ProtonCompositeBuffer();
        assertNotNull(buffer.getArray());
        assertSame(buffer.getArray(), buffer.getArray());

        byte[] data1 = new byte[] {0, 1, 2, 3, 4};

        buffer.append(data1, 1, data1.length - 1);
        assertEquals(1, buffer.getArrayOffset());

        assertEquals(data1, buffer.getArray());
    }

    @Test
    public void testArrayUnsupportedWhenCompositeHasMultipleChunks() {
        ProtonCompositeBuffer buffer = new ProtonCompositeBuffer();

        byte[] data1 = new byte[] {0, 1, 2, 3, 4};
        byte[] data2 = new byte[] {5, 6, 7, 8, 9};

        final int dataLength = data1.length + data2.length;

        buffer.append(data1).append(data2);
        assertEquals(dataLength, buffer.getReadableBytes());

        try {
            buffer.getArray();
            fail("Should not be able to get an array after more than one array added");
        } catch (UnsupportedOperationException uoe) {}
    }

    //----- Test arrayOffset method ------------------------------------------//

    @Test
    public void testArrayOffsetZeroWhenNoChunksInBuffer() {
        ProtonCompositeBuffer buffer = new ProtonCompositeBuffer();
        assertEquals(0, buffer.getArrayOffset());

        byte[] data1 = new byte[] {0, 1, 2, 3, 4};

        buffer.append(data1, 1, data1.length - 1);
        assertEquals(1, buffer.getArrayOffset());
    }

    @Test
    public void testArrayOffsetUnsupportedWhenCompositeHasMultipleChunks() {
        ProtonCompositeBuffer buffer = new ProtonCompositeBuffer();

        byte[] data1 = new byte[] {0, 1, 2, 3, 4};
        byte[] data2 = new byte[] {5, 6, 7, 8, 9};

        final int dataLength = data1.length + data2.length;

        buffer.append(data1).append(data2);
        assertEquals(dataLength, buffer.getReadableBytes());

        try {
            buffer.getArrayOffset();
            fail("Should not be able to get an offset after more than one array added");
        } catch (UnsupportedOperationException uoe) {}
    }

    @Test
    public void testArrayOffsetIsZeroRegardlessOfPositionOnNonSlicedBuffer() {
        ProtonCompositeBuffer buffer = new ProtonCompositeBuffer();

        byte[] data = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};

        buffer.append(data);

        assertTrue(buffer.hasArray());
        assertSame(data, buffer.getArray());
        assertEquals(0, buffer.getArrayOffset());

        buffer.setReadIndex(1);

        assertSame(data, buffer.getArray());
        assertEquals(0, buffer.getArrayOffset());

        buffer.setReadIndex(buffer.getReadableBytes());

        assertSame(data, buffer.getArray());
        assertEquals(0, buffer.getArrayOffset());

        buffer.setReadIndex(0);

        assertSame(data, buffer.getArray());
        assertEquals(0, buffer.getArrayOffset());
    }

    @Test
    public void testArrayOffsetIsFixedOnSliceRegardlessOfPosition() {
        ProtonCompositeBuffer buffer = new ProtonCompositeBuffer();

        byte[] data = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};

        buffer.append(data);

        assertTrue(buffer.hasArray());
        assertEquals(0, buffer.getArrayOffset());

        buffer.setReadIndex(1);
        ProtonBuffer slice = buffer.slice();

        assertEquals(1, slice.getArrayOffset());

        slice.setReadIndex(slice.getReadableBytes());

        assertEquals(1, slice.getArrayOffset());

        slice.setReadIndex(0);

        assertEquals(1, slice.getArrayOffset());

        slice.setReadIndex(1);

        ProtonBuffer anotherSlice = slice.slice();

        assertEquals(2, anotherSlice.getArrayOffset());
    }

    @Test
    public void testArrayOffset() {
        ProtonCompositeBuffer buffer = new ProtonCompositeBuffer();
        assertTrue(buffer.hasArray());
        assertEquals(0, buffer.getArrayOffset());
        assertNotNull(buffer.getArray());
        assertEquals(0, buffer.getArray().length);

        buffer.append(new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});

        assertTrue(buffer.hasArray());
        assertEquals("Unexpected array offset", 0, buffer.getArrayOffset());
    }

    @Test
    public void testArrayOffsetAfterDuplicate() {
        ProtonCompositeBuffer buffer = new ProtonCompositeBuffer();
        buffer.append(new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});

        assertEquals("Unexpected get result", 0, buffer.readByte());

        ProtonBuffer duplicate = buffer.duplicate();

        assertTrue(duplicate.hasArray());
        assertEquals("Unexpected array offset after duplication", 0, duplicate.getArrayOffset());

        assertEquals("Unexpected get result", 1, duplicate.readByte());

        assertEquals("Unexpected array offset after duplicate use", 0, duplicate.getArrayOffset());
        assertEquals("Unexpected get result", 2, duplicate.readByte());

        assertEquals("Unexpected array offset on original", 0, buffer.getArrayOffset());
    }

    @Test
    public void testArrayOffsetAfterSliceDuplicated() {
        ProtonCompositeBuffer buffer = new ProtonCompositeBuffer();
        buffer.append(new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});

        assertEquals("Unexpected get result", 0, buffer.readByte());

        ProtonBuffer slice = buffer.slice();
        ProtonBuffer sliceDuplicated = slice.duplicate();

        assertTrue(sliceDuplicated.hasArray());
        assertEquals("Unexpected array offset after duplication", 0, sliceDuplicated.getArrayOffset());

        assertEquals("Unexpected get result", 1, sliceDuplicated.readByte());

        assertEquals("Unexpected array offset after duplicate use", 0, sliceDuplicated.getArrayOffset());
        assertEquals("Unexpected get result", 2, sliceDuplicated.readByte());

        assertEquals("Unexpected array offset on original", 0, buffer.getArrayOffset());
        assertEquals("Unexpected array offset on slice", 1, slice.getArrayOffset());
    }

    //----- Test appending data to the buffer --------------------------------//

    @Test
    public void testAppendToBufferAtEndOfContentArray() {
        ProtonCompositeBuffer buffer = new ProtonCompositeBuffer();

        byte[] source1 = new byte[] { 0, 1, 2, 3 };

        assertTrue(buffer.hasArray());
        assertEquals(0, buffer.numberOfBuffers());

        buffer.append(source1);

        assertTrue(buffer.hasArray());
        assertEquals(1, buffer.numberOfBuffers());

        buffer.setReadIndex(source1.length);

        assertFalse(buffer.isReadable());
        assertEquals(0, buffer.getReadableBytes());

        byte[] source2 = new byte[] { 4, 5, 6, 7 };
        buffer.append(source2);

        assertTrue(buffer.isReadable());
        assertEquals(source2.length, buffer.getReadableBytes());
        assertFalse(buffer.hasArray());
        assertEquals(2, buffer.numberOfBuffers());
        assertEquals(source1.length, buffer.getReadIndex());

        // Check each position in the array is read
        for(int i = 0; i < source2.length; i++) {
            assertEquals(source1.length + i, buffer.readByte());
        }
    }

    @Test
    public void testAppendToBufferAtEndOfContentList() {
        ProtonCompositeBuffer buffer = new ProtonCompositeBuffer();

        byte[] source1 = new byte[] { 0, 1, 2, 3 };
        byte[] source2 = new byte[] { 4, 5, 6, 7 };

        buffer.append(source1);
        buffer.append(source2);

        assertFalse(buffer.hasArray());
        assertEquals(2, buffer.numberOfBuffers());

        buffer.setReadIndex(source1.length + source2.length);

        assertFalse(buffer.isReadable());
        assertEquals(0, buffer.getReadableBytes());

        byte[] source3 = new byte[] { 8, 9, 10, 11 };
        buffer.append(source3);

        assertTrue(buffer.isReadable());
        assertEquals(source3.length, buffer.getReadableBytes());
        assertFalse(buffer.hasArray());
        assertEquals(3, buffer.numberOfBuffers());
        assertEquals(source1.length + source2.length, buffer.getReadIndex());

        // Check each position in the array is read
        for(int i = 0; i < source3.length; i++) {
            assertEquals(source1.length + source2.length + i, buffer.readByte());
        }
    }

    @Test
    public void testAppendToBufferAtWhenWriteIndexNotAtEnd() {
        ProtonCompositeBuffer buffer = new ProtonCompositeBuffer();

        byte[] source1 = new byte[] { 0, 1, 2, 3 };
        byte[] source2 = new byte[] { 4, 5, 6, 7 };

        buffer.append(source1);

        assertEquals(source1.length, buffer.getWriteIndex());

        buffer.append(source2);

        assertEquals(source2.length + source1.length, buffer.getWriteIndex());

        byte[] source3 = new byte[] { 8, 9, 10, 11 };

        buffer.setWriteIndex(2);

        buffer.append(source3);

        assertEquals(2, buffer.getWriteIndex());
        assertFalse(buffer.hasArray());
        assertEquals(3, buffer.numberOfBuffers());
    }

    @Test
    public void testAppendNullByteArray() {
        ProtonCompositeBuffer buffer = new ProtonCompositeBuffer();

        try {
            buffer.append((byte[]) null);
            fail("Should not be able to add a null array");
        } catch (NullPointerException npe) {}
    }

    @Test
    public void testAppendNullByteArrayWithArgs() {
        ProtonCompositeBuffer buffer = new ProtonCompositeBuffer();

        try {
            buffer.append((byte[]) null, 0, 0);
            fail("Should not be able to add a null array");
        } catch (NullPointerException npe) {}
    }

    @Test
    public void testAppendNullReadableBuffer() {
        ProtonCompositeBuffer buffer = new ProtonCompositeBuffer();

        try {
            buffer.append((ProtonBuffer) null);
            fail("Should not be able to add a null array");
        } catch (NullPointerException npe) {}
    }

    @Test
    public void testAppendEmptyByteArray() {
        ProtonCompositeBuffer buffer = new ProtonCompositeBuffer();

        buffer.append(new byte[0]);

        assertFalse(buffer.isReadable());
        assertTrue(buffer.hasArray());
        assertEquals(0, buffer.numberOfBuffers());
    }

    //----- Test various cases of Duplicate ----------------------------------//

    @Test
    public void testDuplicateOnEmptyBuffer() {
        ProtonCompositeBuffer buffer = new ProtonCompositeBuffer();
        ProtonBuffer dup = buffer.duplicate();

        assertNotSame(buffer, dup);
        assertEquals(0, dup.capacity());
        assertEquals(0, buffer.capacity());
        assertEquals(0, dup.getReadIndex());
        assertEquals(0, buffer.getReadIndex());
        assertEquals(0, dup.getWriteIndex());
        assertEquals(0, buffer.getWriteIndex());
        assertContentEquals(buffer, dup);

        // TODO - Compact
//        try {
//            dup.reclaimRead();
//        } catch (Throwable t) {
//            fail("Compacting an empty duplicate should not fail");
//        }
    }

    @Test
    public void testDuplicateWithSingleArrayContent() {
        ProtonCompositeBuffer buffer = new ProtonCompositeBuffer();

        buffer.append(new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
        buffer.markReadIndex();
        buffer.setReadIndex(buffer.getWriteIndex());

        // duplicate's contents should be the same as buffer
        ProtonBuffer duplicate = buffer.duplicate();
        assertNotSame(buffer, duplicate);
        assertEquals(buffer.capacity(), duplicate.capacity());
        assertEquals(buffer.getReadIndex(), duplicate.getReadIndex());
        assertEquals(buffer.getWriteIndex(), duplicate.getWriteIndex());
        assertContentEquals(buffer, duplicate);

        // duplicate's read index, mark, and write index should be independent to buffer
        duplicate.resetReadIndex();
        assertEquals(duplicate.getReadIndex(), duplicate.getWriteIndex());
        duplicate.clear();
        assertEquals(buffer.getReadIndex(), buffer.getWriteIndex());
        buffer.resetReadIndex();
        assertEquals(buffer.getReadIndex(), 0);

        // One array buffer should share backing array
        assertTrue(buffer.hasArray());
        assertTrue(duplicate.hasArray());
        assertSame(buffer.getArray(), duplicate.getArray());
    }

    @Test
    public void testDuplicateWithSingleArrayContentCompactionIsNoOpWhenNotRead() {
        ProtonCompositeBuffer buffer = new ProtonCompositeBuffer();
        buffer.append(new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});

        ProtonBuffer duplicate = buffer.duplicate();

        assertEquals(10, buffer.capacity());
        assertEquals(buffer.capacity(), duplicate.capacity());

        // TODO - Compact
//        buffer.reclaimRead();
//        assertEquals(10, buffer.capacity());
//        assertEquals(buffer.capacity(), duplicate.capacity());
//
//        duplicate.reclaimRead();
//        assertEquals(10, buffer.capacity());
//        assertEquals(buffer.capacity(), duplicate.capacity());
    }

    @Test
    public void testAppendedBufferCannotForceMaxCapacityExceeded() {
        ProtonCompositeBuffer buffer = new ProtonCompositeBuffer(6);

        byte[] source1 = new byte[] { 0, 1, 2, 3 };
        byte[] source2 = new byte[] { 4, 5, 6, 7 };

        buffer.append(source1);
        assertEquals(source1.length, buffer.capacity());

        try {
            buffer.append(source2);
            fail("Should not be able to exceed max capacity limit.");
        } catch (IndexOutOfBoundsException iae) {
        }

        assertEquals(source1.length, buffer.capacity());
    }

    //----- Tests for hashCode -----------------------------------------------//

    @Test
    public void testHashCodeNotFromIdentity() throws CharacterCodingException {
        ProtonCompositeBuffer buffer = new ProtonCompositeBuffer();

        assertEquals(1, buffer.hashCode());

        byte[] data = new byte[] {9, 8, 7, 6, 5, 4, 3, 2, 1, 0};

        buffer.append(data);

        assertTrue(buffer.hashCode() != 1);
        assertNotEquals(buffer.hashCode(), System.identityHashCode(buffer));
        assertEquals(buffer.hashCode(), buffer.hashCode());
    }

    @Test
    public void testHashCodeOnSameBackingBuffer() throws CharacterCodingException {
        ProtonCompositeBuffer buffer1 = new ProtonCompositeBuffer();
        ProtonCompositeBuffer buffer2 = new ProtonCompositeBuffer();
        ProtonCompositeBuffer buffer3 = new ProtonCompositeBuffer();

        byte[] data = new byte[] {9, 8, 7, 6, 5, 4, 3, 2, 1, 0};

        buffer1.append(data);
        buffer2.append(data);
        buffer3.append(data);

        assertEquals(buffer1.hashCode(), buffer2.hashCode());
        assertEquals(buffer2.hashCode(), buffer3.hashCode());
        assertEquals(buffer3.hashCode(), buffer1.hashCode());
    }

    @Test
    public void testHashCodeOnDifferentBackingBuffer() throws CharacterCodingException {
        ProtonCompositeBuffer buffer1 = new ProtonCompositeBuffer();
        ProtonCompositeBuffer buffer2 = new ProtonCompositeBuffer();

        byte[] data1 = new byte[] {9, 8, 7, 6, 5, 4, 3, 2, 1, 0};
        byte[] data2 = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};

        buffer1.append(data1);
        buffer2.append(data2);

        assertNotEquals(buffer1.hashCode(), buffer2.hashCode());
    }

    @Test
    public void testHashCodeOnSplitBufferContentsNotSame() throws CharacterCodingException {
        ProtonCompositeBuffer buffer1 = new ProtonCompositeBuffer();
        ProtonCompositeBuffer buffer2 = new ProtonCompositeBuffer();

        byte[] data1 = new byte[] {9, 8, 7, 6, 5, 4, 3, 2, 1, 0};
        byte[] data2 = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};

        buffer1.append(data1).append(data2);
        buffer2.append(data2).append(data1);

        assertNotEquals(buffer1.hashCode(), buffer2.hashCode());
    }

    @Test
    public void testHashCodeOnSplitBufferContentsSame() throws CharacterCodingException {
        ProtonCompositeBuffer buffer1 = new ProtonCompositeBuffer();
        ProtonCompositeBuffer buffer2 = new ProtonCompositeBuffer();

        byte[] data1 = new byte[] {9, 8, 7, 6, 5, 4, 3, 2, 1, 0};
        byte[] data2 = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};

        buffer1.append(data1).append(data2);
        buffer2.append(data1).append(data2);

        assertEquals(buffer1.hashCode(), buffer2.hashCode());
    }

    @Test
    public void testHashCodeMatchesByteBufferWhenLimitSetGivesNoRemaining() throws CharacterCodingException {
        byte[] data = new byte[] {9, 8, 7, 6, 5, 4, 3, 2, 1, 0};

        ProtonCompositeBuffer buffer1 = new ProtonCompositeBuffer();
        buffer1.append(data);
        buffer1.setReadIndex(buffer1.getWriteIndex());

        ProtonBuffer buffer2 = ProtonByteBufferAllocator.DEFAULT.wrap(data);
        buffer2.setReadIndex(buffer1.getWriteIndex());

        assertEquals(buffer1.hashCode(), buffer2.hashCode());
    }

    @Test
    public void testHashCodeMatchesByteBufferSingleArrayContents() throws CharacterCodingException {
        byte[] data = new byte[] {9, 8, 7, 6, 5, 4, 3, 2, 1, 0};

        ProtonCompositeBuffer buffer1 = new ProtonCompositeBuffer();
        buffer1.append(data);

        ProtonBuffer buffer2 = ProtonByteBufferAllocator.DEFAULT.wrap(data);

        assertEquals(buffer1.hashCode(), buffer2.hashCode());
    }

    @Test
    public void testHashCodeMatchesByteBufferSingleArrayContentsWithSlice() throws CharacterCodingException {
        byte[] data = new byte[] {9, 8, 7, 6, 5, 4, 3, 2, 1, 0};

        ProtonCompositeBuffer buffer1 = new ProtonCompositeBuffer();
        buffer1.append(data);

        ProtonBuffer buffer2 = ProtonByteBufferAllocator.DEFAULT.wrap(data);

        ProtonBuffer slice1 = buffer1.setReadIndex(1).slice();
        ProtonBuffer slice2 = buffer2.setReadIndex(1).slice();

        assertEquals(slice1.hashCode(), slice2.hashCode());
    }

    @Test
    public void testHashCodeMatchesByteBufferMultipleArrayContents() throws CharacterCodingException {
        byte[] data = new byte[] {9, 8, 7, 6, 5, 4, 3, 2, 1, 0};

        byte[] data1 = new byte[] {9, 8, 7, 6, 5};
        byte[] data2 = new byte[] {4, 3, 2, 1, 0};

        ProtonCompositeBuffer buffer1 = new ProtonCompositeBuffer();
        buffer1.append(data1);
        buffer1.append(data2);

        ProtonBuffer buffer2 = ProtonByteBufferAllocator.DEFAULT.wrap(data);

        assertEquals(buffer1.hashCode(), buffer2.hashCode());
    }

    @Test
    public void testHashCodeMatchesByteBufferMultipleArrayContentsWithSlice() throws CharacterCodingException {
        byte[] data = new byte[] {9, 8, 7, 6, 5, 4, 3, 2, 1, 0};

        byte[] data1 = new byte[] {9, 8, 7, 6, 5};
        byte[] data2 = new byte[] {4, 3, 2, 1, 0};

        ProtonCompositeBuffer buffer1 = new ProtonCompositeBuffer();
        buffer1.append(data1);
        buffer1.append(data2);

        ProtonBuffer buffer2 = ProtonByteBufferAllocator.DEFAULT.wrap(data);

        ProtonBuffer slice1 = buffer1.setReadIndex(1).setWriteIndex(4).slice();
        ProtonBuffer slice2 = buffer2.setReadIndex(1).setWriteIndex(4).slice();

        assertEquals(slice1.hashCode(), slice2.hashCode());
    }

    @Test
    public void testHashCodeMatchesByteBufferMultipleArrayContentsWithRangeOfLimits() throws CharacterCodingException {
        byte[] data = new byte[] {10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0};

        byte[] data1 = new byte[] {10, 9};
        byte[] data2 = new byte[] {8, 7};
        byte[] data3 = new byte[] {6, 5, 4};
        byte[] data4 = new byte[] {3};
        byte[] data5 = new byte[] {2, 1, 0};

        ProtonCompositeBuffer buffer1 = new ProtonCompositeBuffer();
        buffer1.append(data1).append(data2).append(data3).append(data4).append(data5);

        ProtonBuffer buffer2 = ProtonByteBufferAllocator.DEFAULT.wrap(data);

        for (int i = 0; i < data.length; ++i) {
            buffer1.setWriteIndex(i);
            buffer2.setWriteIndex(i);

            assertEquals(buffer1.hashCode(), buffer2.hashCode());
        }
    }

    //----- Tests for equals -------------------------------------------------//

    @Test
    public void testEqualsOnSameBackingBuffer() throws CharacterCodingException {
        ProtonCompositeBuffer buffer1 = new ProtonCompositeBuffer();
        ProtonCompositeBuffer buffer2 = new ProtonCompositeBuffer();
        ProtonCompositeBuffer buffer3 = new ProtonCompositeBuffer();

        byte[] data = new byte[] {9, 8, 7, 6, 5, 4, 3, 2, 1, 0};

        buffer1.append(data);
        buffer2.append(data);
        buffer3.append(data);

        assertEquals(buffer1, buffer2);
        assertEquals(buffer2, buffer3);
        assertEquals(buffer3, buffer1);

        assertEquals(0, buffer1.getReadIndex());
        assertEquals(0, buffer2.getReadIndex());
        assertEquals(0, buffer3.getReadIndex());
    }

    @Test
    public void testEqualsOnDifferentBackingBuffer() throws CharacterCodingException {
        ProtonCompositeBuffer buffer1 = new ProtonCompositeBuffer();
        ProtonCompositeBuffer buffer2 = new ProtonCompositeBuffer();

        byte[] data1 = new byte[] {9, 8, 7, 6, 5, 4, 3, 2, 1, 0};
        byte[] data2 = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};

        buffer1.append(data1);
        buffer2.append(data2);

        assertNotEquals(buffer1, buffer2);

        assertEquals(0, buffer1.getReadIndex());
        assertEquals(0, buffer2.getReadIndex());
    }

    @Test
    public void testEqualsWhenContentsInMultipleArraysNotSame() throws CharacterCodingException {
        ProtonCompositeBuffer buffer1 = new ProtonCompositeBuffer();
        ProtonCompositeBuffer buffer2 = new ProtonCompositeBuffer();

        byte[] data1 = new byte[] {9, 8, 7, 6, 5, 4, 3, 2, 1, 0};
        byte[] data2 = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};

        buffer1.append(data1).append(data2);
        buffer2.append(data2).append(data1);

        assertNotEquals(buffer1, buffer2);

        assertEquals(0, buffer1.getReadIndex());
        assertEquals(0, buffer2.getReadIndex());
    }

    @Test
    public void testEqualsWhenContentsInMultipleArraysSame() throws CharacterCodingException {
        ProtonCompositeBuffer buffer1 = new ProtonCompositeBuffer();
        ProtonCompositeBuffer buffer2 = new ProtonCompositeBuffer();

        byte[] data1 = new byte[] {9, 8, 7, 6, 5, 4, 3, 2, 1, 0};
        byte[] data2 = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};

        buffer1.append(data1).append(data2);
        buffer2.append(data1).append(data2);

        assertEquals(buffer1, buffer2);

        assertEquals(0, buffer1.getReadIndex());
        assertEquals(0, buffer2.getReadIndex());
    }

    @Test
    public void testEqualsWhenContentRemainingWithDifferentStartPositionsSame() throws CharacterCodingException {
        doEqualsWhenContentRemainingWithDifferentStartPositionsSameTestImpl(false);
    }

    @Test
    public void testEqualsWhenContentRemainingWithDifferentStartPositionsSameMultipleArrays() throws CharacterCodingException {
        doEqualsWhenContentRemainingWithDifferentStartPositionsSameTestImpl(true);
    }

    private void doEqualsWhenContentRemainingWithDifferentStartPositionsSameTestImpl(boolean multipleArrays) {
        ProtonCompositeBuffer buffer1 = new ProtonCompositeBuffer();
        ProtonCompositeBuffer buffer2 = new ProtonCompositeBuffer();

        byte[] data1 = new byte[] {-1, -1, 0, 1, 2, 3, 4, 5};
        byte[] data2 = new byte[] {-1, -1, -1, 0, 1, 2, 3, 4, 5};

        buffer1.append(data1);
        buffer1.setReadIndex(2);
        buffer1.markWriteIndex();

        // Offset wrapped buffer should behave same as buffer 1
        buffer2.append(data2, 1, data1.length);
        buffer2.setReadIndex(2);
        buffer2.markWriteIndex();

        if (multipleArrays) {
            byte[] data3 = new byte[] { 5, 4, 3, 2, 1 };
            buffer1.append(data3).resetWriteIndex();
            buffer2.append(data3).resetWriteIndex();
        }

        assertEquals(buffer1, buffer2);

        assertEquals(2, buffer1.getReadIndex());
        assertEquals(2, buffer2.getReadIndex());
    }

    @Test
    public void testEqualsWhenContentRemainingWithDifferentStartPositionsNotSame() throws CharacterCodingException {
        doEqualsWhenContentRemainingWithDifferentStartPositionsNotSameTestImpl(false);
    }

    @Test
    public void testEqualsWhenContentRemainingWithDifferentStartPositionsNotSameMultipleArrays() throws CharacterCodingException {
        doEqualsWhenContentRemainingWithDifferentStartPositionsNotSameTestImpl(true);
    }

    private void doEqualsWhenContentRemainingWithDifferentStartPositionsNotSameTestImpl(boolean multipleArrays) {
        ProtonCompositeBuffer buffer1 = new ProtonCompositeBuffer();
        ProtonCompositeBuffer buffer2 = new ProtonCompositeBuffer();

        byte[] data1 = new byte[] {-1, -1, 0, 1, 2, 3, 4, 5};
        byte[] data2 = new byte[] {-1, -1, -1, 0, 1, 2, 3, 4, -1};

        buffer1.append(data1);
        buffer1.setReadIndex(2);

        buffer2.append(data2);
        buffer2.setReadIndex(3);

        if (multipleArrays) {
            byte[] data3 = new byte[] { 5, 4, 3, 2, 1 };
            buffer1.append(data3);
            buffer2.append(data3);
        }

        assertNotEquals(buffer1, buffer2);

        assertEquals(2, buffer1.getReadIndex());
        assertEquals(3, buffer2.getReadIndex());
    }

    @Test
    public void testEqualsWhenContentRemainingWithDifferentlyPositionedSlicesSame() throws CharacterCodingException {
        doEqualsWhenContentRemainingWithDifferentlyPositionedSlicesSameTestImpl(false);
    }

    @Test
    public void testEqualsWhenContentRemainingWithDifferentlyPositionedSlicesSameMultipleArrays() throws CharacterCodingException {
        doEqualsWhenContentRemainingWithDifferentlyPositionedSlicesSameTestImpl(true);
    }

    private void doEqualsWhenContentRemainingWithDifferentlyPositionedSlicesSameTestImpl(boolean multipleArrays) {
        ProtonCompositeBuffer buffer1 = new ProtonCompositeBuffer();
        ProtonCompositeBuffer buffer2 = new ProtonCompositeBuffer();

        byte[] data1 = new byte[] {-1, -1, 0, 1, 2, 3, 4, 5};
        byte[] data2 = new byte[] {-1, -1, -1, 0, 1, 2, 3, 4, 5};

        buffer1.append(data1);
        buffer1.setReadIndex(2);

        buffer2.append(data2);
        buffer2.setReadIndex(3);

        if (multipleArrays) {
            byte[] data3 = new byte[] { 5, 4, 3, 2, 1 };
            buffer1.append(data3);
            buffer2.append(data3);
        }

        ProtonBuffer slicedBuffer1 = buffer1.slice();
        ProtonBuffer slicedBuffer2 = buffer2.slice();

        assertEquals(slicedBuffer1, slicedBuffer2);
        assertEquals(slicedBuffer2, slicedBuffer1);

        assertEquals(0, slicedBuffer1.getReadIndex());
        assertEquals(0, slicedBuffer2.getReadIndex());
    }

    @Test
    public void testEqualsWhenContentRemainingWithDifferentlyPositionedSlicesNotSame() throws CharacterCodingException {
        doEqualsWhenContentRemainingWithDifferentlyPositionedSlicesNotSameTestImpl(false);
    }

    @Test
    public void testEqualsWhenContentRemainingWithDifferentlyPositionedSlicesNotSameMultipleArrays() throws CharacterCodingException {
        doEqualsWhenContentRemainingWithDifferentlyPositionedSlicesNotSameTestImpl(true);
    }

    private void doEqualsWhenContentRemainingWithDifferentlyPositionedSlicesNotSameTestImpl(boolean multipleArrays) {
        ProtonCompositeBuffer buffer1 = new ProtonCompositeBuffer();
        ProtonCompositeBuffer buffer2 = new ProtonCompositeBuffer();

        byte[] data1 = new byte[] {-1, -1, 0, 1, 2, 3, 4, 5};
        byte[] data2 = new byte[] {-1, -1, -1, 0, 1, 2, 3, 4, -1};

        buffer1.append(data1);
        buffer1.setReadIndex(2);

        buffer2.append(data2);
        buffer2.setReadIndex(3);

        if (multipleArrays) {
            byte[] data3 = new byte[] { 5, 4, 3, 2, 1 };
            buffer1.append(data3);
            buffer2.append(data3);
        }

        ProtonBuffer slicedBuffer1 = buffer1.slice();
        ProtonBuffer slicedBuffer2 = buffer2.slice();

        assertNotEquals(slicedBuffer1, slicedBuffer2);
        assertNotEquals(slicedBuffer2, slicedBuffer1);

        assertEquals(0, slicedBuffer1.getReadIndex());
        assertEquals(0, slicedBuffer2.getReadIndex());
    }

    @Test
    public void testEqualsWhenContentRemainingIsSubsetOfSingleChunkInMultiArrayBufferSame() {
        ProtonCompositeBuffer buffer1 = new ProtonCompositeBuffer();
        ProtonCompositeBuffer buffer2 = new ProtonCompositeBuffer();

        byte[] data1 = new byte[] {-1, -1, 0, 1, 2, 3, 4, 5};
        byte[] data2 = new byte[] {-1, -1, -1, 0, 1, 2, 3, 4, 5};

        buffer1.append(data1);
        buffer1.setReadIndex(2);

        // Offset the wrapped buffer which means these two should behave the same
        buffer2.append(data2, 1, data2.length - 1);
        buffer2.setReadIndex(2);

        byte[] data3 = new byte[] { 5, 4, 3, 2, 1 };
        buffer1.append(data3);
        buffer2.append(data3);

        buffer1.setWriteIndex(data1.length);
        buffer2.setWriteIndex(data1.length);

        assertEquals(6, buffer1.getReadableBytes());
        assertEquals(6, buffer2.getReadableBytes());

        assertEquals(buffer1, buffer2);
        assertEquals(buffer2, buffer1);

        assertEquals(2, buffer1.getReadIndex());
        assertEquals(2, buffer2.getReadIndex());
    }

    @Test
    public void testEqualsWhenContentRemainingIsSubsetOfSingleChunkInMultiArrayBufferNotSame() {
        ProtonCompositeBuffer buffer1 = new ProtonCompositeBuffer();
        ProtonCompositeBuffer buffer2 = new ProtonCompositeBuffer();

        byte[] data1 = new byte[] {-1, -1, 0, 1, 2, 3, 4, 5};
        byte[] data2 = new byte[] {-1, -1, -1, 0, 1, 2, 3, 4, -1};

        buffer1.append(data1);
        buffer1.setReadIndex(2);

        buffer2.append(data2);
        buffer2.setReadIndex(3);

        byte[] data3 = new byte[] { 5, 4, 3, 2, 1 };
        buffer1.append(data3);
        buffer2.append(data3);

        buffer1.setWriteIndex(data1.length);
        buffer2.setWriteIndex(data2.length);

        assertEquals(6, buffer1.getReadableBytes());
        assertEquals(6, buffer2.getReadableBytes());

        assertNotEquals(buffer1, buffer2);
        assertNotEquals(buffer2, buffer1);

        assertEquals(2, buffer1.getReadIndex());
        assertEquals(3, buffer2.getReadIndex());
    }

    //----- Test toByteBuffer implementation for Composites

    @Test
    public void testToByteBufferWhenEmpty() {
        ProtonCompositeBuffer buffer = new ProtonCompositeBuffer();
        assertNotNull(buffer.toByteBuffer());
        assertSame(buffer.toByteBuffer(), buffer.toByteBuffer());
        assertEquals(0, buffer.toByteBuffer().capacity());
    }

    @Test
    public void testToByteBufferAcrossArrays() {
        ProtonCompositeBuffer buffer = new ProtonCompositeBuffer();

        buffer.append(new byte[] {0})
              .append(new byte[] {1, 2})
              .append(new byte[] {3, 4, 5})
              .append(new byte[] {6})
              .append(new byte[] {7, 8, 9});

        assertEquals(10, buffer.getReadableBytes());
        assertFalse(buffer.hasArray());
        assertTrue(buffer.isReadable());
        assertEquals(0, buffer.getReadIndex());
        assertEquals(10, buffer.getWriteIndex());

        ByteBuffer nioBuffer = buffer.toByteBuffer(0, 1);
        assertNotNull(nioBuffer);
        assertEquals(1,  nioBuffer.capacity());
        assertEquals(0, nioBuffer.get(0));

        nioBuffer = buffer.toByteBuffer(5, 5);
        assertNotNull(nioBuffer);
        assertEquals(5,  nioBuffer.capacity());
        assertEquals(5, nioBuffer.get(0));
        assertEquals(6, nioBuffer.get(1));
        assertEquals(7, nioBuffer.get(2));
        assertEquals(8, nioBuffer.get(3));
        assertEquals(9, nioBuffer.get(4));
    }

    //----- Tests for altering capacity of composite buffer instances

    @Test
    public void testReduceCapacityAndReadSequentialShortValues() throws CharacterCodingException {
        byte[] data1 = new byte[] {0, 1, 0, 2, 0, 3, 0, 4};
        byte[] data2 = new byte[] {0, 5, 0, 6, 0, 7, 0, 8};
        byte[] data3 = new byte[] {0, 9, 0, 10, 0, 11, 0, 12};

        ProtonCompositeBuffer buffer = new ProtonCompositeBuffer();
        buffer.append(data1).append(data2).append(data3);

        final int initialNumShorts = buffer.capacity() / 2;

        for (int i = 0; i < initialNumShorts; ++i) {
            assertEquals(i + 1, buffer.readShort());
        }

        buffer.setReadIndex(0);
        buffer.capacity(buffer.capacity() / 2);

        final int newNumShorts = buffer.capacity() / 2;
        assertEquals(initialNumShorts / 2, newNumShorts);

        for (int i = 0; i < newNumShorts; ++i) {
            assertEquals(i + 1, buffer.readShort());
        }
    }

    @Test
    public void testReduceCapacityToZero() throws CharacterCodingException {
        byte[] data1 = new byte[] {0, 1, 0, 2, 0, 3, 0, 4};
        byte[] data2 = new byte[] {0, 5, 0, 6, 0, 7, 0, 8};
        byte[] data3 = new byte[] {0, 9, 0, 10, 0, 11, 0, 12, 0, 13};

        ProtonCompositeBuffer buffer = new ProtonCompositeBuffer();
        buffer.append(data1).append(data2);

        assertFalse(buffer.hasArray());
        assertEquals(data1.length + data2.length, buffer.capacity());

        buffer.capacity(0);

        buffer.append(data3);

        assertEquals(data3.length, buffer.capacity());
        assertTrue(buffer.hasArray());
    }

    //----- Test Access to composite buffers when they are offset

    @Test
    public void testCompositeWithOffsetBuffersReadsSequentialShorts() throws CharacterCodingException {
        byte[] data1 = new byte[] {1, 1, 0, 0, 0, 1, 0, 2};
        byte[] data2 = new byte[] {0, 3, 0, 4, 0, 5, 1, 1};
        byte[] data3 = new byte[] {1, 1, 1, 1, 0, 6, 0, 7};

        ProtonBuffer offset1 = new ProtonByteBuffer(data1).skipBytes(2);
        ProtonBuffer offset2 = new ProtonByteBuffer(data2).setWriteIndex(data2.length - 2);
        ProtonBuffer offset3 = new ProtonByteBuffer(data3).skipBytes(4);

        ProtonCompositeBuffer buffer = new ProtonCompositeBuffer();
        buffer.append(offset1).append(offset2).append(offset3);

        assertEquals(data1.length + data2.length + data3.length, buffer.capacity() + 8);

        final int initialNumShorts = buffer.capacity() / 2;

        for (int i = 0; i < initialNumShorts; ++i) {
            assertEquals(i, buffer.readShort());
        }
    }

    @Test
    public void testByteArrayTransferWithOffsetComposites() {
        testByteArrayTransfer(false);
    }

    @Test
    public void testByteArrayTransferDirectBackedBufferOfOffsetComposites() {
        assumeTrue(canAllocateDirectBackedBuffers());
        testByteArrayTransfer(true);
    }

    private void testByteArrayTransfer(boolean direct) {
        final ProtonBuffer buffer;

        if (direct) {
            buffer = allocateDirectBufferOfOffsetComposites(LARGE_CAPACITY);
        } else {
            buffer = allocateBufferOfOffsetComposites(LARGE_CAPACITY);
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

    //----- Test buffer walking for each methods

    @Test
    public void testForeachBufferReturnsDuplicates() {
        ProtonBuffer buffer1 = ProtonByteBufferAllocator.DEFAULT.wrap(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 });
        ProtonBuffer buffer2 = ProtonByteBufferAllocator.DEFAULT.wrap(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 });

        ProtonCompositeBuffer composite = new ProtonCompositeBuffer();

        composite.append(buffer1);
        composite.append(buffer2);

        assertEquals(2, composite.numberOfBuffers());

        final AtomicInteger walked = new AtomicInteger();

        composite.foreachBuffer(buffer -> {
            walked.incrementAndGet();

            if (buffer == buffer1 || buffer == buffer2) {
                throw new AssertionError("Buffer returned should not be any of the source buffers.");
            }
        });

        assertEquals(2, walked.get());
    }

    @Test
    public void testForeachInternalBufferReturnsDuplicates() {
        ProtonBuffer buffer1 = ProtonByteBufferAllocator.DEFAULT.wrap(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 });
        ProtonBuffer buffer2 = ProtonByteBufferAllocator.DEFAULT.wrap(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 });

        ProtonCompositeBuffer composite = new ProtonCompositeBuffer();

        composite.append(buffer1);
        composite.append(buffer2);

        assertEquals(2, composite.numberOfBuffers());

        final AtomicInteger walked = new AtomicInteger();

        composite.foreachInternalBuffer(buffer -> {
            walked.incrementAndGet();

            if (buffer != buffer1 && buffer != buffer2) {
                throw new AssertionError("Buffer returned should be one of the source buffers.");
            }
        });

        assertEquals(2, walked.get());
    }

    //----- Implement abstract methods from the abstract buffer test base class

    @Override
    protected boolean canAllocateDirectBackedBuffers() {
        return true;
    }

    @Override
    protected ProtonBuffer allocateBuffer(int initialCapacity) {
        return new ProtonCompositeBuffer(Integer.MAX_VALUE).capacity(initialCapacity);
    }

    @Override
    protected ProtonBuffer allocateDirectBuffer(int initialCapacity) {
        return new ProtonCompositeBuffer(Integer.MAX_VALUE).append(
            new ProtonNioByteBuffer(ByteBuffer.allocateDirect(initialCapacity), 0));
    }

    @Override
    protected ProtonBuffer allocateBuffer(int initialCapacity, int maxCapacity) {
        return new ProtonCompositeBuffer(maxCapacity).capacity(initialCapacity);
    }

    @Override
    protected ProtonBuffer allocateDirectBuffer(int initialCapacity, int maxCapacity) {
        return new ProtonCompositeBuffer(maxCapacity).append(
            new ProtonNioByteBuffer(ByteBuffer.allocateDirect(initialCapacity), 0));
    }

    @Override
    protected ProtonBuffer wrapBuffer(byte[] array) {
        ProtonCompositeBuffer composite = new ProtonCompositeBuffer(Integer.MAX_VALUE);
        return composite.append(ProtonByteBufferAllocator.DEFAULT.wrap(array));
    }

    // TODO - Once abstract buffer test base doesn't assume the buffer under test has a backing array
    //        we should create a variant of this test that always creates a composite that is made up
    //        of offset buffers and buffers where the write index doesn't touch the end.

    private ProtonBuffer allocateBufferOfOffsetComposites(int capacity) {
        ProtonBuffer buffer1 = new ProtonNioByteBuffer(ByteBuffer.allocate((capacity / 2) + 10)).skipBytes(10);
        ProtonBuffer buffer2 = new ProtonNioByteBuffer(ByteBuffer.allocate((capacity / 2) + 10)).skipBytes(10);

        return new ProtonCompositeBuffer().append(buffer1).append(buffer2).setWriteIndex(0);
    }

    private ProtonBuffer allocateDirectBufferOfOffsetComposites(int capacity) {
        ProtonBuffer buffer1 = new ProtonNioByteBuffer(ByteBuffer.allocateDirect((capacity / 2) + 10)).skipBytes(10);
        ProtonBuffer buffer2 = new ProtonNioByteBuffer(ByteBuffer.allocateDirect((capacity / 2) + 10)).skipBytes(10);

        return new ProtonCompositeBuffer().append(buffer1).append(buffer2).setWriteIndex(0);
    }

    //----- Test Support Methods

    private void assertContentEquals(ProtonBuffer source, ProtonBuffer other) {
        assertEquals(source.capacity(), other.capacity());
        for (int i = 0; i < source.capacity(); i++) {
            assertEquals(source.getByte(i), other.getByte(i));
        }
    }

    private void assertContentEquals(ProtonBuffer buffer, byte array[], int offset, int length) {
        for (int i = 0; i < length; i++) {
            assertEquals(buffer.getByte(i), array[offset + i]);
        }
    }
}
