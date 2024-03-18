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

package org.apache.qpid.protonj2.buffer.impl;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

import org.apache.qpid.protonj2.buffer.ProtonAbstractBufferTest;
import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferAllocator;
import org.apache.qpid.protonj2.buffer.ProtonBufferClosedException;
import org.apache.qpid.protonj2.buffer.ProtonBufferComponent;
import org.apache.qpid.protonj2.buffer.ProtonBufferComponentAccessor;
import org.apache.qpid.protonj2.buffer.ProtonBufferReadOnlyException;
import org.apache.qpid.protonj2.buffer.ProtonCompositeBuffer;
import org.apache.qpid.protonj2.buffer.netty.Netty5ProtonBufferAllocator;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import io.netty5.buffer.BufferAllocator;

/**
 * Tests for the proton composite buffer implementation.
 */
public class ProtonCompositeBufferImplTest extends ProtonAbstractBufferTest {

    @Override
    public ProtonBufferAllocator createTestCaseAllocator() {
        return new ProtonCompositeBufferAlloactor();
    }

    @Test
    public void testAppendReadableBufferOntoCompositeWithCapacityHigherThanReadableBytesWhenBothAreReadOnly() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator();
             ProtonCompositeBuffer composite = allocator.composite().ensureWritable(255);
             ProtonBuffer buffer = createProtonDefaultAllocator().allocate(8).implicitGrowthLimit(8)) {

            composite.writeLong(0x0102030405060708L);
            composite.convertToReadOnly();
            assertEquals(255, composite.capacity());

            buffer.writeLong(0x0102030405060708L);
            buffer.convertToReadOnly();
            assertEquals(8, buffer.capacity());

            composite.append(buffer);

            assertEquals(16, composite.capacity());
            assertEquals(16, composite.getWriteOffset());
            assertEquals(0, composite.getReadOffset());
            assertEquals(16, composite.getReadableBytes());

            byte[] copyOfComposite = new byte[16];

            composite.readBytes(copyOfComposite, 0, copyOfComposite.length);
        }
    }

    @Test
    public void testManipulateReadIndexWithOneArrayAppended() {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
             ProtonCompositeBuffer buffer = allocator.composite()) {

            assertEquals(0, buffer.componentCount());
            assertEquals(0, buffer.readableComponentCount());
            assertEquals(0, buffer.writableComponentCount());

            buffer.append(allocator.copy(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 }));

            assertEquals(1, buffer.componentCount());
            assertEquals(1, buffer.readableComponentCount());
            assertEquals(0, buffer.writableComponentCount());

            assertEquals(10, buffer.capacity());
            assertEquals(10, buffer.getWriteOffset());
            assertEquals(0, buffer.getReadOffset());

            buffer.setReadOffset(5);
            assertEquals(5, buffer.getReadOffset());

            assertEquals(1, buffer.componentCount());
            assertEquals(1, buffer.readableComponentCount());
            assertEquals(0, buffer.writableComponentCount());

            buffer.setReadOffset(6);
            assertEquals(6, buffer.getReadOffset());

            buffer.setReadOffset(10);
            assertEquals(10, buffer.getReadOffset());

            assertEquals(1, buffer.componentCount());
            assertEquals(0, buffer.readableComponentCount());
            assertEquals(0, buffer.writableComponentCount());

            try {
                buffer.setReadOffset(11);
                fail("Should throw a IndexOutOfBoundsException");
            } catch (IndexOutOfBoundsException e) {}

            buffer.setReadOffset(0);
            assertEquals(0, buffer.getReadOffset());

            buffer.setWriteOffset(0);
            assertEquals(10, buffer.getWritableBytes());

            assertEquals(1, buffer.componentCount());
            assertEquals(0, buffer.readableComponentCount());
            assertEquals(1, buffer.writableComponentCount());
        }
    }

    @Test
    public void testPositionWithTwoArraysAppended() {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
             ProtonCompositeBuffer buffer = allocator.composite()) {

            buffer.append(allocator.copy(new byte[] { 0, 1, 2, 3, 4 }))
                  .append(allocator.copy(new byte[] { 5, 6, 7, 8, 9 }));

            assertEquals(2, buffer.componentCount());
            assertEquals(2, buffer.readableComponentCount());
            assertEquals(0, buffer.writableComponentCount());

            assertEquals(10, buffer.capacity());
            assertEquals(10, buffer.getReadableBytes());

            buffer.setReadOffset(5);
            assertEquals(5, buffer.getReadOffset());

            buffer.setReadOffset(6);
            assertEquals(6, buffer.getReadOffset());

            buffer.setReadOffset(10);
            assertEquals(10, buffer.getReadOffset());

            try {
                buffer.setReadOffset(11);
                fail("Should throw a IndexOutOfBoundsException");
            } catch (IndexOutOfBoundsException e) {}

            buffer.setReadOffset(0);
            assertEquals(0, buffer.getReadOffset());
        }
    }

    @Test
    public void testGetByteWithManyArraysWithOneElements() {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
             ProtonCompositeBuffer buffer = allocator.composite()) {

            buffer.append(allocator.copy(new byte[] {0}))
                  .append(allocator.copy(new byte[] {1}))
                  .append(allocator.copy(new byte[] {2}))
                  .append(allocator.copy(new byte[] {3}))
                  .append(allocator.copy(new byte[] {4}))
                  .append(allocator.copy(new byte[] {5}))
                  .append(allocator.copy(new byte[] {6}))
                  .append(allocator.copy(new byte[] {7}))
                  .append(allocator.copy(new byte[] {8}))
                  .append(allocator.copy(new byte[] {9}));

            assertEquals(10, buffer.getReadableBytes());
            assertTrue(buffer.isReadable());
            assertEquals(0, buffer.getReadOffset());

            for (int i = 0; i < 10; ++i) {
                assertEquals(i, buffer.readByte());
            }

            assertEquals(0, buffer.getReadableBytes());
            assertEquals(10, buffer.getReadOffset());
            assertEquals(10, buffer.getWriteOffset());

            try {
                buffer.readByte();
                fail("Should not be able to read past end");
            } catch (IndexOutOfBoundsException e) {}
        }
    }

    @Test
    public void testGetByteWithManyArraysWithVariedElements() {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
             ProtonCompositeBuffer buffer = allocator.composite()) {

            buffer.append(allocator.copy(new byte[] {0}))
                  .append(allocator.copy(new byte[] {1, 2}))
                  .append(allocator.copy(new byte[] {3, 4, 5}))
                  .append(allocator.copy(new byte[] {6}))
                  .append(allocator.copy(new byte[] {7, 8, 9}));

            assertEquals(10, buffer.getReadableBytes());
            assertTrue(buffer.isReadable());
            assertEquals(0, buffer.getReadOffset());
            assertEquals(10, buffer.getWriteOffset());

            for (int i = 0; i < 10; ++i) {
                assertEquals(i, buffer.readByte());
            }

            assertEquals(0, buffer.getReadableBytes());
            assertEquals(10, buffer.getReadOffset());

            try {
                buffer.readByte();
                fail("Should not be able to read past end");
            } catch (IndexOutOfBoundsException e) {}
        }
    }

    @Test
    public void testGetShortWithTwoArraysContainingOneElement() {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
             ProtonCompositeBuffer buffer = allocator.composite()) {

            buffer.append(allocator.copy(new byte[] {8}))
                  .append(allocator.copy(new byte[] {0}));

            assertEquals(2, buffer.getReadableBytes());
            assertTrue(buffer.isReadable());
            assertEquals(0, buffer.getReadOffset());

            assertEquals(2048, buffer.readShort());

            assertEquals(0, buffer.getReadableBytes());
            assertFalse(buffer.isReadable());
            assertEquals(2, buffer.getReadOffset());

            try {
                buffer.readShort();
                fail("Should not be able to read past end");
            } catch (IndexOutOfBoundsException e) {}
        }
    }

    @Test
    public void testGetIntWithTwoArraysContainingOneElement() {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
             ProtonCompositeBuffer buffer = allocator.composite()) {

            buffer.append(allocator.copy(new byte[] { 0 ,0 }))
                  .append(allocator.copy(new byte[] { 8, 0 }));

            assertEquals(4, buffer.getReadableBytes());
            assertTrue(buffer.isReadable());
            assertEquals(0, buffer.getReadOffset());

            assertEquals(2048, buffer.readInt());

            assertEquals(0, buffer.getReadableBytes());
            assertFalse(buffer.isReadable());
            assertEquals(4, buffer.getReadOffset());

            try {
                buffer.readInt();
                fail("Should not be able to read past end");
            } catch (IndexOutOfBoundsException e) {}
        }
    }

    @Test
    public void testGetLongWithTwoArraysContainingOneElement() {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
             ProtonCompositeBuffer buffer = allocator.composite()) {

            buffer.append(allocator.copy(new byte[] { 0 ,0, 0, 0 }))
                  .append(allocator.copy(new byte[] { 0, 0, 8, 0 }));

            assertEquals(8, buffer.getReadableBytes());
            assertTrue(buffer.isReadable());
            assertEquals(0, buffer.getReadOffset());

            assertEquals(2048, buffer.readLong());

            assertEquals(0, buffer.getReadableBytes());
            assertFalse(buffer.isReadable());
            assertEquals(8, buffer.getReadOffset());

            try {
                buffer.readLong();
                fail("Should not be able to read past end");
            } catch (IndexOutOfBoundsException e) {}
        }
    }

    @Test
    public void testGetbyteArrayIntIntWithContentsInMultipleArrays() {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
             ProtonCompositeBuffer buffer = allocator.composite()) {

            byte[] data1 = new byte[] {0, 1, 2, 3, 4};
            byte[] data2 = new byte[] {5, 6, 7, 8, 9};

            final int dataLength = data1.length + data2.length;

            buffer.append(allocator.copy(data1)).append(allocator.copy(data2));

            assertEquals(dataLength, buffer.getReadableBytes());

            byte array[] = new byte[buffer.getReadableBytes()];

            try {
                buffer.readBytes(new byte[dataLength + 1], 0, dataLength + 1);
                fail("Should throw IndexOutOfBoundsException");
            } catch (IndexOutOfBoundsException e) {
            }

            assertEquals(buffer.getReadOffset(), 0);

            try {
                buffer.readBytes(array, -1, array.length);
                fail("Should throw IllegalArgumentException");
            } catch (IllegalArgumentException e) {
            }

            buffer.readBytes(array, array.length, 0);

            try {
                buffer.readBytes(array, array.length + 1, 1);
                fail("Should throw IndexOutOfBoundsException");
            } catch (IndexOutOfBoundsException e) {
            }

            assertEquals(buffer.getReadOffset(), 0);

            try {
                buffer.readBytes(array, 2, -1);
                fail("Should throw IndexOutOfBoundsException");
            } catch (IndexOutOfBoundsException e) {
            }

            try {
                buffer.readBytes(array, 2, array.length);
                fail("Should throw IndexOutOfBoundsException");
            } catch (IndexOutOfBoundsException e) {
            }

            try {
                buffer.readBytes((byte[])null, -1, 0);
                fail("Should throw IllegalArgumentException");
            } catch (IllegalArgumentException e) {
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

            assertEquals(buffer.getReadOffset(), 0);

            ProtonBuffer self = buffer.readBytes(array, 0, array.length);
            assertEquals(buffer.getReadOffset(), buffer.capacity());
            assertContentEquals(buffer, array, 0, array.length);
            assertSame(self, buffer);
        }
    }

    @Test
    public void testSetAndGetShortAcrossMultipleArrays() {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
             ProtonCompositeBuffer buffer = allocator.composite()) {

            final int NUM_ELEMENTS = 4;

            for (short i = 0; i < Short.BYTES * NUM_ELEMENTS; ++i) {
                buffer.append(allocator.copy(new byte[] {0}));
            }

            for (short i = 0, j = 1; i < buffer.getReadableBytes(); i += Short.BYTES, j++) {
                buffer.setShort(i, j);
            }

            assertEquals(Short.BYTES * NUM_ELEMENTS, buffer.getReadableBytes());
            assertTrue(buffer.isReadable());
            assertEquals(0, buffer.getReadOffset());

            for (int i = 0, j = 1; i < buffer.getReadableBytes(); i += Short.BYTES, j++) {
                assertEquals(j, buffer.getShort(i));
            }

            assertEquals(Short.BYTES * NUM_ELEMENTS, buffer.getReadableBytes());
            assertEquals(0, buffer.getReadOffset());
            assertEquals(Short.BYTES * NUM_ELEMENTS, buffer.getReadableBytes());
        }
    }

    @Test
    public void testSetAndGetIntegersAcrossMultipleArrays() {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
             ProtonCompositeBuffer buffer = allocator.composite()) {

            final int NUM_ELEMENTS = 4;

            for (short i = 0; i < Integer.BYTES * NUM_ELEMENTS; ++i) {
                buffer.append(allocator.copy(new byte[] {0}));
            }

            for (short i = 0, j = 1; i < buffer.getReadableBytes(); i += Integer.BYTES, j++) {
                buffer.setShort(i, j);
            }

            assertEquals(Integer.BYTES * NUM_ELEMENTS, buffer.getReadableBytes());
            assertTrue(buffer.isReadable());
            assertEquals(0, buffer.getReadOffset());

            for (int i = 0, j = 1; i < buffer.getReadableBytes(); i += Integer.BYTES, j++) {
                assertEquals(j, buffer.getShort(i));
            }

            assertEquals(Integer.BYTES * NUM_ELEMENTS, buffer.getReadableBytes());
            assertEquals(0, buffer.getReadOffset());
            assertEquals(Integer.BYTES * NUM_ELEMENTS, buffer.getReadableBytes());
        }
    }

    @Test
    public void testSetAndGetLongsAcrossMultipleArrays() {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
             ProtonCompositeBuffer buffer = allocator.composite()) {

            final int NUM_ELEMENTS = 4;

            for (short i = 0; i < Long.BYTES * NUM_ELEMENTS; ++i) {
                buffer.append(allocator.copy(new byte[] {0}));
            }

            for (short i = 0, j = 1; i < buffer.getReadableBytes(); i += Long.BYTES, j++) {
                buffer.setLong(i, j);
            }

            assertEquals(Long.BYTES * NUM_ELEMENTS, buffer.getReadableBytes());
            assertTrue(buffer.isReadable());
            assertEquals(0, buffer.getReadOffset());

            for (int i = 0, j = 1; i < buffer.getReadableBytes(); i += Long.BYTES, j++) {
                assertEquals(j, buffer.getLong(i));
            }

            assertEquals(Long.BYTES * NUM_ELEMENTS, buffer.getReadableBytes());
            assertEquals(0, buffer.getReadOffset());
            assertEquals(Long.BYTES * NUM_ELEMENTS, buffer.getReadableBytes());
        }
    }

    @SuppressWarnings("resource")
    @Test
    public void testCompositeBuffersCannotHaveDuplicateComponents() {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator()) {
            ProtonBuffer a = allocator.allocate(4);
            assertThrows(IllegalStateException.class, () -> allocator.composite(new ProtonBuffer[] {a, a}));

            ProtonBuffer b = allocator.allocate(4);
            try (ProtonCompositeBuffer composite = allocator.composite(b)) {
                assertThrows(IllegalStateException.class, () -> composite.append(b));
            }
        }
    }

    @Test
    public void testCompositeBufferTransfersOwnership() {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator()) {
            ProtonBuffer buffer1 = allocator.allocate(8);
            ProtonBuffer buffer2 = allocator.allocate(8);
            ProtonBuffer buffer3 = allocator.allocate(8);

            ProtonBuffer composite = allocator.composite(new ProtonBuffer[] { buffer1, buffer2, buffer3 });
            assertEquals(24, composite.capacity());

            assertTrue(buffer1.isClosed());
            assertTrue(buffer2.isClosed());
            assertTrue(buffer3.isClosed());
        }
    }

    @Test
    public void testCompositeBufferFromMultipleBuffers() {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
             ProtonBuffer composite = allocator.composite(new ProtonBuffer[] {
                                                          allocator.allocate(8),
                                                          allocator.allocate(8),
                                                          allocator.allocate(8) })) {
            assertEquals(24, composite.capacity());
            assertEquals(0, composite.getReadableBytes());
            assertEquals(24, composite.getWritableBytes());
        }
    }

    @Test
    public void testCompositeBufferFromBuffersWithReadableData() {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
             ProtonBuffer composite = allocator.composite(new ProtonBuffer[] {
                 allocator.allocate(8).writeInt(42),
                 allocator.allocate(8),
                 allocator.allocate(8) })) {
            assertEquals(24, composite.capacity());
            assertEquals(4, composite.getReadableBytes());
            assertEquals(20, composite.getWritableBytes());
        }
    }

    @Test
    public void testCompositeBufferFromBufferWithHole() {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
             ProtonBuffer composite = allocator.composite(new ProtonBuffer[] {
                     allocator.allocate(8).writeInt(42),
                     // This leaves the 4 writable bytes in prior buffer inaccessible (creates a hole):
                     allocator.allocate(8).writeInt(42),
                     allocator.allocate(8) })) {
            assertEquals(20, composite.capacity());
            assertEquals(8, composite.getReadableBytes());
            assertEquals(12, composite.getWritableBytes());
        }
    }

    @SuppressWarnings("resource")
    @Test
    @Disabled
    public void testCompositeBufferMustNotBeAllowedToContainThemselves() {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
            ProtonCompositeBuffer bufA = allocator.composite(allocator.allocate(4));
            ProtonBuffer sendA = bufA.transfer()) {
            try {
                assertThrows(ProtonBufferClosedException.class, () -> bufA.append(sendA));
            } finally {
                sendA.close();
            }

            ProtonCompositeBuffer bufB = allocator.composite(allocator.allocate(4));
            ProtonBuffer sendB = bufB.transfer();
            try (ProtonCompositeBuffer compositeBuffer = allocator.composite(sendB)) {
                assertThrows(IllegalStateException.class, () -> compositeBuffer.append(sendB));
            } finally {
                sendB.close();
            }
        }
    }

    @Test
    public void testAppendWritableOnCompositeBuffersUsesBigEndianByteOrder() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator()) {
            ProtonBuffer composite;
            try (ProtonBuffer a = allocator.allocate(4)) {
                composite = allocator.composite(a);
            }
            try (composite) {
                composite.writeInt(0x01020304);
                composite.ensureWritable(4);
                composite.writeInt(0x05060708);
                assertEquals(0x0102030405060708L, composite.readLong());
            }
        }
    }

    @Test
    public void testAppendingToNonOwnedCompositeBufferMustThrow() {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
             ProtonBuffer a = allocator.allocate(8);
             ProtonBuffer b = allocator.allocate(8);
             ProtonCompositeBuffer composed = allocator.composite(a)) {

            try (ProtonBuffer ignore = composed.transfer()) {
                assertThrows(IllegalStateException.class, () -> composed.append(b));
            }
        }
    }

    @SuppressWarnings("resource")
	@Test
    public void testAppendingCompositeBufferToItselfMustThrow() {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator()) {
            ProtonCompositeBuffer composite;
            try (ProtonBuffer a = allocator.allocate(8)) {
                composite = allocator.composite(a);
            }
            try (composite) {
                assertThrows(ProtonBufferClosedException.class, () -> composite.append(composite));
            }
        }
    }

    @Test
    public void testAppendingToCompositeBufferWithNullMustThrow() {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
             ProtonCompositeBuffer composite = allocator.composite()) {
            assertThrows(NullPointerException.class, () -> composite.append(null));
        }
    }

    @Test
    public void testextendingWithZeroCapacityBufferHasNoEffect() {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
             ProtonCompositeBuffer composite = allocator.composite()) {
            composite.append(allocator.composite());

            assertEquals(0, composite.capacity());

            Iterable<ProtonBuffer> decomposed = composite.decomposeBuffer();
            Iterator<ProtonBuffer> iter = decomposed.iterator();
            assertFalse(iter.hasNext());
        }
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator()) {
            ProtonBuffer a = allocator.allocate(1);
            ProtonCompositeBuffer composite = allocator.composite(a);
            assertEquals(1, composite.capacity());
            try (ProtonBuffer b = allocator.composite()) {
                composite.append(b);
            }
            assertEquals(1, composite.capacity());

            Iterable<ProtonBuffer> decomposed = composite.decomposeBuffer();
            Iterator<ProtonBuffer> iter = decomposed.iterator();
            assertTrue(iter.hasNext());
            iter.next();
            assertFalse(iter.hasNext());
        }
    }

    @Test
    public void testExtendingCompositeBufferMustIncreaseCapacityByGivenBuffer() {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
             ProtonCompositeBuffer composite = allocator.composite()) {
            assertEquals(0, composite.capacity());
            try (ProtonBuffer buf = allocator.allocate(8)) {
                composite.append(buf);
            }
            assertEquals(8, composite.capacity());
            composite.writeLong(0x0102030405060708L);
            assertEquals(0x0102030405060708L, composite.readLong());
        }
    }

    @Test
    public void testEmptyCompositeBufferMustAllowSettingOffsetsToZero() {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
             ProtonCompositeBuffer composite = allocator.composite()) {
            composite.setReadOffset(0);
            composite.setWriteOffset(0);
            composite.clear();
        }
    }

    @Test
    public void testEmptyCompositeBufferMustAllowExtendingWithBuffer() {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
             ProtonCompositeBuffer composite = allocator.composite()) {

            try (ProtonBuffer b = allocator.allocate(8)) {
                composite.append(b);
                assertEquals(8, composite.capacity());
            }
        }
    }

    @Test
    public void testEmptyCompositeBufferMustAllowExtendingWithReadOnlyBuffer() {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
             ProtonCompositeBuffer composite = allocator.composite()) {

            try (ProtonBuffer b = allocator.allocate(8).convertToReadOnly()) {
                composite.append(b);
                assertTrue(composite.isReadOnly());
            }
        }
    }

    @Test
    public void testWhenExtendingCompositeBufferWithWriteOffsetAtCapacityExtensionWriteOffsetCanBeNonZero() {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator()) {
            ProtonCompositeBuffer composite;
            try (ProtonBuffer a = allocator.allocate(8)) {
                composite = allocator.composite(a);
            }
            try (composite) {
                composite.writeLong(0);
                try (ProtonBuffer b = allocator.allocate(8)) {
                    b.writeInt(1);
                    composite.append(b);
                    assertEquals(16, composite.capacity());
                    assertEquals(12, composite.getWriteOffset());
                }
            }
        }
    }

    @Test
    public void testWhenAppendingCompositeBufferWithWriteOffsetLessThanCapacityThenReadableBytesMustConcatenate() {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator()) {
            ProtonCompositeBuffer composite;
            try (ProtonBuffer a = allocator.allocate(8)) {
                composite = allocator.composite(a);
            }
            try (composite) {
                composite.writeInt(1);
                try (ProtonBuffer b = allocator.allocate(8)) {
                    b.writeInt(2);
                    composite.append(b);
                }
                assertEquals(8, composite.getReadableBytes());
                assertEquals(0x00000001_00000002L, composite.readLong());
                assertEquals(12, composite.capacity());

                try (ProtonBuffer c = allocator.allocate(8)) {
                    c.writeLong(3);
                    composite.append(c);
                }
                assertEquals(8, composite.getReadableBytes());
                assertEquals(0x00000000_00000003L, composite.readLong());
                assertEquals(16, composite.capacity()); // 2*4 writable bytes lost in the gaps. 24 - 8 = 16.
                try (ProtonBuffer b = allocator.allocate(8)) {
                    b.setInt(0, 1);
                    composite.append(b);
                }
                assertEquals(24, composite.capacity());
                assertEquals(16, composite.getWriteOffset());
            }
        }
    }

    @Test
    public void testMustConcatenateWritableBytesWhenExtensionWriteOffsetIsZero() {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator()) {
            try (ProtonCompositeBuffer composite = allocator.composite(allocator.allocate(8))) {
                composite.writeInt(0x01020304);
                assertEquals(4, composite.getWritableBytes());
                composite.append(allocator.allocate(8));
                assertEquals(12, composite.getWritableBytes());
                assertEquals(16, composite.capacity());
                composite.writeLong(0x05060708_0A0B0C0DL);
                assertEquals(4, composite.getWritableBytes());
                assertEquals(12, composite.getReadableBytes());
                assertEquals(0x01020304, composite.readInt());
                assertEquals(0x05060708_0A0B0C0DL, composite.readLong());
                assertEquals(0, composite.getReadableBytes());
            }
        }
    }

    @Test
    public void testWhenExtendingCompositeBufferWithReadOffsetAtCapacityExtensionReadOffsetCanBeNonZero() {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator()) {
            ProtonCompositeBuffer composite;
            try (ProtonBuffer a = allocator.allocate(8)) {
                composite = allocator.composite(a);
            }
            try (composite) {
                composite.writeLong(0);
                composite.readLong();
                try (ProtonBuffer b = allocator.allocate(8)) {
                    b.writeInt(1);
                    b.readInt();
                    composite.append(b);
                    assertEquals(16, composite.capacity());
                    assertEquals(12, composite.getWriteOffset());
                }
            }
        }
    }

    @Test
    public void testWhenExtendingCompositeBufferWithReadOffsetLessThanCapacityThenReadableBytesMustConcatenate() {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
             ProtonCompositeBuffer composite = allocator.composite(allocator.allocate(8))) {
            composite.writeLong(0);
            composite.readInt();

            ProtonBuffer b = allocator.allocate(8);
            b.writeInt(1);
            b.readInt(); // 'b' has 4 writable bytes, no readable bytes.
            composite.append(b);
            assertEquals(12, composite.capacity());
            assertEquals(8, composite.getWriteOffset()); // woff from first component
            assertEquals(4, composite.getReadOffset());
            assertEquals(4, composite.getReadableBytes());
            assertEquals(4, composite.getWritableBytes());

            ProtonBuffer c = allocator.allocate(8);
            c.writeLong(1);
            c.readLong(); // no readable or writable bytes.
            composite.append(c);
            assertEquals(12, composite.capacity());
            assertEquals(8, composite.getWriteOffset());
            assertEquals(4, composite.getReadOffset());
            assertEquals(4, composite.getReadableBytes());
            assertEquals(4, composite.getWritableBytes());

            // contribute 4 readable bytes, but make the existing writable bytes unavailable
            composite.append(allocator.allocate(8).writeInt(1));
            assertEquals(16, composite.capacity());
            assertEquals(12, composite.getWriteOffset());
            assertEquals(4, composite.getReadOffset());
        }
    }

    @Test
    public void testCompositionMustIgnoreMiddleBuffersWithNoReadableBytes() {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
             ProtonBuffer a = allocator.allocate(8).setWriteOffset(8).setReadOffset(4);
             ProtonBuffer b = allocator.allocate(8).setWriteOffset(4).setReadOffset(4);
             ProtonBuffer c = allocator.allocate(8).setWriteOffset(4).setReadOffset(0);
             ProtonCompositeBuffer composite = allocator.composite(new ProtonBuffer[] { a, b, c })) {

            int count = 0;
            for (ProtonBuffer buffer : composite.decomposeBuffer()) {
                buffer.close();
                count++;
            }

            assertEquals(2, count);
        }
    }

    @Test
    public void testAppendMustIgnoreMiddleBuffersWithNoReadableBytes() {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
             ProtonBuffer a = allocator.allocate(8).setWriteOffset(8).setReadOffset(4);
             ProtonBuffer b = allocator.allocate(8).setWriteOffset(4).setReadOffset(4);
             ProtonBuffer c = allocator.allocate(8).setWriteOffset(4).setReadOffset(0);
             ProtonCompositeBuffer composite = allocator.composite()) {

            composite.append(a);
            composite.append(b);
            composite.append(c);

            int count = 0;
            for (ProtonBuffer buffer : composite.decomposeBuffer()) {
                buffer.close();
                count++;
            }

            assertEquals(2, count);
        }
    }

    @Test
    public void testComposeWithUnwrittenLeadingBuffersAndUnreadTailBufferTrimsLeading() {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
             ProtonBuffer a = allocator.allocate(8);
             ProtonBuffer b = allocator.allocate(8);
             ProtonBuffer c = allocator.allocate(8).setWriteOffset(4).setReadOffset(0);
             ProtonCompositeBuffer composite = allocator.composite(new ProtonBuffer[] { a, b, c })) {

            int count = 0;
            for (ProtonBuffer buffer : composite.decomposeBuffer()) {
                buffer.close();
                count++;
            }

            assertEquals(1, count);
        }
    }

    @Test
    public void testAppendMustFlattenCompositeBuffers() {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
             ProtonBuffer a = allocator.copy(new byte[1]);
             ProtonBuffer b = allocator.copy(new byte[1]);
             ProtonCompositeBuffer composite = allocator.composite()) {

            composite.append(allocator.composite(new ProtonBuffer[] { a, b }));

            int count = 0;
            for (ProtonBuffer buffer : composite.decomposeBuffer()) {
                buffer.close();
                count++;
            }

            assertEquals(2, count);
        }
    }

    @Test
    public void testComposingReadOnlyBuffersMustCreateReadOnlyCompositeBuffer() {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
             ProtonBuffer a = allocator.allocate(4).convertToReadOnly();
             ProtonBuffer b = allocator.allocate(4).convertToReadOnly();
             ProtonCompositeBuffer composite = allocator.composite(new ProtonBuffer[] { a, b })) {
            assertTrue(composite.isReadOnly());
            verifyWriteInaccessible(composite, ProtonBufferReadOnlyException.class);
        }
    }

    @SuppressWarnings("resource")
    @Test
    public void testComposingReadOnlyAndWritableBuffersMustThrow() {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator()) {
            try (ProtonBuffer a = allocator.allocate(8).convertToReadOnly();
                 ProtonBuffer b = allocator.allocate(8)) {
                assertThrows(IllegalArgumentException.class, () -> allocator.composite(new ProtonBuffer[] { a, b }));
            }
            try (ProtonBuffer a = allocator.allocate(8);
                 ProtonBuffer b = allocator.allocate(8).convertToReadOnly()) {
                assertThrows(IllegalArgumentException.class, () -> allocator.composite(new ProtonBuffer[] { a, b }));
            }
            try (ProtonBuffer a = allocator.allocate(8).convertToReadOnly();
                 ProtonBuffer b = allocator.allocate(8)) {
                assertThrows(IllegalArgumentException.class, () -> allocator.composite(new ProtonBuffer[] { b, a }));
            }
            try (ProtonBuffer a = allocator.allocate(8).convertToReadOnly();
                 ProtonBuffer b = allocator.allocate(8);
                 ProtonBuffer c = allocator.allocate(8).convertToReadOnly()) {
                assertThrows(IllegalArgumentException.class, () -> allocator.composite(new ProtonBuffer[] { a, b, c }));
            }
            try (ProtonBuffer a = allocator.allocate(8).convertToReadOnly();
                 ProtonBuffer b = allocator.allocate(8);
                 ProtonBuffer c = allocator.allocate(8)) {
                assertThrows(IllegalArgumentException.class, () -> allocator.composite(new ProtonBuffer[] { b, a, c }));
            }
        }
    }

    @SuppressWarnings("resource")
    @Test
    public void testCompositeWritableBufferCannotBeExtendedWithReadOnlyBuffer() {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
             ProtonCompositeBuffer composite = allocator.composite(allocator.allocate(8))) {

            try (ProtonBuffer b = allocator.allocate(8).convertToReadOnly()) {
                assertThrows(IllegalArgumentException.class, () -> composite.append(b));
            }
        }
    }

    @SuppressWarnings("resource")
    @Test
    public void testCompositeReadOnlyBufferCannotBeExtendedWithWritableBuffer() {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
             ProtonCompositeBuffer composite = allocator.composite(allocator.allocate(8).convertToReadOnly())) {

            try (ProtonBuffer b = allocator.allocate(8)) {
                assertThrows(IllegalArgumentException.class, () -> composite.append(b));
            }
        }
    }

    @Test
    public void testDecomposeOfEmptyBufferMustGiveEmptyIterable() {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator()) {
             ProtonCompositeBuffer composite = allocator.composite();

            Iterable<ProtonBuffer> decomposed = composite.decomposeBuffer();
            Iterator<ProtonBuffer> iter = decomposed.iterator();
            assertFalse(iter.hasNext());

            verifyInaccessible(composite);
            assertDoesNotThrow(composite::close);
        }
    }

    @Test
    public void testAppendMoreReadableSpaceToCompositeBufferConcatinates() {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator()) {
            try (ProtonCompositeBuffer composite = allocator.composite();
                 ProtonBuffer a = allocator.allocate(4);
                 ProtonBuffer b = allocator.allocate(4)) {

                a.writeInt(0x01020304);
                assertEquals(4, a.getReadableBytes());
                assertEquals(0, a.getWritableBytes());
                b.writeInt(0x05060708);
                assertEquals(4, b.getReadableBytes());
                assertEquals(0, b.getWritableBytes());

                composite.append(a);
                assertTrue(a.isClosed());
                assertEquals(4, composite.getReadableBytes());
                assertEquals(0, composite.getWritableBytes());
                assertEquals(0, composite.getReadOffset());
                assertEquals(4, composite.getWriteOffset());
                composite.append(b);
                assertTrue(b.isClosed());
                assertEquals(8, composite.getReadableBytes());
                assertEquals(0, composite.getWritableBytes());
                assertEquals(0, composite.getReadOffset());
                assertEquals(8, composite.getWriteOffset());

                assertEquals(0x0102030405060708L, composite.readLong());
                assertEquals(0, composite.getReadableBytes());
                assertEquals(0, composite.getWritableBytes());
                assertEquals(8, composite.getReadOffset());
                assertEquals(8, composite.getWriteOffset());
            }
        }
    }

    @Test
    public void testDecomposeOfCompositeBufferMustGiveComponentIterable() {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator()) {
            ProtonCompositeBuffer composite = allocator.composite(new ProtonBuffer[] {
                allocator.allocate(3), allocator.allocate(3), allocator.allocate(2) });

            composite.writeLong(0x0102030405060708L);
            assertEquals(0x01020304, composite.readInt());
            Iterable<ProtonBuffer> components = composite.decomposeBuffer();
            verifyInaccessible(composite);

            Iterator<ProtonBuffer> iterator = components.iterator();
            ProtonBuffer first = iterator.next();
            assertEquals(0, first.getReadableBytes());
            assertEquals(0, first.getWritableBytes());

            ProtonBuffer second = iterator.next();
            assertEquals(2, second.getReadableBytes());
            assertEquals(0, second.getWritableBytes());

            ProtonBuffer third = iterator.next();
            assertEquals(2, third.getReadableBytes());
            assertEquals(0, third.getWritableBytes());

            assertEquals(0x0506, second.readShort());
            assertEquals(0x0708, third.readShort());
        }
    }

    @Test
    public void testHashCodeNotFromIdentity() throws CharacterCodingException {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
             ProtonCompositeBuffer composite = allocator.composite()) {

            assertEquals(1, composite.hashCode());

            byte[] data = new byte[] {9, 8, 7, 6, 5, 4, 3, 2, 1, 0};

            composite.append(allocator.copy(data));

            assertTrue(composite.hashCode() != 1);
            assertNotEquals(composite.hashCode(), System.identityHashCode(composite));
            assertEquals(composite.hashCode(), composite.hashCode());
        }
    }

    @Test
    public void testHashCodeOnSameBackingBuffer() throws CharacterCodingException {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
             ProtonCompositeBuffer composite1 = allocator.composite();
             ProtonCompositeBuffer composite2 = allocator.composite();
             ProtonCompositeBuffer composite3 = allocator.composite()) {

            byte[] data = new byte[] {9, 8, 7, 6, 5, 4, 3, 2, 1, 0};

            composite1.append(allocator.copy(data));
            composite2.append(allocator.copy(data));
            composite3.append(allocator.copy(data));

            assertEquals(composite1.hashCode(), composite2.hashCode());
            assertEquals(composite2.hashCode(), composite3.hashCode());
            assertEquals(composite3.hashCode(), composite1.hashCode());
        }
    }

    @Test
    public void testHashCodeOnDifferentBackingBuffer() throws CharacterCodingException {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
             ProtonCompositeBuffer composite1 = allocator.composite();
             ProtonCompositeBuffer composite2 = allocator.composite()) {

            byte[] data1 = new byte[] {9, 8, 7, 6, 5, 4, 3, 2, 1, 0};
            byte[] data2 = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};

            composite1.append(allocator.copy(data1));
            composite2.append(allocator.copy(data2));

            assertNotEquals(composite1.hashCode(), composite2.hashCode());
        }
    }

    @Test
    public void testHashCodeOnSplitBufferContentsNotSame() throws CharacterCodingException {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
             ProtonCompositeBuffer composite1 = allocator.composite();
             ProtonCompositeBuffer composite2 = allocator.composite()) {

            byte[] data1 = new byte[] {9, 8, 7, 6, 5, 4, 3, 2, 1, 0};
            byte[] data2 = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};

            composite1.append(allocator.copy(data1)).append(allocator.copy(data2));
            composite2.append(allocator.copy(data2)).append(allocator.copy(data1));

            assertNotEquals(composite1.hashCode(), composite2.hashCode());
        }
    }

    @Test
    public void testHashCodeOnSplitBufferContentsSame() throws CharacterCodingException {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
             ProtonCompositeBuffer composite1 = allocator.composite();
             ProtonCompositeBuffer composite2 = allocator.composite()) {

            byte[] data1 = new byte[] {9, 8, 7, 6, 5, 4, 3, 2, 1, 0};
            byte[] data2 = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};

            composite1.append(allocator.copy(data1)).append(allocator.copy(data2));
            composite2.append(allocator.copy(data1)).append(allocator.copy(data2));

            assertEquals(composite1.hashCode(), composite2.hashCode());
        }
    }

    @Test
    public void testHashCodeMatchesByteBufferWhenLimitSetGivesNoRemaining() throws CharacterCodingException {
        byte[] data = new byte[] {9, 8, 7, 6, 5, 4, 3, 2, 1, 0};

        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
             ProtonCompositeBuffer composite1 = allocator.composite();
             ProtonCompositeBuffer composite2 = allocator.composite()) {

            composite1.append(allocator.copy(data));
            composite1.setReadOffset(composite1.getWriteOffset());

            composite2.append(allocator.copy(data));
            composite2.setReadOffset(composite1.getWriteOffset());

            assertEquals(composite1.hashCode(), composite2.hashCode());
        }
    }

    @Test
    public void testHashCodeMatchesByteBufferSingleArrayContents() throws CharacterCodingException {
        byte[] data = new byte[] {9, 8, 7, 6, 5, 4, 3, 2, 1, 0};

        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
             ProtonCompositeBuffer composite1 = allocator.composite();
             ProtonCompositeBuffer composite2 = allocator.composite()) {

            composite1.append(allocator.copy(data));
            composite2.append(allocator.copy(data));

            assertEquals(composite1.hashCode(), composite2.hashCode());
        }
    }

    @Test
    public void testHashCodeMatchesByteBufferMultipleArrayContents() throws CharacterCodingException {
        byte[] data = new byte[] {9, 8, 7, 6, 5, 4, 3, 2, 1, 0};

        byte[] data1 = new byte[] {9, 8, 7, 6, 5};
        byte[] data2 = new byte[] {4, 3, 2, 1, 0};

        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
             ProtonCompositeBuffer composite1 = allocator.composite();
             ProtonCompositeBuffer composite2 = allocator.composite()) {

            composite1.append(allocator.copy(data1));
            composite1.append(allocator.copy(data2));

            composite2.append(allocator.copy(data));

            assertEquals(composite1.hashCode(), composite2.hashCode());
        }
    }

    @Test
    public void testHashCodeMatchesByteBufferMultipleArrayContentsWithRangeOfLimits() throws CharacterCodingException {
        byte[] data = new byte[] {10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0};

        byte[] data1 = new byte[] {10, 9};
        byte[] data2 = new byte[] {8, 7};
        byte[] data3 = new byte[] {6, 5, 4};
        byte[] data4 = new byte[] {3};
        byte[] data5 = new byte[] {2, 1, 0};

        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
             ProtonCompositeBuffer composite1 = allocator.composite();
             ProtonCompositeBuffer composite2 = allocator.composite()) {

            composite1.append(allocator.copy(data1))
                      .append(allocator.copy(data2))
                      .append(allocator.copy(data3))
                      .append(allocator.copy(data4))
                      .append(allocator.copy(data5));

            composite2.append(allocator.copy(data));

            for (int i = 0; i < data.length; ++i) {
                composite1.setWriteOffset(i);
                composite2.setWriteOffset(i);

                assertEquals(composite1.hashCode(), composite2.hashCode());
            }
        }
    }

    @Test
    public void testReadStringFromEmptyBuffer() throws CharacterCodingException {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
             ProtonCompositeBuffer composite = allocator.composite()) {

            assertEquals("", composite.toString(StandardCharsets.UTF_8));
        }
    }

    @Test
    public void testReadStringFromUTF8InSingleArray() throws CharacterCodingException {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
             ProtonCompositeBuffer composite = allocator.composite()) {

            final String testString = "Test String to Decode!";
            byte[] encoded = testString.getBytes(StandardCharsets.UTF_8);

            composite.append(allocator.copy(encoded));

            assertEquals(testString, composite.toString(StandardCharsets.UTF_8));
        }
    }

    @Test
    public void testReadStringFromUTF8InSingleArrayWithLimits() throws CharacterCodingException {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
             ProtonCompositeBuffer composite = allocator.composite()) {

            final String testString = "Test String to Decode!";
            byte[] encoded = testString.getBytes(StandardCharsets.UTF_8);

            // Only read the first character
            composite.append(allocator.copy(encoded));
            composite.setWriteOffset(1);

            assertEquals("T", composite.toString(StandardCharsets.UTF_8));
        }
    }

    @Test
    public void testReadStringFromUTF8InMultipleArrays() throws CharacterCodingException {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
             ProtonCompositeBuffer composite = allocator.composite()) {

            final String testString = "Test String to Decode!!";
            byte[] encoded = testString.getBytes(StandardCharsets.UTF_8);

            byte[] first = new byte[encoded.length / 2];
            byte[] second = new byte[encoded.length - (encoded.length / 2)];

            System.arraycopy(encoded, 0, first, 0, first.length);
            System.arraycopy(encoded, first.length, second, 0, second.length);

            composite.append(allocator.copy(first)).append(allocator.copy(second));

            String result = composite.toString(StandardCharsets.UTF_8);

            assertEquals(testString, result);
        }
    }

    @Test
    public void testReadStringFromUTF8InMultipleArraysWithLimits() throws CharacterCodingException {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
             ProtonCompositeBuffer composite = allocator.composite()) {

            final String testString = "Test String to Decode!";
            byte[] encoded = testString.getBytes(StandardCharsets.UTF_8);

            byte[] first = new byte[encoded.length / 2];
            byte[] second = new byte[encoded.length - (encoded.length / 2)];

            System.arraycopy(encoded, 0, first, 0, first.length);
            System.arraycopy(encoded, first.length, second, 0, second.length);

            composite.append(allocator.copy(first)).append(allocator.copy(second));

            // Only read the first character
            composite.setWriteOffset(1);

            assertEquals("T", composite.toString(StandardCharsets.UTF_8));
        }
    }

    @Test
    public void testReadUnicodeStringAcrossArrayBoundaries() throws IOException {
        String expected = "\u1f4a9\u1f4a9\u1f4a9";

        byte[] utf8 = expected.getBytes(StandardCharsets.UTF_8);

        byte[] slice1 = new byte[] { utf8[0] };
        byte[] slice2 = new byte[utf8.length - 1];

        System.arraycopy(utf8, 1, slice2, 0, slice2.length);

        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
             ProtonCompositeBuffer composite = allocator.composite()) {

            composite.append(allocator.copy(slice1));
            composite.append(allocator.copy(slice2));

            String result = composite.toString(StandardCharsets.UTF_8);

            assertEquals(expected, result, "Failed to round trip String correctly: ");
        }
    }

    @Test
    public void testReadUnicodeStringAcrossMultipleArrayBoundaries() throws IOException {
        String expected = "\u1f4a9\u1f4a9\u1f4a9";

        byte[] utf8 = expected.getBytes(StandardCharsets.UTF_8);

        byte[] slice1 = new byte[] { utf8[0] };
        byte[] slice2 = new byte[] { utf8[1], utf8[2] };
        byte[] slice3 = new byte[] { utf8[3], utf8[4] };
        byte[] slice4 = new byte[utf8.length - 5];

        System.arraycopy(utf8, 5, slice4, 0, slice4.length);

        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
             ProtonCompositeBuffer composite = allocator.composite()) {

            composite.append(allocator.copy(slice1));
            composite.append(allocator.copy(slice2));
            composite.append(allocator.copy(slice3));
            composite.append(allocator.copy(slice4));

            String result = composite.toString(StandardCharsets.UTF_8);

            assertEquals(expected, result, "Failed to round trip String correctly: ");
        }
    }

    @Test
    public void testReadUnicodeStringEachByteInOwnArray() throws IOException {
        String expected = "\u1f4a9";

        byte[] utf8 = expected.getBytes(StandardCharsets.UTF_8);

        assertEquals(4, utf8.length);

        byte[] slice1 = new byte[] { utf8[0] };
        byte[] slice2 = new byte[] { utf8[1] };
        byte[] slice3 = new byte[] { utf8[2] };
        byte[] slice4 = new byte[] { utf8[3] };

        System.arraycopy(utf8, 1, slice2, 0, slice2.length);

        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
             ProtonCompositeBuffer composite = allocator.composite()) {

            composite.append(allocator.copy(slice1));
            composite.append(allocator.copy(slice2));
            composite.append(allocator.copy(slice3));
            composite.append(allocator.copy(slice4));

            String result = composite.toString(StandardCharsets.UTF_8);

            assertEquals(expected, result, "Failed to round trip String correctly: ");
        }
    }

    @Test
    public void testEqualsOnSameBackingBuffer() throws CharacterCodingException {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
             ProtonCompositeBuffer composite1 = allocator.composite();
             ProtonCompositeBuffer composite2 = allocator.composite();
             ProtonCompositeBuffer composite3 = allocator.composite()) {

            byte[] data = new byte[] {9, 8, 7, 6, 5, 4, 3, 2, 1, 0};

            composite1.append(allocator.copy(data));
            composite2.append(allocator.copy(data));
            composite3.append(allocator.copy(data));

            assertEquals(composite1, composite2);
            assertEquals(composite2, composite3);
            assertEquals(composite3, composite1);

            assertEquals(0, composite1.getReadOffset());
            assertEquals(0, composite2.getReadOffset());
            assertEquals(0, composite3.getReadOffset());
        }
    }

    @Test
    public void testEqualsOnDifferentBackingBuffer() throws CharacterCodingException {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
             ProtonCompositeBuffer composite1 = allocator.composite();
             ProtonCompositeBuffer composite2 = allocator.composite()) {

            byte[] data1 = new byte[] {9, 8, 7, 6, 5, 4, 3, 2, 1, 0};
            byte[] data2 = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};

            composite1.append(allocator.copy(data1));
            composite2.append(allocator.copy(data2));

            assertNotEquals(composite1, composite2);

            assertEquals(0, composite1.getReadOffset());
            assertEquals(0, composite2.getReadOffset());
        }
    }

    @Test
    public void testEqualsWhenContentsInMultipleArraysNotSame() throws CharacterCodingException {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
             ProtonCompositeBuffer composite1 = allocator.composite();
             ProtonCompositeBuffer composite2 = allocator.composite()) {

            byte[] data1 = new byte[] {9, 8, 7, 6, 5, 4, 3, 2, 1, 0};
            byte[] data2 = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};

            composite1.append(allocator.copy(data1)).append(allocator.copy(data2));
            composite2.append(allocator.copy(data2)).append(allocator.copy(data1));

            assertNotEquals(composite1, composite2);

            assertEquals(0, composite1.getReadOffset());
            assertEquals(0, composite2.getReadOffset());
        }
    }

    @Test
    public void testEqualsWhenContentsInMultipleArraysSame() throws CharacterCodingException {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
             ProtonCompositeBuffer composite1 = allocator.composite();
             ProtonCompositeBuffer composite2 = allocator.composite()) {

            byte[] data1 = new byte[] {9, 8, 7, 6, 5, 4, 3, 2, 1, 0};
            byte[] data2 = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};

            composite1.append(allocator.copy(data1)).append(allocator.copy(data2));
            composite2.append(allocator.copy(data1)).append(allocator.copy(data2));

            assertEquals(composite1, composite2);

            assertEquals(0, composite1.getReadOffset());
            assertEquals(0, composite2.getReadOffset());
        }
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
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
             ProtonCompositeBuffer composite1 = allocator.composite();
             ProtonCompositeBuffer composite2 = allocator.composite()) {

            byte[] data1 = new byte[] {-1, -1, 0, 1, 2, 3, 4, 5};
            byte[] data2 = new byte[] {-1, -1, -1, 0, 1, 2, 3, 4, 5};

            composite1.append(allocator.copy(data1));
            composite1.setReadOffset(2);
            final int writeOffset1 = composite1.getWriteOffset();

            // Offset wrapped buffer should behave same as buffer 1
            composite2.append(allocator.copy(data2, 1, data1.length));
            composite2.setReadOffset(2);
            final int writeOffset2 = composite2.getWriteOffset();

            if (multipleArrays) {
                byte[] data3 = new byte[] { 5, 4, 3, 2, 1 };
                composite1.append(allocator.copy(data3)).setWriteOffset(writeOffset1);
                composite2.append(allocator.copy(data3)).setWriteOffset(writeOffset2);
            }

            assertEquals(composite1, composite2);

            assertEquals(2, composite1.getReadOffset());
            assertEquals(2, composite2.getReadOffset());
        }
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
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
             ProtonCompositeBuffer composite1 = allocator.composite();
             ProtonCompositeBuffer composite2 = allocator.composite()) {

            byte[] data1 = new byte[] {-1, -1, 0, 1, 2, 3, 4, 5};
            byte[] data2 = new byte[] {-1, -1, -1, 0, 1, 2, 3, 4, -1};

            composite1.append(allocator.copy(data1));
            composite1.setReadOffset(2);

            composite2.append(allocator.copy(data2));
            composite2.setReadOffset(3);

            if (multipleArrays) {
                byte[] data3 = new byte[] { 5, 4, 3, 2, 1 };
                composite1.append(allocator.copy(data3));
                composite2.append(allocator.copy(data3));
            }

            assertNotEquals(composite1, composite2);

            assertEquals(2, composite1.getReadOffset());
            assertEquals(3, composite2.getReadOffset());
        }
    }

    @Test
    public void testEqualsWhenContentRemainingIsSubsetOfSingleChunkInMultiArrayBufferSame() {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
             ProtonCompositeBuffer composite1 = allocator.composite();
             ProtonCompositeBuffer composite2 = allocator.composite()) {

            byte[] data1 = new byte[] {-1, -1, 0, 1, 2, 3, 4, 5};
            byte[] data2 = new byte[] {-1, -1, -1, 0, 1, 2, 3, 4, 5};

            composite1.append(allocator.copy(data1));
            composite1.setReadOffset(2);

            // Offset the wrapped buffer which means these two should behave the same
            composite2.append(allocator.copy(data2, 1, data2.length - 1));
            composite2.setReadOffset(2);

            byte[] data3 = new byte[] { 5, 4, 3, 2, 1 };
            composite1.append(allocator.copy(data3));
            composite2.append(allocator.copy(data3));

            composite1.setWriteOffset(data1.length);
            composite2.setWriteOffset(data1.length);

            assertEquals(6, composite1.getReadableBytes());
            assertEquals(6, composite2.getReadableBytes());

            assertEquals(composite1, composite2);
            assertEquals(composite2, composite1);

            assertEquals(2, composite1.getReadOffset());
            assertEquals(2, composite2.getReadOffset());
        }
    }

    @Test
    public void testEqualsWhenContentRemainingIsSubsetOfSingleChunkInMultiArrayBufferNotSame() {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
             ProtonCompositeBuffer composite1 = allocator.composite();
             ProtonCompositeBuffer composite2 = allocator.composite()) {

            byte[] data1 = new byte[] {-1, -1, 0, 1, 2, 3, 4, 5};
            byte[] data2 = new byte[] {-1, -1, -1, 0, 1, 2, 3, 4, -1};

            composite1.append(allocator.copy(data1));
            composite1.setReadOffset(2);

            composite2.append(allocator.copy(data2));
            composite2.setReadOffset(3);

            byte[] data3 = new byte[] { 5, 4, 3, 2, 1 };
            composite1.append(allocator.copy(data3));
            composite2.append(allocator.copy(data3));

            composite1.setWriteOffset(data1.length);
            composite2.setWriteOffset(data2.length);

            assertEquals(6, composite1.getReadableBytes());
            assertEquals(6, composite2.getReadableBytes());

            assertNotEquals(composite1, composite2);
            assertNotEquals(composite2, composite1);

            assertEquals(2, composite1.getReadOffset());
            assertEquals(3, composite2.getReadOffset());
        }
    }

    @SuppressWarnings("resource")
    @Test
    public void testSplitComponentsFloorMustThrowOnOutOfBounds() {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
             ProtonCompositeBuffer composite = allocator.composite(new ProtonBuffer[] { allocator.allocate(8), allocator.allocate(8) })) {

            assertThrows(IllegalArgumentException.class, () -> composite.splitComponentsFloor(-1));
            assertThrows(IllegalArgumentException.class, () -> composite.splitComponentsFloor(17));
            try (ProtonCompositeBuffer split = composite.splitComponentsFloor(16)) {
                assertEquals(16, split.capacity());
                assertEquals(0, composite.capacity());
            }
        }
    }

    @Test
    public void testSplitComponentsFloorMustGiveEmptyBufferForOffsetInFirstComponent() {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
             ProtonCompositeBuffer composite = allocator.composite(new ProtonBuffer[] { allocator.allocate(8), allocator.allocate(8) })) {

            try (ProtonCompositeBuffer split = composite.splitComponentsFloor(4)) {
                assertFalse(split.isClosed());
                assertEquals(0, split.capacity());

                assertFalse(composite.isClosed());
                assertEquals(16, composite.capacity());
            }
        }
    }

    @Test
    public void testSplitComponentsFloorMustGiveEmptyBufferForOffsetLastByteInFirstComponent() {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
             ProtonCompositeBuffer composite = allocator.composite(new ProtonBuffer[] { allocator.allocate(8), allocator.allocate(8) })) {

            try (ProtonCompositeBuffer split = composite.splitComponentsFloor(7)) {
                assertFalse(split.isClosed());
                assertEquals(0, split.capacity());

                assertFalse(composite.isClosed());
                assertEquals(16, composite.capacity());
            }
        }
    }

    @Test
    public void testSplitComponentsFloorMustGiveBufferWithFirstComponentForOffsetInSecondComponent() {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
             ProtonCompositeBuffer composite = allocator.composite(
                 new ProtonBuffer[] { allocator.allocate(8), allocator.allocate(8) })) {

            try (ProtonCompositeBuffer split = composite.splitComponentsFloor(12)) {
                assertFalse(split.isClosed());
                assertEquals(8, split.capacity());

                assertFalse(composite.isClosed());
                assertEquals(8, composite.capacity());
            }
        }
    }

    @Test
    public void testSplitComponentsFloorMustGiveBufferWithFirstComponentForOffsetOnFirstByteInSecondComponent() {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
             ProtonCompositeBuffer composite = allocator.composite(
                 new ProtonBuffer[] { allocator.allocate(8), allocator.allocate(8) })) {

            try (ProtonCompositeBuffer split = composite.splitComponentsFloor(8)) {
                assertFalse(split.isClosed());
                assertEquals(8, split.capacity());

                assertFalse(composite.isClosed());
                assertEquals(8, composite.capacity());
            }
        }
    }

    @SuppressWarnings("resource")
    @Test
    public void testSplitComponentsCeilMustThrowOnOutOfBounds() {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
             ProtonCompositeBuffer composite = allocator.composite(
                 new ProtonBuffer[] { allocator.allocate(8), allocator.allocate(8) })) {

            assertThrows(IllegalArgumentException.class, () -> composite.splitComponentsCeil(-1));
            assertThrows(IllegalArgumentException.class, () -> composite.splitComponentsCeil(17));
            try (ProtonCompositeBuffer split = composite.splitComponentsCeil(16)) {
                assertEquals(16, split.capacity());
                assertEquals(0, composite.capacity());
            }
        }
    }

    @Test
    public void testSplitComponentsCeilMustGiveBufferWithFirstComponentForOffsetInFirstComponent() {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
             ProtonCompositeBuffer composite = allocator.composite(
                 new ProtonBuffer[] { allocator.allocate(8), allocator.allocate(8) })) {

            try (ProtonCompositeBuffer split = composite.splitComponentsCeil(4)) {
                assertFalse(split.isClosed());
                assertEquals(8, split.capacity());

                assertFalse(composite.isClosed());
                assertEquals(8, composite.capacity());
            }
        }
    }

    @Test
    public void testSplitComponentsCeilMustGiveBufferWithFirstComponentFofOffsetOnLastByteInFirstComponent() {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
             ProtonCompositeBuffer composite = allocator.composite(
                 new ProtonBuffer[] { allocator.allocate(8), allocator.allocate(8) })) {

            try (ProtonCompositeBuffer split = composite.splitComponentsCeil(7)) {
                assertFalse(split.isClosed());
                assertEquals(8, split.capacity());

                assertFalse(composite.isClosed());
                assertEquals(8, composite.capacity());
            }
        }
    }

    @Test
    public void testSplitComponentsCeilMustGiveBufferWithFirstAndSecondComponentForOffsetInSecondComponent() {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
             ProtonCompositeBuffer composite = allocator.composite(new ProtonBuffer[] { allocator.allocate(8), allocator.allocate(8) })) {

            try (ProtonCompositeBuffer split = composite.splitComponentsCeil(12)) {
                assertFalse(split.isClosed());
                assertEquals(16, split.capacity());

                assertFalse(composite.isClosed());
                assertEquals(0, composite.capacity());
            }
        }

        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
             ProtonCompositeBuffer composite = allocator.composite(
                 new ProtonBuffer[] { allocator.allocate(8), allocator.allocate(8), allocator.allocate(8) })) {
            try (ProtonCompositeBuffer split = composite.splitComponentsCeil(12)) {
                assertFalse(split.isClosed());
                assertEquals(16, split.capacity());

                assertFalse(composite.isClosed());
                assertEquals(8, composite.capacity());
            }
        }
    }

    @Test
    public void testSplitComponentsCeilMustGiveBufferWithFirstComponentForOffsetOnFirstByteInSecondComponent() {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
             ProtonCompositeBuffer composite = allocator.composite(new ProtonBuffer[] { allocator.allocate(8), allocator.allocate(8) })) {

            try (ProtonCompositeBuffer split = composite.splitComponentsCeil(7)) {
                assertFalse(split.isClosed());
                assertEquals(8, split.capacity());

                assertFalse(composite.isClosed());
                assertEquals(8, composite.capacity());
            }
        }
    }

    @Test
    public void testSplitComponentsCeilMustGiveEmptyBufferForOffsetOnFirstByteInFirstComponent() {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
             ProtonCompositeBuffer composite = allocator.composite(
                 new ProtonBuffer[] { allocator.allocate(8), allocator.allocate(8) })) {

            try (ProtonCompositeBuffer split = composite.splitComponentsCeil(0)) {
                assertFalse(split.isClosed());
                assertEquals(0, split.capacity());

                assertFalse(composite.isClosed());
                assertEquals(16, composite.capacity());
            }
        }
    }

    @Test
    public void testReadThenCopyDataFromCompositeWithMultipleAppendedElements() {
        doTestReadThenCopyDataFromCompositeWithMultipleAppendedElements(false);
    }

    @Test
    public void testReadThenCopyDataFromReadOnlyCompositeWithMultipleAppendedElements() {
        doTestReadThenCopyDataFromCompositeWithMultipleAppendedElements(true);
    }

    private void doTestReadThenCopyDataFromCompositeWithMultipleAppendedElements(boolean makeReadOnly) {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
             ProtonCompositeBuffer composite = allocator.composite()) {

            byte[] data1 = new byte[] { 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02 };
            byte[] data2 = new byte[] { 0x03, 0x04, 0x05, 0x06, 0x07, 0x08 };
            byte[] data3 = new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08 };

            composite.append(allocator.copy(data1));
            composite.append(allocator.copy(data2));
            composite.append(allocator.copy(data3));

            if (makeReadOnly) {
                composite.convertToReadOnly();
            }

            assertEquals(0x01020304, composite.readInt());
            assertEquals(0x01020304, composite.readInt());
            assertEquals(0x01020304, composite.readInt());

            ProtonBuffer copy = composite.copy(composite.getReadOffset(), Long.BYTES + Long.BYTES, true);

            assertEquals(16, copy.capacity());
            assertEquals(0x0102030405060708l, composite.readLong());
            assertEquals(0x0102030405060708l, composite.readLong());
            assertEquals(0x0102030405060708l, copy.readLong());
            assertEquals(0x0102030405060708l, copy.readLong());
        }
    }

    @Test
    public void testSplitBufferLastBufferNoReadableBytes() {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator()) {
            ProtonBuffer buffer1 = allocator.allocate(8).writeLong(0x0102030405060708L);
            buffer1.advanceReadOffset(4);

            ProtonBuffer buffer2 = allocator.allocate(8).writeLong(0x0102030405060708L);
            buffer2.advanceReadOffset(8);

            try (ProtonCompositeBuffer composite = allocator.composite(new ProtonBuffer[] { buffer1, buffer2 })) {
                try (ProtonBuffer split = composite.split()) {
                    assertEquals(2, split.componentCount());
                    assertEquals(8, split.capacity());
                    assertEquals(4, split.getReadableBytes());
                    assertEquals(0, split.getWritableBytes());

                    assertEquals(0, composite.componentCount());
                    assertEquals(0, composite.capacity());
                    assertEquals(0, composite.getReadableBytes());
                    assertEquals(0, composite.getWritableBytes());
                }
            }
        }
    }

    @Test
    public void testSplitCompositeOfManyBufferInLastBufferSegment() {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
             ProtonCompositeBuffer composite = allocator.composite()) {

            byte[] data1 = new byte[] { 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02 };
            byte[] data2 = new byte[] { 0x03, 0x04, 0x05, 0x06, 0x07, 0x08 };
            byte[] data3 = new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08 };
            byte[] data4 = new byte[] { 0x03, 0x04, 0x05, 0x06, 0x07, 0x08 };
            byte[] data5 = new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08 };

            composite.append(allocator.copy(data1));
            composite.append(allocator.copy(data2));
            composite.append(allocator.copy(data3));
            composite.append(allocator.copy(data4));
            composite.append(allocator.copy(data5));

            final int totalCapacity = data1.length +
                                      data2.length +
                                      data3.length +
                                      data4.length +
                                      data5.length;

            assertEquals(totalCapacity, composite.capacity());
            assertEquals(totalCapacity, composite.getWriteOffset());

            composite.setWriteOffset(totalCapacity - 3);

            assertEquals(totalCapacity - 3, composite.getWriteOffset());

            final ProtonBuffer split = composite.split();

            assertEquals(totalCapacity - 3, split.getWriteOffset());
            assertEquals(3, composite.capacity());

            assertEquals(0, composite.getReadOffset());
            assertEquals(0, composite.getWriteOffset());

            composite.advanceWriteOffset(3);

            assertEquals(0x06, composite.readByte());
            assertEquals(0x07, composite.readByte());
            assertEquals(0x08, composite.readByte());
        }
    }

    @Test
    public void testSplitCompositeOfManyBuffersInsideOFMiddleSegment() {
        try (ProtonBufferAllocator allocator = createProtonDefaultAllocator();
             ProtonCompositeBuffer composite = allocator.composite()) {

            byte[] data1 = new byte[] { 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02 };
            byte[] data2 = new byte[] { 0x03, 0x04, 0x05, 0x06, 0x07, 0x08 };
            byte[] data3 = new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08 };
            byte[] data4 = new byte[] { 0x03, 0x04, 0x05, 0x06, 0x07, 0x08 };
            byte[] data5 = new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08 };

            composite.append(allocator.copy(data1));
            composite.append(allocator.copy(data2));
            composite.append(allocator.copy(data3));
            composite.append(allocator.copy(data4));
            composite.append(allocator.copy(data5));

            final int totalCapacity = data1.length +
                                      data2.length +
                                      data3.length +
                                      data4.length +
                                      data5.length;
            final int splitPoint = data1.length + data2.length + (data3.length / 2);

            assertEquals(totalCapacity, composite.capacity());
            assertEquals(totalCapacity, composite.getWriteOffset());

            final ProtonBuffer split = composite.split(splitPoint);

            assertEquals(splitPoint, split.getWriteOffset());
            assertEquals(totalCapacity - splitPoint, composite.capacity());

            assertEquals(0, composite.getReadOffset());
            assertEquals(totalCapacity - splitPoint, composite.getWriteOffset());
        }
    }

    @Test
    public void testBufferExposesNativeAddressValues() {
        try (ProtonBufferAllocator allocator = new Netty5ProtonBufferAllocator(BufferAllocator.offHeapUnpooled());
             ProtonBuffer buffer = allocator.allocate(16)) {

            buffer.writeLong(Long.MAX_VALUE);
            buffer.readByte();

            try (ProtonBufferComponentAccessor accessor = buffer.componentAccessor()) {
                for (ProtonBufferComponent component : accessor.components()) {
                    assertTrue(component.getNativeAddress() != 0);
                    assertTrue(component.getNativeReadAddress() != 0);
                    assertTrue(component.getNativeWriteAddress() != 0);
                }
            }
        }
    }

    @Test
    public void testBufferExposesNativeAddressValuesForNativeBackedBuffers() {
        try (ProtonBufferAllocator offHeapAllocator = new Netty5ProtonBufferAllocator(BufferAllocator.offHeapUnpooled());
             ProtonBufferAllocator onHeapAllocator = createProtonDefaultAllocator();
             ProtonCompositeBuffer buffer = onHeapAllocator.composite()) {

            buffer.append(offHeapAllocator.allocate(16));
            buffer.append(onHeapAllocator.allocate(16));

            buffer.writeLong(Long.MAX_VALUE);
            buffer.writeLong(Long.MAX_VALUE);
            buffer.readByte();

            int count = 0;

            try (ProtonBufferComponentAccessor accessor = buffer.componentAccessor()) {
                for (ProtonBufferComponent component : accessor.components()) {
                    if (count++ == 0) {
                        assertTrue(component.getNativeAddress() != 0);
                        assertTrue(component.getNativeReadAddress() != 0);
                        assertTrue(component.getNativeWriteAddress() != 0);
                    } else {
                        assertTrue(component.getNativeAddress() == 0);
                        assertTrue(component.getNativeReadAddress() == 0);
                        assertTrue(component.getNativeWriteAddress() == 0);
                    }
                }
            }
        }
    }
}
