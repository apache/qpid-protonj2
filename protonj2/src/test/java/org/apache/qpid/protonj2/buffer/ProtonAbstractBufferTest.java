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

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;

import org.apache.qpid.protonj2.buffer.impl.ProtonByteArrayBufferAllocator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A set of proton buffer basic tests that exercise common functionality that
 * all buffer types should implement in a consistent manner.
 */
public abstract class ProtonAbstractBufferTest {

    /**
     * @return a new allocator used to create the buffer type under test.
     */
    public abstract ProtonBufferAllocator createTestCaseAllocator();

    /**
     * @return an instance of the built in proton buffer allocator.
     */
    public ProtonBufferAllocator createProtonDefaultAllocator() {
        return new ProtonByteArrayBufferAllocator();
    }

    public static final int LARGE_CAPACITY = 4096; // Must be even for these tests
    public static final int BLOCK_SIZE = 128;
    public static final int DEFAULT_CAPACITY = 64;

    protected long seed;
    protected Random random;

    private static FileChannel closedChannel;
    private static FileChannel channel;

    @BeforeAll
    static void setUpChannels(@TempDir Path parentDirectory) throws IOException {
        closedChannel = tempFileChannel(parentDirectory);
        closedChannel.close();
        channel = tempFileChannel(parentDirectory);
    }

    @AfterAll
    static void tearDownChannels() throws IOException {
        channel.close();
    }

    @BeforeEach
    public void setUp() {
        seed = System.currentTimeMillis();
        random = new Random();
        random.setSeed(seed);
    }

    @Test
    public void testBufferShouldNotBeAccessibleAfterClose() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(24)) {
            buf.writeLong(42);
            buf.close();
            verifyInaccessible(buf);
        }
    }

    @Test
    public void testEqualsSelf() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(24)) {
            final byte[] payload = new byte[] { 0, 1, 2, 3, 4 };
            buf.writeBytes(payload);
            assertTrue(buf.equals(buf));
        }
    }

    @Test
    public void testOffsettedGetOfByteMustBoundsCheckOnNegativeOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertThrows(IndexOutOfBoundsException.class, () -> buf.getByte(-1));
        }
    }

    @Test
    public void testOffsettedGetOfByteFromReadOnlyMustBoundsCheckOnNegativeOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertThrows(IndexOutOfBoundsException.class, () -> buf.getByte(-1));
        }
    }

    @Test
    public void testOffsettedGetOfByteMustNotBoundsCheckWhenReadOffsetAndSizeIsEqualToWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(1, buf.componentCount());
            assertEquals(0, buf.readableComponentCount());
            assertEquals(1, buf.writableComponentCount());

            byte value = 0x01;
            buf.writeByte(value);
            assertEquals(value, buf.getByte(0));

            assertEquals(1, buf.componentCount());
            assertEquals(1, buf.readableComponentCount());
            assertEquals(1, buf.writableComponentCount());
        }
    }

    @Test
    public void testOffsettedGetOfByteMustReadWithDefaultEndianByteOrder() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            byte value = 0x01;
            buf.writeByte(value);
            buf.setByte(0, (byte) 0x10);
            assertEquals(0x10, buf.getByte(0));
        }
    }

    @Test
    public void testOffsettedGetOfByteMustBoundsCheckWhenReadOffsetAndSizeIsGreaterThanWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            byte value = 0x01;
            buf.writeByte(value);
            assertThrows(IndexOutOfBoundsException.class, () -> buf.getByte(-1));
        }
    }

    @Test
    public void testOffsettedGetOfByteReadOnlyMustBoundsCheckWhenReadOffsetAndSizeIsGreaterThanWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            byte value = 0x01;
            buf.writeByte(value);
            assertThrows(IndexOutOfBoundsException.class, () -> buf.convertToReadOnly().getByte(-1));
        }
    }

    @Test
    public void testOffsettedGetOfByteMustNotBoundsCheckWhenReadOffsetIsGreaterThanWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            buf.getByte(0);
        }
    }

    @Test
    public void testOffsettedGetOfByteMustBoundsCheckWhenReadOffsetIsGreaterThanCapacity() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertThrows(IndexOutOfBoundsException.class, () -> buf.getByte(8));
        }
    }

    @Test
    public void testOffsettedGetOfByteReadOnlyMustNotBoundsCheckWhenReadOffsetIsGreaterThanWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            buf.convertToReadOnly().getByte(0);
        }
    }

    @Test
    public void testOffsettedGetOfByteReadOnlyMustBoundsCheckWhenReadOffsetIsGreaterThanCapacity() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertThrows(IndexOutOfBoundsException.class, () -> buf.convertToReadOnly().getByte(8));
        }
    }

    @Test
    public void testOffsettedGetOfUnsignedByteMustBoundsCheckOnNegativeOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertThrows(IndexOutOfBoundsException.class, () -> buf.getUnsignedByte(-1));
        }
    }

    @Test
    public void testOffsettedGetOfUnsignedByteReadOnlyMustBoundsCheckOnNegativeOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertThrows(IndexOutOfBoundsException.class, () -> buf.convertToReadOnly().getUnsignedByte(-1));
        }
    }

    @Test
    public void testOffsettedGetOfUnsignedByteMustNotBoundsCheckWhenReadOffsetAndSizeIsEqualToWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            int value = 0x01;
            buf.writeUnsignedByte(value);
            assertEquals(value, buf.getUnsignedByte(0));
        }
    }

    @Test
    public void testOffsettedGetOfUnsignedByteMustReadWithDefaultEndianByteOrder() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            int value = 0x01;
            buf.writeUnsignedByte(value);
            buf.setByte(0, (byte) 0x10);
            assertEquals(0x10, buf.getUnsignedByte(0));
        }
    }

    @Test
    public void testOffsettedGetOfUnsignedByteMustNotBoundsCheckWhenReadOffsetAndSizeIsGreaterThanWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            int value = 0x01;
            buf.writeUnsignedByte(value);
            buf.getUnsignedByte(1);
        }
    }

    @Test
    public void testOffsettedGetOfUnsignedByteMustBoundsCheckWhenReadOffsetAndSizeIsGreaterThanCapacity() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertThrows(IndexOutOfBoundsException.class, () -> buf.getUnsignedByte(8));
        }
    }

    @Test
    public void testOffsettedGetOfUnsignedByteReadOnlyMustNotBoundsCheckWhenReadOffsetAndSizeIsGreaterThanWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            int value = 0x01;
            buf.writeUnsignedByte(value);
            buf.convertToReadOnly().getUnsignedByte(1);
        }
    }

    @Test
    public void testOffsettedGetOfUnsignedByteReadOnlyMustBoundsCheckWhenReadOffsetAndSizeIsGreaterThanCapacity() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertThrows(IndexOutOfBoundsException.class, () -> buf.convertToReadOnly().getUnsignedByte(8));
        }
    }

    @Test
    public void testOffsettedGetOfUnsignedByteMustNotBoundsCheckWhenReadOffsetIsGreaterThanWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            buf.getUnsignedByte(0);
        }
    }

    @Test
    public void testOffsettedGetOfUnsignedByteMustBoundsCheckWhenReadOffsetIsGreaterThanCapacity() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertThrows(IndexOutOfBoundsException.class, () -> buf.getUnsignedByte(8));
        }
    }

    @Test
    public void testOffsettedGetOfUnsignedByteReadOnlyMustNotBoundsCheckWhenReadOffsetIsGreaterThanWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            buf.convertToReadOnly().getUnsignedByte(0);
        }
    }

    @Test
    public void testOffsettedGetOfUnsignedByteReadOnlyMustBoundsCheckWhenReadOffsetIsGreaterThanCapacity() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertThrows(IndexOutOfBoundsException.class, () -> buf.convertToReadOnly().getUnsignedByte(8));
        }
    }

    @Test
    public void testOffsettedSetOfByteMustBoundsCheckWhenWriteOffsetIsNegative() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(Long.BYTES, buf.capacity());
            byte value = 0x01;
            assertThrows(IndexOutOfBoundsException.class, () -> buf.setByte(-1, value));
            buf.setWriteOffset(Long.BYTES);
            // Verify contents are unchanged.
            assertEquals(0, buf.readLong());
        }
    }

    @Test
    public void testOffsettedSetOfByteMustBoundsCheckWhenWriteOffsetAndSizeIsBeyondCapacity() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(Long.BYTES, buf.capacity());
            byte value = 0x01;
            assertThrows(IndexOutOfBoundsException.class, () -> buf.setByte(8, value));
            buf.setWriteOffset(Long.BYTES);
            // Verify contents are unchanged.
            assertEquals(0, buf.readLong());
        }
    }

    @Test
    public void testOffsettedSetOfByteMustHaveDefaultEndianByteOrder() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            byte value = 0x01;
            buf.setByte(0, value);
            buf.setWriteOffset(Long.BYTES);
            assertEquals((byte) 0x01, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
        }
    }

    @Test
    public void testOffsettedSetOfUnsignedByteMustBoundsCheckWhenWriteOffsetIsNegative() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(Long.BYTES, buf.capacity());
            int value = 0x01;
            assertThrows(IndexOutOfBoundsException.class, () -> buf.setUnsignedByte(-1, value));
            buf.setWriteOffset(Long.BYTES);
            // Verify contents are unchanged.
            assertEquals(0, buf.readLong());
        }
    }

    @Test
    public void testOffsettedSetOfUnsignedByteMustBoundsCheckWhenWriteOffsetAndSizeIsBeyondCapacity() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(Long.BYTES, buf.capacity());
            int value = 0x01;
            assertThrows(IndexOutOfBoundsException.class, () -> buf.setUnsignedByte(8, value));
            buf.setWriteOffset(Long.BYTES);
            // Verify contents are unchanged.
            assertEquals(0, buf.readLong());
        }
    }

    @Test
    public void testOffsettedSetOfUnsignedByteMustHaveDefaultEndianByteOrder() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            int value = 0x01;
            buf.setUnsignedByte(0, value);
            buf.setWriteOffset(Long.BYTES);
            assertEquals((byte) 0x01, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
        }
    }

    @Test
    public void testWriteBytesMustWriteAllBytesFromByteArray() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            buf.writeByte((byte) 1);
            buf.writeBytes(new byte[] { 2, 3, 4, 5, 6, 7 });
            assertEquals(7, buf.getWriteOffset());
            assertEquals(0, buf.getReadOffset());
            assertArrayEquals(new byte[] { 1, 2, 3, 4, 5, 6, 7 }, ProtonBufferUtils.toByteArray(buf));
            // Actual buffer should have one more byte in its capacity
            buf.advanceWriteOffset(1);
            assertArrayEquals(new byte[] { 1, 2, 3, 4, 5, 6, 7, 0 }, ProtonBufferUtils.toByteArray(buf));
        }
    }

    @Test
    public void testWriteBytesWithOffsetMustWriteAllBytesFromByteArray() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(3)) {
            buf.writeByte((byte) 1);
            buf.writeBytes(new byte[] { 2, 3, 4, 5, 6, 7 }, 1, 2);
            assertEquals(3, buf.getWriteOffset());
            assertEquals(0, buf.getReadOffset());
            assertArrayEquals(new byte[] { 1, 3, 4 }, ProtonBufferUtils.toByteArray(buf));
        }
    }

    @Test
    public void testReadBytesWithOffsetMustWriteAllBytesIntoByteArray() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            buf.writeLong(0x0102030405060708L);
            byte[] array = new byte[4];
            assertEquals(1, buf.readByte());
            assertEquals(2, buf.readByte());
            buf.readBytes(array, 1, 2);
            assertEquals(8, buf.getWriteOffset());
            assertEquals(4, buf.getReadOffset());
            assertArrayEquals(new byte[] { 0, 3, 4, 0 }, array);
        }
    }

    @Test
    public void testWriteBytesMustWriteAllBytesFromHeapByteBuffer() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            buf.writeByte((byte) 1);
            ByteBuffer source = ByteBuffer.allocate(8).put(new byte[] { 2, 3, 4, 5, 6, 7 }).flip();
            buf.writeBytes(source);
            assertEquals(source.position(), source.limit());
            assertEquals(7, buf.getWriteOffset());
            assertEquals(0, buf.getReadOffset());
            assertArrayEquals(new byte[] { 1, 2, 3, 4, 5, 6, 7 }, ProtonBufferUtils.toByteArray(buf));
        }
    }

    @Test
    public void testWriteBytesMustWriteAllBytesFromDirectByteBuffer() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            buf.writeByte((byte) 1);
            ByteBuffer source = ByteBuffer.allocateDirect(8).put(new byte[] { 2, 3, 4, 5, 6, 7 }).flip();
            buf.writeBytes(source);
            assertEquals(source.position(), source.limit());
            assertEquals(7, buf.getWriteOffset());
            assertEquals(0, buf.getReadOffset());
            assertArrayEquals(new byte[] { 1, 2, 3, 4, 5, 6, 7 }, ProtonBufferUtils.toByteArray(buf));
        }
    }

    @Test
    public void testWriteBytesHeapByteBufferMustExpandCapacityIfBufferIsTooSmall() {
        // With zero offsets
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            ByteBuffer source = ByteBuffer.wrap(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7 });
            buf.writeBytes(source);
            assertTrue(buf.capacity() >= source.capacity());
            assertEquals(source.position(), source.limit());
            assertEquals(16, buf.getWriteOffset());
            assertEquals(0, buf.getReadOffset());
            assertArrayEquals(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7 },
                ProtonBufferUtils.toByteArray(buf));
        }
        // With non-zero offsets
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            ByteBuffer source = ByteBuffer.wrap(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7 });
            buf.writeByte((byte) -1).readByte();
            buf.writeBytes(source);
            assertTrue(buf.capacity() >= source.capacity() + 1);
            assertEquals(source.position(), source.limit());
            assertEquals(17, buf.getWriteOffset());
            assertEquals(1, buf.getReadOffset());
            assertArrayEquals(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7 },
                ProtonBufferUtils.toByteArray(buf));
        }
    }

    @Test
    public void testWriteBytesHeapByteBufferMustThrowIfCannotBeExpanded() {
        // With zero offsets
        try (ProtonBufferAllocator allocator = createTestCaseAllocator();
             ProtonBuffer buf = allocator.allocate(8).implicitGrowthLimit(15)) {
            ByteBuffer source = ByteBuffer.wrap(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7 });
            assertThrows(IndexOutOfBoundsException.class, () -> buf.writeBytes(source));
            assertEquals(8, buf.capacity());
            assertEquals(0, source.position());
            assertEquals(0, buf.getWriteOffset());
            assertEquals(0, buf.getReadOffset());
        }
        // With non-zero offsets
        try (ProtonBufferAllocator allocator = createTestCaseAllocator();
             ProtonBuffer buf = allocator.allocate(8).implicitGrowthLimit(15)) {
            ByteBuffer source = ByteBuffer.wrap(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7 });
            buf.writeByte((byte) -1).readByte();
            assertThrows(IndexOutOfBoundsException.class, () -> buf.writeBytes(source));
            assertEquals(8, buf.capacity());
            assertEquals(0, source.position());
            assertEquals(1, buf.getWriteOffset());
            assertEquals(1, buf.getReadOffset());
        }
    }

    @Test
    public void testWiteBytesDirectByteBufferMustExpandCapacityIfBufferIsTooSmall() {
        // With zero offsets
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            ByteBuffer source = ByteBuffer
                .allocateDirect(16).put(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7 }).flip();
            buf.writeBytes(source);
            assertTrue(buf.capacity() >= source.capacity());
            assertEquals(source.position(), source.limit());
            assertEquals(16, buf.getWriteOffset());
            assertEquals(0, buf.getReadOffset());
            assertArrayEquals(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7 },
                ProtonBufferUtils.toByteArray(buf));
        }
        // With non-zero offsets
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            ByteBuffer source = ByteBuffer
                .allocateDirect(16).put(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7 }).flip();
            buf.writeByte((byte) -1).readByte();
            buf.writeBytes(source);
            assertTrue(buf.capacity() >= source.capacity() + 1);
            assertEquals(source.position(), source.limit());
            assertEquals(17, buf.getWriteOffset());
            assertEquals(1, buf.getReadOffset());
            assertArrayEquals(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7 },
                ProtonBufferUtils.toByteArray(buf));
        }
    }

    @Test
    public void testWriteBytesDirectByteBufferMustThrowIfCannotBeExpanded() {
        // With zero offsets
        try (ProtonBufferAllocator allocator = createTestCaseAllocator();
             ProtonBuffer buf = allocator.allocate(8).implicitGrowthLimit(15)) {
            ByteBuffer source =
                ByteBuffer.allocateDirect(16).put(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7 }).flip();
            assertThrows(IndexOutOfBoundsException.class, () -> buf.writeBytes(source));
            assertEquals(8, buf.capacity());
            assertEquals(0, source.position());
            assertEquals(0, buf.getWriteOffset());
            assertEquals(0, buf.getReadOffset());
        }
        // With non-zero offsets
        try (ProtonBufferAllocator allocator = createTestCaseAllocator();
             ProtonBuffer buf = allocator.allocate(8).implicitGrowthLimit(15)) {
            ByteBuffer source = ByteBuffer
                .allocateDirect(16).put(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7 }).flip();
            buf.writeByte((byte) -1).readByte();
            assertThrows(IndexOutOfBoundsException.class, () -> buf.writeBytes(source));
            assertEquals(8, buf.capacity());
            assertEquals(0, source.position());
            assertEquals(1, buf.getWriteOffset());
            assertEquals(1, buf.getReadOffset());
        }
    }

    @Test
    public void testWriteBytesByteArrayMustExpandCapacityIfTooSmall() {
        // Starting at offsets zero.
        byte[] expected = "0123456789ABCDEF".getBytes(StandardCharsets.US_ASCII);
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(16, expected.length);
            buf.writeBytes(expected);
            assertTrue(buf.capacity() >= expected.length);
            byte[] actual = new byte[expected.length];
            buf.readBytes(actual, 0, actual.length);
            assertArrayEquals(expected, actual);
        }

        // With non-zero start offsets.
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            buf.writeByte((byte) 1).readByte();
            assertEquals(16, expected.length);
            buf.writeBytes(expected);
            assertTrue(buf.capacity() >= expected.length + 1);
            byte[] actual = new byte[expected.length];
            buf.readBytes(actual, 0, actual.length);
            assertArrayEquals(expected, actual);
        }
    }

    @Test
    public void testWriteBytesByteArrayMustThrowIfCannotBeExpanded() {
        // Starting at offsets zero.
        byte[] expected = "0123456789ABCDEF".getBytes(StandardCharsets.US_ASCII);
        try (ProtonBufferAllocator allocator = createTestCaseAllocator();
             ProtonBuffer buf = allocator.allocate(8).implicitGrowthLimit(15)) {
            assertEquals(16, expected.length);
            assertThrows(IndexOutOfBoundsException.class, () -> buf.writeBytes(expected));
            assertEquals(8, buf.capacity());
            assertEquals(0, buf.getReadableBytes());
        }

        // With non-zero start offsets.
        try (ProtonBufferAllocator allocator = createTestCaseAllocator();
             ProtonBuffer buf = allocator.allocate(8).implicitGrowthLimit(15)) {
            buf.writeByte((byte) 1).readByte();
            assertEquals(16, expected.length);
            assertThrows(IndexOutOfBoundsException.class, () -> buf.writeBytes(expected));
            assertEquals(8, buf.capacity());
            assertEquals(0, buf.getReadableBytes());
        }
    }

    @Test
    public void testWriteBytesByteArrayWithOffsetMustExpandCapacityIfTooSmall() {
        // Starting at offsets zero.
        byte[] expected = "0123456789ABCDEF".getBytes(StandardCharsets.US_ASCII);
        try (ProtonBufferAllocator allocator = createTestCaseAllocator();
             ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(16, expected.length);
            buf.writeBytes(expected, 1, expected.length - 1);
            assertTrue(buf.capacity() >= expected.length - 1);
            byte[] actual = new byte[expected.length - 1];
            buf.readBytes(actual, 0, actual.length);
            assertArrayEquals("123456789ABCDEF".getBytes(StandardCharsets.US_ASCII), actual);
        }

        // With non-zero start offsets.
        try (ProtonBufferAllocator allocator = createTestCaseAllocator();
             ProtonBuffer buf = allocator.allocate(8)) {
            buf.writeByte((byte) 1).readByte();
            assertEquals(16, expected.length);
            buf.writeBytes(expected, 1, expected.length - 1);
            assertTrue(buf.capacity() >= expected.length);
            byte[] actual = new byte[expected.length - 1];
            buf.readBytes(actual, 0, actual.length);
            assertArrayEquals("123456789ABCDEF".getBytes(StandardCharsets.US_ASCII), actual);
        }
    }

    @Test
    public void testWriteBytesByteArrayWithOffsetMustThrowIfCannotBeExpanded() {
        // Starting at offsets zero.
        byte[] expected = "0123456789ABCDEF".getBytes(StandardCharsets.US_ASCII);
        try (ProtonBufferAllocator allocator = createTestCaseAllocator();
             ProtonBuffer buf = allocator.allocate(8).implicitGrowthLimit(14)) {
            assertEquals(16, expected.length);
            assertThrows(IndexOutOfBoundsException.class, () -> buf.writeBytes(expected, 1, expected.length - 1));
            assertEquals(8, buf.capacity());
            assertEquals(0, buf.getReadableBytes());
        }

        // With non-zero start offsets.
        try (ProtonBufferAllocator allocator = createTestCaseAllocator();
             ProtonBuffer buf = allocator.allocate(8).implicitGrowthLimit(14)) {
            buf.writeByte((byte) 1).readByte();
            assertEquals(16, expected.length);
            assertThrows(IndexOutOfBoundsException.class, () -> buf.writeBytes(expected, 1, expected.length - 1));
            assertEquals(8, buf.capacity());
            assertEquals(0, buf.getReadableBytes());
        }
    }

    @Test
    public void testWriteBytesBufferMustExpandCapacityIfTooSmall() {
        // Starting at offsets zero.
        byte[] expectedByteArray = "0123456789ABCDEF".getBytes(StandardCharsets.US_ASCII);
        try (ProtonBufferAllocator allocator = createTestCaseAllocator();
             ProtonBuffer buf = allocator.allocate(8);
             ProtonBuffer expected = allocator.copy(expectedByteArray)) {
            assertEquals(16, expected.getReadableBytes());
            buf.writeBytes(expected);
            assertTrue(buf.capacity() >= expectedByteArray.length);
            byte[] actual = new byte[expectedByteArray.length];
            buf.readBytes(actual, 0, actual.length);
            assertArrayEquals(expectedByteArray, actual);
        }

        // With non-zero start offsets.
        try (ProtonBufferAllocator allocator = createTestCaseAllocator();
             ProtonBuffer buf = allocator.allocate(8);
             ProtonBuffer expected = allocator.copy(expectedByteArray)) {
            buf.writeByte((byte) 1).readByte();
            assertEquals(16, expected.getReadableBytes());
            buf.writeBytes(expected);
            assertTrue(buf.capacity() >= expectedByteArray.length + 1);
            byte[] actual = new byte[expectedByteArray.length];
            buf.readBytes(actual, 0, actual.length);
            assertArrayEquals(expectedByteArray, actual);
        }
    }

    @Test
    public void testWriteBytesBufferMustThrowIfCannotBeExpanded() {
        // Starting at offsets zero.
        byte[] expectedByteArray = "0123456789ABCDEF".getBytes(StandardCharsets.US_ASCII);
        try (ProtonBufferAllocator allocator = createTestCaseAllocator();
             ProtonBuffer buf = allocator.allocate(8).implicitGrowthLimit(15);
             ProtonBuffer expected = allocator.copy(expectedByteArray)) {
            assertEquals(16, expected.getReadableBytes());
            assertThrows(IndexOutOfBoundsException.class, () -> buf.writeBytes(expected));
            assertEquals(8, buf.capacity());
            assertEquals(0, buf.getReadableBytes());
        }

        // With non-zero start offsets.
        try (ProtonBufferAllocator allocator = createTestCaseAllocator();
             ProtonBuffer buf = allocator.allocate(8).implicitGrowthLimit(15);
             ProtonBuffer expected = allocator.copy(expectedByteArray)) {
            buf.writeByte((byte) 1).readByte();
            assertEquals(16, expected.getReadableBytes());
            assertThrows(IndexOutOfBoundsException.class, () -> buf.writeBytes(expected));
            assertEquals(8, buf.capacity());
            assertEquals(0, buf.getReadableBytes());
        }
    }

    @Test
    public void testReadBytesIntoHeapByteBuffer() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator();
             ProtonBuffer buf = allocator.allocate(8)) {
            buf.writeLong(0x0102030405060708L);
            ByteBuffer dest = ByteBuffer.allocate(4);
            assertEquals(1, buf.readByte());
            assertEquals(2, buf.readByte());
            buf.readBytes(dest);
            assertEquals(dest.position(), dest.limit());
            assertEquals(8, buf.getWriteOffset());
            assertEquals(6, buf.getReadOffset());
            assertArrayEquals(new byte[] { 0x03, 0x04, 0x05, 0x06 }, ProtonBufferUtils.toByteArray(dest.flip()));
        }
    }

    @Test
    public void testReadBytesIntoDirectByteBuffer() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator();
             ProtonBuffer buf = allocator.allocate(8)) {
            buf.writeLong(0x0102030405060708L);
            ByteBuffer dest = ByteBuffer.allocateDirect(4);
            assertEquals(1, buf.readByte());
            assertEquals(2, buf.readByte());
            buf.readBytes(dest);
            assertEquals(dest.position(), dest.limit());
            assertEquals(8, buf.getWriteOffset());
            assertEquals(6, buf.getReadOffset());
            assertArrayEquals(new byte[] { 0x03, 0x04, 0x05, 0x06 }, ProtonBufferUtils.toByteArray(dest.flip()));
        }
    }

    @Test
    public void testWriteBytesMustTransferDataAndUpdateOffsets() {
        try (ProtonBufferAllocator protonAlloc = createProtonDefaultAllocator();
             ProtonBufferAllocator allocator = createTestCaseAllocator();
             ProtonBuffer target = protonAlloc.allocate(37);
             ProtonBuffer source = allocator.allocate(35)) {

            for (int i = 0; i < 35; i++) {
                source.writeByte((byte) (i + 1));
            }
            target.writeBytes(source);
            assertEquals(0, target.getReadOffset());
            assertEquals(35, target.getWriteOffset());
            assertEquals(35, source.getReadOffset());
            assertEquals(35, source.getWriteOffset());

            source.setReadOffset(0);
            assertEquals(source, target);
        }
    }

    @Test
    public void testOffsettedGetOfCharMustBoundsCheckOnNegativeOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertThrows(IndexOutOfBoundsException.class, () -> buf.getChar(-1));
        }
    }

    @Test
    public void testOffsettedGetOfCharReadOnlyMustBoundsCheckOnNegativeOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertThrows(IndexOutOfBoundsException.class, () -> buf.convertToReadOnly().getChar(-1));
        }
    }

    @Test
    public void testOffsettedGetOfCharMustNotBoundsCheckWhenReadOffsetAndSizeIsEqualToWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            char value = 0x0102;
            buf.writeChar(value);
            assertEquals(value, buf.getChar(0));
        }
    }

    @Test
    public void testOffsettedGetOfCharMustReadWithDefaultEndianByteOrder() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            char value = 0x0102;
            buf.writeChar(value);
            buf.setByte(0, (byte) 0x10);
            assertEquals(0x1002, buf.getChar(0));
        }
    }

    @Test
    public void testOffsettedGetOfCharMustNotBoundsCheckWhenReadOffsetAndSizeIsGreaterThanWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            char value = 0x0102;
            buf.writeChar(value);
            buf.getChar(1);
        }
    }

    @Test
    public void testOffsettedGetOfCharMustBoundsCheckWhenReadOffsetAndSizeIsGreaterThanCapacity() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertThrows(IndexOutOfBoundsException.class, () -> buf.getChar(7));
        }
    }

    @Test
    public void testOffsettedGetOfCharReadOnlyMustNotBoundsCheckWhenReadOffsetAndSizeIsGreaterThanWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            char value = 0x0102;
            buf.writeChar(value);
            buf.convertToReadOnly().getChar(1);
        }
    }

    @Test
    public void testOffsettedGetOfCharReadOnlyMustBoundsCheckWhenReadOffsetAndSizeIsGreaterThanCapacity() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertThrows(IndexOutOfBoundsException.class, () -> buf.convertToReadOnly().getChar(7));
        }
    }

    @Test
    public void testOffsettedGetOfCharMustNotBoundsCheckWhenReadOffsetIsGreaterThanWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            buf.getChar(0);
        }
    }

    @Test
    public void testOffsettedGetOfCharMustBoundsCheckWhenReadOffsetIsGreaterThanCapacity() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertThrows(IndexOutOfBoundsException.class, () -> buf.getChar(8));
        }
    }

    @Test
    public void testOffsettedGetOfCharReadOnlyMustNotBoundsCheckWhenReadOffsetIsGreaterThanWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            buf.convertToReadOnly().getChar(0);
        }
    }

    @Test
    public void testOffsettedGetOfCharReadOnlyMustBoundsCheckWhenReadOffsetIsGreaterThanCapacity() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertThrows(IndexOutOfBoundsException.class, () -> buf.convertToReadOnly().getChar(8));
        }
    }

    @Test
    public void testOffsettedSetOfCharMustBoundsCheckWhenWriteOffsetIsNegative() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(Long.BYTES, buf.capacity());
            char value = 0x0102;
            assertThrows(IndexOutOfBoundsException.class, () -> buf.setChar(-1, value));
            buf.setWriteOffset(Long.BYTES);
            // Verify contents are unchanged.
            assertEquals(0, buf.readLong());
        }
    }

    @Test
    public void testOffsettedSetOfCharMustBoundsCheckWhenWriteOffsetAndSizeIsBeyondCapacity() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(Long.BYTES, buf.capacity());
            char value = 0x0102;
            assertThrows(IndexOutOfBoundsException.class, () -> buf.setChar(7, value));
            buf.setWriteOffset(Long.BYTES);
            // Verify contents are unchanged.
            assertEquals(0, buf.readLong());
        }
    }

    @Test
    public void testOffsettedSetOfCharMustHaveDefaultEndianByteOrder() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            char value = 0x0102;
            buf.setChar(0, value);
            buf.setWriteOffset(Long.BYTES);
            assertEquals((byte) 0x01, buf.readByte());
            assertEquals((byte) 0x02, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
        }
    }

    @Test
    public void testOffsettedGetOfDoubleMustBoundsCheckOnNegativeOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertThrows(IndexOutOfBoundsException.class, () -> buf.getDouble(-1));
        }
    }

    @Test
    public void testOffsettedGetOfDoubleReadOnlyMustBoundsCheckOnNegativeOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertThrows(IndexOutOfBoundsException.class, () -> buf.convertToReadOnly().getDouble(-1));
        }
    }

    @Test
    public void testOffsettedGetOfDoubleMustNotBoundsCheckWhenReadOffsetAndSizeIsEqualToWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            double value = Double.longBitsToDouble(0x0102030405060708L);
            buf.writeDouble(value);
            assertEquals(value, buf.getDouble(0));
        }
    }

    @Test
    public void testOffsettedGetOfDoubleMustReadWithDefaultEndianByteOrder() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            double value = Double.longBitsToDouble(0x0102030405060708L);
            buf.writeDouble(value);
            buf.setByte(0, (byte) 0x10);
            assertEquals(Double.longBitsToDouble(0x1002030405060708L), buf.getDouble(0));
        }
    }

    @Test
    public void testOffsettedGetOfDoubleMustBoundsCheckWhenReadOffsetAndSizeIsGreaterThanWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            double value = Double.longBitsToDouble(0x0102030405060708L);
            buf.writeDouble(value);
            assertThrows(IndexOutOfBoundsException.class, () -> buf.getDouble(1));
        }
    }

    @Test
    public void testOffsettedGetOfDoubleReadOnlyMustBoundsCheckWhenReadOffsetAndSizeIsGreaterThanWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            double value = Double.longBitsToDouble(0x0102030405060708L);
            buf.writeDouble(value);
            assertThrows(IndexOutOfBoundsException.class, () -> buf.convertToReadOnly().getDouble(1));
        }
    }

    @Test
    public void testOffsettedGetOfDoubleMustNotBoundsCheckWhenReadOffsetIsGreaterThanWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            buf.getDouble(0);
        }
    }

    @Test
    public void testOffsettedGetOfDoubleMustBoundsCheckWhenReadOffsetIsGreaterThanCapacity() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertThrows(IndexOutOfBoundsException.class, () -> buf.getDouble(8));
        }
    }

    @Test
    public void testOffsettedGetOfDoubleReadOnlyMustNotBoundsCheckWhenReadOffsetIsGreaterThanWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            buf.convertToReadOnly().getDouble(0);
        }
    }

    @Test
    public void testOffsettedGetOfDoubleReadOnlyMustBoundsCheckWhenReadOffsetIsGreaterThanCapacity() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertThrows(IndexOutOfBoundsException.class, () -> buf.convertToReadOnly().getDouble(8));
        }
    }

    @Test
    public void testOffsettedSetOfDoubleMustBoundsCheckWhenWriteOffsetIsNegative() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(Long.BYTES, buf.capacity());
            double value = Double.longBitsToDouble(0x0102030405060708L);
            assertThrows(IndexOutOfBoundsException.class, () -> buf.setDouble(-1, value));
            buf.setWriteOffset(Long.BYTES);
            // Verify contents are unchanged.
            assertEquals(0, buf.readLong());
        }
    }

    @Test
    public void testOffsettedSetOfDoubleMustBoundsCheckWhenWriteOffsetAndSizeIsBeyondCapacity() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(Long.BYTES, buf.capacity());
            double value = Double.longBitsToDouble(0x0102030405060708L);
            assertThrows(IndexOutOfBoundsException.class, () -> buf.setDouble(1, value));
            buf.setWriteOffset(Long.BYTES);
            // Verify contents are unchanged.
            assertEquals(0, buf.readLong());
        }
    }

    @Test
    public void testOffsettedSetOfDoubleMustHaveDefaultEndianByteOrder() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            double value = Double.longBitsToDouble(0x0102030405060708L);
            buf.setDouble(0, value);
            buf.setWriteOffset(Long.BYTES);
            assertEquals((byte) 0x01, buf.readByte());
            assertEquals((byte) 0x02, buf.readByte());
            assertEquals((byte) 0x03, buf.readByte());
            assertEquals((byte) 0x04, buf.readByte());
            assertEquals((byte) 0x05, buf.readByte());
            assertEquals((byte) 0x06, buf.readByte());
            assertEquals((byte) 0x07, buf.readByte());
            assertEquals((byte) 0x08, buf.readByte());
        }
    }

    @Test
    public void testOffsettedGetOfFloatMustBoundsCheckOnNegativeOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertThrows(IndexOutOfBoundsException.class, () -> buf.getFloat(-1));
        }
    }

    @Test
    public void testOffsettedGetOfFloatReadOnlyMustBoundsCheckOnNegativeOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertThrows(IndexOutOfBoundsException.class, () -> buf.convertToReadOnly().getFloat(-1));
        }
    }

    @Test
    public void testOffsettedGetOfFloatMustNotBoundsCheckWhenReadOffsetAndSizeIsEqualToWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            float value = Float.intBitsToFloat(0x01020304);
            buf.writeFloat(value);
            assertEquals(value, buf.getFloat(0));
        }
    }

    @Test
    public void testOffsettedGetOfFloatMustReadWithDefaultEndianByteOrder() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            float value = Float.intBitsToFloat(0x01020304);
            buf.writeFloat(value);
            buf.setByte(0, (byte) 0x10);
            assertEquals(Float.intBitsToFloat(0x10020304), buf.getFloat(0));
        }
    }

    @Test
    public void testOffsettedGetOfFloatMustNotBoundsCheckWhenReadOffsetAndSizeIsGreaterThanWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            float value = Float.intBitsToFloat(0x01020304);
            buf.writeFloat(value);
            buf.getFloat(1);
        }
    }

    @Test
    public void testOffsettedGetOfFloatMustBoundsCheckWhenReadOffsetAndSizeIsGreaterThanCapacity() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertThrows(IndexOutOfBoundsException.class, () -> buf.getFloat(7));
        }
    }

    @Test
    public void testOffsettedGetOfFloatReadOnlyMustNotBoundsCheckWhenReadOffsetAndSizeIsGreaterThanWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            float value = Float.intBitsToFloat(0x01020304);
            buf.writeFloat(value);
            buf.convertToReadOnly().getFloat(1);
        }
    }

    @Test
    public void testOffsettedGetOfFloatReadOnlyMustBoundsCheckWhenReadOffsetAndSizeIsGreaterThanCapacity() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertThrows(IndexOutOfBoundsException.class, () -> buf.convertToReadOnly().getFloat(5));
        }
    }

    @Test
    public void testOffsettedGetOfFloatMustNotBoundsCheckWhenReadOffsetIsGreaterThanWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            buf.getFloat(0);
        }
    }

    @Test
    public void testOffsettedGetOfFloatMustBoundsCheckWhenReadOffsetIsGreaterThanCapacity() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertThrows(IndexOutOfBoundsException.class, () -> buf.getFloat(8));
        }
    }

    @Test
    public void testOffsettedGetOfFloatReadOnlyMustNotBoundsCheckWhenReadOffsetIsGreaterThanWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            buf.convertToReadOnly().getFloat(0);
        }
    }

    @Test
    public void testOffsettedGetOfFloatReadOnlyMustBoundsCheckWhenReadOffsetIsGreaterThan() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertThrows(IndexOutOfBoundsException.class, () -> buf.convertToReadOnly().getFloat(8));
        }
    }

    @Test
    public void testOffsettedSetOfFloatMustBoundsCheckWhenWriteOffsetIsNegative() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(Long.BYTES, buf.capacity());
            float value = Float.intBitsToFloat(0x01020304);
            assertThrows(IndexOutOfBoundsException.class, () -> buf.setFloat(-1, value));
            buf.setWriteOffset(Long.BYTES);
            // Verify contents are unchanged.
            assertEquals(0, buf.readLong());
        }
    }

    @Test
    public void testOffsettedSetOfFloatMustBoundsCheckWhenWriteOffsetAndSizeIsBeyondCapacity() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(Long.BYTES, buf.capacity());
            float value = Float.intBitsToFloat(0x01020304);
            assertThrows(IndexOutOfBoundsException.class, () -> buf.setFloat(5, value));
            buf.setWriteOffset(Long.BYTES);
            // Verify contents are unchanged.
            assertEquals(0, buf.readLong());
        }
    }

    @Test
    public void testOffsettedSetOfFloatMustHaveDefaultEndianByteOrder() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            float value = Float.intBitsToFloat(0x01020304);
            buf.setFloat(0, value);
            buf.setWriteOffset(Long.BYTES);
            assertEquals((byte) 0x01, buf.readByte());
            assertEquals((byte) 0x02, buf.readByte());
            assertEquals((byte) 0x03, buf.readByte());
            assertEquals((byte) 0x04, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
        }
    }

    @Test
    public void testOffsettedGetOfIntMustBoundsCheckOnNegativeOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertThrows(IndexOutOfBoundsException.class, () -> buf.getInt(-1));
        }
    }

    @Test
    public void testOffsettedGetOfIntReadOnlyMustBoundsCheckOnNegativeOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertThrows(IndexOutOfBoundsException.class, () -> buf.convertToReadOnly().getInt(-1));
        }
    }

    @Test
    public void testOffsettedGetOfIntMustNotBoundsCheckWhenReadOffsetAndSizeIsEqualToWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            int value = 0x01020304;
            buf.writeInt(value);
            assertEquals(value, buf.getInt(0));
        }
    }

    @Test
    public void testOffsettedGetOfIntMustReadWithDefaultEndianByteOrder() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            int value = 0x01020304;
            buf.writeInt(value);
            buf.setByte(0, (byte) 0x10);
            assertEquals(0x10020304, buf.getInt(0));
        }
    }

    @Test
    public void testOffsettedGetOfIntMustBoundsCheckWhenReadOffsetAndSizeIsGreaterThanWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            int value = 0x01020304;
            buf.writeInt(value);
            assertThrows(IndexOutOfBoundsException.class, () -> buf.getInt(-1));
        }
    }

    @Test
    public void testOffsettedGetOfIntReadOnlyMustBoundsCheckWhenReadOffsetAndSizeIsGreaterThanWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            int value = 0x01020304;
            buf.writeInt(value);
            assertThrows(IndexOutOfBoundsException.class, () -> buf.convertToReadOnly().getInt(-1));
        }
    }

    @Test
    public void testOffsettedGetOfIntMustNotBoundsCheckWhenReadOffsetIsGreaterThanWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            buf.getInt(0);
        }
    }

    @Test
    public void testOffsettedGetOfIntMustBoundsCheckWhenReadOffsetIsGreaterThanCapacity() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertThrows(IndexOutOfBoundsException.class, () -> buf.getInt(8));
        }
    }

    @Test
    public void testOffsettedGetOfIntReadOnlyMustNotBoundsCheckWhenReadOffsetIsGreaterThanWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            buf.convertToReadOnly().getInt(0);
        }
    }

    @Test
    public void testOffsettedGetOfIntReadOnlyMustBoundsCheckWhenReadOffsetIsGreaterThanCapacity() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertThrows(IndexOutOfBoundsException.class, () -> buf.convertToReadOnly().getInt(8));
        }
    }

    @Test
    public void testOffsettedGetOfUnsignedIntMustBoundsCheckOnNegativeOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertThrows(IndexOutOfBoundsException.class, () -> buf.getUnsignedInt(-1));
        }
    }

    @Test
    public void testOffsettedGetOfUnsignedIntReadOnlyMustBoundsCheckOnNegativeOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertThrows(IndexOutOfBoundsException.class, () -> buf.convertToReadOnly().getUnsignedInt(-1));
        }
    }

    @Test
    public void testOffsettedGetOfUnsignedIntMustNotBoundsCheckWhenReadOffsetAndSizeIsEqualToWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            long value = 0x01020304;
            buf.writeUnsignedInt(value);
            assertEquals(value, buf.getUnsignedInt(0));
        }
    }

    @Test
    public void testOffsettedGetOfUnsignedIntMustReadWithDefaultEndianByteOrder() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            long value = 0x01020304;
            buf.writeUnsignedInt(value);
            buf.setByte(0, (byte) 0x10);
            assertEquals(0x10020304, buf.getUnsignedInt(0));
        }
    }

    @Test
    public void testOffsettedGetOfUnsignedIntMustNotBoundsCheckWhenReadOffsetAndSizeIsGreaterThanWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            long value = 0x01020304;
            buf.writeUnsignedInt(value);
            buf.getUnsignedInt(1);
        }
    }

    @Test
    public void testOffsettedGetOfUnsignedIntMustBoundsCheckWhenReadOffsetAndSizeIsGreaterThanCapacity() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertThrows(IndexOutOfBoundsException.class, () -> buf.getUnsignedInt(5));
        }
    }

    @Test
    public void testOffsettedGetOfUnsignedIntReadOnlyMustNotBoundsCheckWhenReadOffsetAndSizeIsGreaterThanWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            long value = 0x01020304;
            buf.writeUnsignedInt(value);
            buf.convertToReadOnly().getUnsignedInt(1);
        }
    }

    @Test
    public void testOffsettedGetOfUnsignedIntReadOnlyMustBoundsCheckWhenReadOffsetAndSizeIsGreaterThanCapacity() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertThrows(IndexOutOfBoundsException.class, () -> buf.convertToReadOnly().getUnsignedInt(5));
        }
    }

    @Test
    public void testOffsettedGetOfUnsignedIntMustNotBoundsCheckWhenReadOffsetIsGreaterThanWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            buf.getUnsignedInt(0);
        }
    }

    @Test
    public void testOffsettedGetOfUnsignedIntMustBoundsCheckWhenReadOffsetIsGreaterThanCapacity() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertThrows(IndexOutOfBoundsException.class, () -> buf.getUnsignedInt(8));
        }
    }

    @Test
    public void testOffsettedGetOfUnsignedIntReadOnlyMustNotBoundsCheckWhenReadOffsetIsGreaterThanWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            buf.convertToReadOnly().getUnsignedInt(0);
        }
    }

    @Test
    public void testOffsettedGetOfUnsignedIntReadOnlyMustBoundsCheckWhenReadOffsetIsGreaterThanCapacity() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertThrows(IndexOutOfBoundsException.class, () -> buf.convertToReadOnly().getUnsignedInt(8));
        }
    }

    @Test
    public void testOffsettedSetOfIntMustBoundsCheckWhenWriteOffsetIsNegative() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(Long.BYTES, buf.capacity());
            int value = 0x01020304;
            assertThrows(IndexOutOfBoundsException.class, () -> buf.setInt(-1, value));
            buf.setWriteOffset(Long.BYTES);
            // Verify contents are unchanged.
            assertEquals(0, buf.readLong());
        }
    }

    @Test
    public void testOffsettedSetOfIntMustBoundsCheckWhenWriteOffsetAndSizeIsBeyondCapacity() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(Long.BYTES, buf.capacity());
            int value = 0x01020304;
            assertThrows(IndexOutOfBoundsException.class, () -> buf.setInt(5, value));
            buf.setWriteOffset(Long.BYTES);
            // Verify contents are unchanged.
            assertEquals(0, buf.readLong());
        }
    }

    @Test
    public void testOffsettedSetOfIntMustHaveDefaultEndianByteOrder() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            int value = 0x01020304;
            buf.setInt(0, value);
            buf.setWriteOffset(Long.BYTES);
            assertEquals((byte) 0x01, buf.readByte());
            assertEquals((byte) 0x02, buf.readByte());
            assertEquals((byte) 0x03, buf.readByte());
            assertEquals((byte) 0x04, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
        }
    }

    @Test
    public void testOffsettedSetOfUnsignedIntMustBoundsCheckWhenWriteOffsetIsNegative() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(Long.BYTES, buf.capacity());
            long value = 0x01020304;
            assertThrows(IndexOutOfBoundsException.class, () -> buf.setUnsignedInt(-1, value));
            buf.setWriteOffset(Long.BYTES);
            // Verify contents are unchanged.
            assertEquals(0, buf.readLong());
        }
    }

    @Test
    public void testOffsettedSetOfUnsignedIntMustBoundsCheckWhenWriteOffsetAndSizeIsBeyondCapacity() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(Long.BYTES, buf.capacity());
            long value = 0x01020304;
            assertThrows(IndexOutOfBoundsException.class, () -> buf.setUnsignedInt(5, value));
            buf.setWriteOffset(Long.BYTES);
            // Verify contents are unchanged.
            assertEquals(0, buf.readLong());
        }
    }

    @Test
    public void testOffsettedSetOfUnsignedIntMustHaveDefaultEndianByteOrder() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            long value = 0x01020304;
            buf.setUnsignedInt(0, value);
            buf.setWriteOffset(Long.BYTES);
            assertEquals((byte) 0x01, buf.readByte());
            assertEquals((byte) 0x02, buf.readByte());
            assertEquals((byte) 0x03, buf.readByte());
            assertEquals((byte) 0x04, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
        }
    }

    @Test
    public void testOffsettedGetOfLongMustBoundsCheckOnNegativeOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertThrows(IndexOutOfBoundsException.class, () -> buf.getLong(-1));
        }
    }

    @Test
    public void testOffsettedGetOfLongReadOnlyMustBoundsCheckOnNegativeOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertThrows(IndexOutOfBoundsException.class, () -> buf.convertToReadOnly().getLong(-1));
        }
    }

    @Test
    public void testOffsettedGetOfLongMustNotBoundsCheckWhenReadOffsetAndSizeIsEqualToWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            long value = 0x0102030405060708L;
            buf.writeLong(value);
            assertEquals(value, buf.getLong(0));
        }
    }

    @Test
    public void testOffsettedGetOfLongMustReadWithDefaultEndianByteOrder() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            long value = 0x0102030405060708L;
            buf.writeLong(value);
            buf.setByte(0, (byte) 0x10);
            assertEquals(0x1002030405060708L, buf.getLong(0));
        }
    }

    @Test
    public void testOffsettedGetOfLongMustBoundsCheckWhenReadOffsetAndSizeIsGreaterThanWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            long value = 0x0102030405060708L;
            buf.writeLong(value);
            assertThrows(IndexOutOfBoundsException.class, () -> buf.getLong(1));
        }
    }

    @Test
    public void testOffsettedGetOfLongReadOnlyMustBoundsCheckWhenReadOffsetAndSizeIsGreaterThanWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            long value = 0x0102030405060708L;
            buf.writeLong(value);
            assertThrows(IndexOutOfBoundsException.class, () -> buf.convertToReadOnly().getLong(1));
        }
    }

    @Test
    public void testOffsettedGetOfLongMustNotBoundsCheckWhenReadOffsetIsGreaterThanWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            buf.getLong(0);
        }
    }

    @Test
    public void testOffsettedGetOfLongMustBoundsCheckWhenReadOffsetIsGreaterThanCapacity() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertThrows(IndexOutOfBoundsException.class, () -> buf.getLong(8));
        }
    }

    @Test
    public void testOffsettedGetOfLongReadOnlyMustNotBoundsCheckWhenReadOffsetIsGreaterThanWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            buf.convertToReadOnly().getLong(0);
        }
    }

    @Test
    public void testOffsettedGetOfLongReadOnlyMustBoundsCheckWhenReadOffsetIsGreaterThanCapacity() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertThrows(IndexOutOfBoundsException.class, () -> buf.convertToReadOnly().getLong(8));
        }
    }

    @Test
    public void testOffsettedSetOfLongMustBoundsCheckWhenWriteOffsetIsNegative() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(Long.BYTES, buf.capacity());
            long value = 0x0102030405060708L;
            assertThrows(IndexOutOfBoundsException.class, () -> buf.setLong(-1, value));
            buf.setWriteOffset(Long.BYTES);
            // Verify contents are unchanged.
            assertEquals(0, buf.readLong());
        }
    }

    @Test
    public void testOffsettedSetOfLongMustBoundsCheckWhenWriteOffsetAndSizeIsBeyondCapacity() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(Long.BYTES, buf.capacity());
            long value = 0x0102030405060708L;
            assertThrows(IndexOutOfBoundsException.class, () -> buf.setLong(1, value));
            buf.setWriteOffset(Long.BYTES);
            // Verify contents are unchanged.
            assertEquals(0, buf.readLong());
        }
    }

    @Test
    public void testOffsettedSetOfLongMustHaveDefaultEndianByteOrder() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            long value = 0x0102030405060708L;
            buf.setLong(0, value);
            buf.setWriteOffset(Long.BYTES);
            assertEquals((byte) 0x01, buf.readByte());
            assertEquals((byte) 0x02, buf.readByte());
            assertEquals((byte) 0x03, buf.readByte());
            assertEquals((byte) 0x04, buf.readByte());
            assertEquals((byte) 0x05, buf.readByte());
            assertEquals((byte) 0x06, buf.readByte());
            assertEquals((byte) 0x07, buf.readByte());
            assertEquals((byte) 0x08, buf.readByte());
        }
    }

    @Test
    public void testOffsettedGetOfShortMustBoundsCheckOnNegativeOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertThrows(IndexOutOfBoundsException.class, () -> buf.getShort(-1));
        }
    }

    @Test
    public void testOffsettedGetOfShortReadOnlyMustBoundsCheckOnNegativeOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertThrows(IndexOutOfBoundsException.class, () -> buf.convertToReadOnly().getShort(-1));
        }
    }

    @Test
    public void testOffsettedGetOfShortMustNotBoundsCheckWhenReadOffsetAndSizeIsEqualToWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            short value = 0x0102;
            buf.writeShort(value);
            assertEquals(value, buf.getShort(0));
        }
    }

    @Test
    public void testOffsettedGetOfShortMustReadWithDefaultEndianByteOrder() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            short value = 0x0102;
            buf.writeShort(value);
            buf.setByte(0, (byte) 0x10);
            assertEquals(0x1002, buf.getShort(0));
        }
    }

    @Test
    public void testOffsettedGetOfShortMustNotBoundsCheckWhenReadOffsetAndSizeIsGreaterThanWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            short value = 0x0102;
            buf.writeShort(value);
            buf.getShort(1);
        }
    }

    @Test
    public void testOffsettedGetOfShortMustBoundsCheckWhenReadOffsetAndSizeIsGreaterThanCapacity() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertThrows(IndexOutOfBoundsException.class, () -> buf.getShort(7));
        }
    }

    @Test
    public void testOffsettedGetOfShortReadOnlyMustNotBoundsCheckWhenReadOffsetAndSizeIsGreaterThanWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            short value = 0x0102;
            buf.writeShort(value);
            buf.convertToReadOnly().getShort(1);
        }
    }

    @Test
    public void testOffsettedGetOfShortReadOnlyMustBoundsCheckWhenReadOffsetAndSizeIsGreaterThanCapacity() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertThrows(IndexOutOfBoundsException.class, () -> buf.convertToReadOnly().getShort(7));
        }
    }

    @Test
    public void testOffsettedGetOfShortMustNotBoundsCheckWhenReadOffsetIsGreaterThanWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            buf.getShort(0);
        }
    }

    @Test
    public void testOffsettedGetOfShortMustBoundsCheckWhenReadOffsetIsGreaterThanCapacity() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertThrows(IndexOutOfBoundsException.class, () -> buf.getShort(7));
        }
    }

    @Test
    public void testOffsettedGetOfShortReadOnlyMustNotBoundsCheckWhenReadOffsetIsGreaterThanWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            buf.convertToReadOnly().getShort(0);
        }
    }

    @Test
    public void testOffsettedGetOfShortReadOnlyMustBoundsCheckWhenReadOffsetIsGreaterThanCapacity() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertThrows(IndexOutOfBoundsException.class, () -> buf.convertToReadOnly().getShort(7));
        }
    }

    @Test
    public void testOffsettedGetOfUnsignedShortMustBoundsCheckOnNegativeOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertThrows(IndexOutOfBoundsException.class, () -> buf.getUnsignedShort(-1));
        }
    }

    @Test
    public void testOffsettedGetOfUnsignedShortReadOnlyMustBoundsCheckOnNegativeOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertThrows(IndexOutOfBoundsException.class, () -> buf.convertToReadOnly().getUnsignedShort(-1));
        }
    }

    @Test
    public void testOffsettedGetOfUnsignedShortMustNotBoundsCheckWhenReadOffsetAndSizeIsEqualToWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            int value = 0x0102;
            buf.writeUnsignedShort(value);
            assertEquals(value, buf.getUnsignedShort(0));
        }
    }

    @Test
    public void testOffsettedGetOfUnsignedShortMustReadWithDefaultEndianByteOrder() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            int value = 0x0102;
            buf.writeUnsignedShort(value);
            buf.setByte(0, (byte) 0x10);
            assertEquals(0x1002, buf.getUnsignedShort(0));
        }
    }

    @Test
    public void testOffsettedGetOfUnsignedShortMustNotBoundsCheckWhenReadOffsetAndSizeIsGreaterThanWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            int value = 0x0102;
            buf.writeUnsignedShort(value);
            buf.getUnsignedShort(1);
        }
    }

    @Test
    public void testOffsettedGetOfUnsignedShortMustBoundsCheckWhenReadOffsetAndSizeIsGreaterThanCapacity() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertThrows(IndexOutOfBoundsException.class, () -> buf.getUnsignedShort(7));
        }
    }

    @Test
    public void testOffsettedGetOfUnsignedShortReadOnlyMustNotBoundsCheckWhenReadOffsetAndSizeIsGreaterThanWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            int value = 0x0102;
            buf.writeUnsignedShort(value);
            buf.convertToReadOnly().getUnsignedShort(1);
        }
    }

    @Test
    public void testOffsettedGetOfUnsignedShortReadOnlyMustBoundsCheckWhenReadOffsetAndSizeIsGreaterThanCapacity() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertThrows(IndexOutOfBoundsException.class, () -> buf.convertToReadOnly().getUnsignedShort(7));
        }
    }

    @Test
    public void testOffsettedGetOfUnsignedShortMustNotBoundsCheckWhenReadOffsetIsGreaterThanWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            buf.getUnsignedShort(0);
        }
    }

    @Test
    public void testOffsettedGetOfUnsignedShortMustBoundsCheckWhenReadOffsetIsGreaterThanCapacity() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertThrows(IndexOutOfBoundsException.class, () -> buf.getUnsignedShort(7));
        }
    }

    @Test
    public void testOffsettedGetOfUnsignedShortReadOnlyMustNotBoundsCheckWhenReadOffsetIsGreaterThanWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            buf.convertToReadOnly().getUnsignedShort(0);
        }
    }

    @Test
    public void testOffsettedGetOfUnsignedShortReadOnlyMustBoundsCheckWhenReadOffsetIsGreaterThanCapacity() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertThrows(IndexOutOfBoundsException.class, () -> buf.convertToReadOnly().getUnsignedShort(7));
        }
    }

    @Test
    public void testOffsettedSetOfShortMustBoundsCheckWhenWriteOffsetIsNegative() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(Long.BYTES, buf.capacity());
            short value = 0x0102;
            assertThrows(IndexOutOfBoundsException.class, () -> buf.setShort(-1, value));
            buf.setWriteOffset(Long.BYTES);
            // Verify contents are unchanged.
            assertEquals(0, buf.readLong());
        }
    }

    @Test
    public void testOffsettedSetOfShortMustBoundsCheckWhenWriteOffsetAndSizeIsBeyondCapacity() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(Long.BYTES, buf.capacity());
            short value = 0x0102;
            assertThrows(IndexOutOfBoundsException.class, () -> buf.setShort(7, value));
            buf.setWriteOffset(Long.BYTES);
            // Verify contents are unchanged.
            assertEquals(0, buf.readLong());
        }
    }

    @Test
    public void testOffsettedSetOfShortMustHaveDefaultEndianByteOrder() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            short value = 0x0102;
            buf.setShort(0, value);
            buf.setWriteOffset(Long.BYTES);
            assertEquals((byte) 0x01, buf.readByte());
            assertEquals((byte) 0x02, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
        }
    }

    @Test
    public void testOffsettedSetOfUnsignedShortMustBoundsCheckWhenWriteOffsetIsNegative() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(Long.BYTES, buf.capacity());
            int value = 0x0102;
            assertThrows(IndexOutOfBoundsException.class, () -> buf.setUnsignedShort(-1, value));
            buf.setWriteOffset(Long.BYTES);
            // Verify contents are unchanged.
            assertEquals(0, buf.readLong());
        }
    }

    @Test
    public void testOffsettedSetOfUnsignedShortMustBoundsCheckWhenWriteOffsetAndSizeIsBeyondCapacity() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(Long.BYTES, buf.capacity());
            int value = 0x0102;
            assertThrows(IndexOutOfBoundsException.class, () -> buf.setUnsignedShort(7, value));
            buf.setWriteOffset(Long.BYTES);
            // Verify contents are unchanged.
            assertEquals(0, buf.readLong());
        }
    }

    @Test
    public void testOffsettedSetOfUnsignedShortMustHaveDefaultEndianByteOrder() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            int value = 0x0102;
            buf.setUnsignedShort(0, value);
            buf.setWriteOffset(Long.BYTES);
            assertEquals((byte) 0x01, buf.readByte());
            assertEquals((byte) 0x02, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
        }
    }

    @Test
    public void testEnsureWritableMustThrowForNegativeSize() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertThrows(IllegalArgumentException.class, () -> buf.ensureWritable(-1));
        }
    }

    @Test
    public void testEnsureWritableMustThrowIfRequestedSizeWouldGrowBeyondMaxAllowed() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertThrows(IllegalArgumentException.class, () -> buf.ensureWritable(Integer.MAX_VALUE - 7));
        }
    }

    @Test
    public void testEnsureWritableMustNotThrowWhenSpaceIsAlreadyAvailable() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            buf.ensureWritable(8);
            assertEquals(8, buf.capacity());
            buf.writeLong(1);
            assertEquals(8, buf.capacity());
        }
    }

    @Test
    public void testEnsureWritableMustExpandBufferCapacity() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(8, buf.getWritableBytes());
            buf.writeLong(0x0102030405060708L);
            assertEquals(0, buf.getWritableBytes());
            buf.ensureWritable(8);
            assertTrue(buf.getWritableBytes() >= 8);
            assertTrue(buf.capacity() >= 16);
            buf.writeLong(0xA1A2A3A4A5A6A7A8L);
            assertEquals(16, buf.getReadableBytes());
            assertEquals(0x0102030405060708L, buf.readLong());
            assertEquals(0xA1A2A3A4A5A6A7A8L, buf.readLong());
            assertThrows(IndexOutOfBoundsException.class, buf::readByte);
        }
    }

    @Test
    public void testMultipleEnsureWritableMustExpandBufferCapacity() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(8, buf.getWritableBytes());
            buf.writeLong(0x0102030405060708L);
            assertEquals(0, buf.getWritableBytes());
            buf.ensureWritable(4); // Ask for four
            buf.ensureWritable(8); // now we expand to eight total
            assertTrue(buf.getWritableBytes() >= 8);
            assertTrue(buf.capacity() >= 16);
            buf.writeLong(0xA1A2A3A4A5A6A7A8L);
            assertEquals(16, buf.getReadableBytes());
            assertEquals(0x0102030405060708L, buf.readLong());
            assertEquals(0xA1A2A3A4A5A6A7A8L, buf.readLong());
            assertThrows(IndexOutOfBoundsException.class, buf::readByte);
        }
    }

    @Test
    public void testEnsureWritableMustExpandCapacityOfEmptyCompositeBuffer() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.composite()) {
            assertEquals(0, buf.getWritableBytes());
            buf.ensureWritable(8);
            assertEquals(8, buf.getWritableBytes());
            buf.writeLong(0xA1A2A3A4A5A6A7A8L);
            assertEquals(8, buf.getReadableBytes());
            assertEquals(0xA1A2A3A4A5A6A7A8L, buf.readLong());
            assertThrows(IndexOutOfBoundsException.class, buf::readByte);
        }
    }

    @Test
    public void testMustBeAbleToCopyAfterEnsureWritable() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(4)) {
            buf.ensureWritable(8);
            assertTrue(buf.getWritableBytes() >= 8);
            assertTrue(buf.capacity() >= 8);
            buf.writeLong(0x0102030405060708L);
            try (ProtonBuffer copy = buf.copy()) {
                long actual = copy.readLong();
                assertEquals(0x0102030405060708L, actual);
            }
        }
    }

    @Test
    public void testEnsureWritableWithCompactionMustNotAllocateIfCompactionIsEnough() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(64)) {
            while (buf.getWritableBytes() > 0) {
                buf.writeByte((byte) 42);
            }
            while (buf.getReadableBytes() > 0) {
                buf.readByte();
            }
            buf.ensureWritable(4, 4, true);
            buf.writeInt(42);
            assertEquals(64, buf.capacity());

            buf.setWriteOffset(60).setReadOffset(60);
            buf.ensureWritable(8, 8, true);
            buf.writeLong(42);
            assertTrue(buf.capacity() >= 64);
        }
    }

    @Test
    public void testEnsureWritableWithLargeMinimumGrowthMustGrowByAtLeastThatMuch() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(16)) {
            buf.writeLong(0).writeInt(0);
            buf.readLong();
            buf.readInt(); // Compaction is now possible as well.
            buf.ensureWritable(8, 32, true); // We don't need to allocate.
            assertEquals(16, buf.capacity());
            buf.writeByte((byte) 1);
            buf.ensureWritable(16, 32, true); // Now we DO need to allocate, because we can't compact.
            assertTrue(buf.capacity() >= 48); // Initial capacity + minimum growth allowed
        }
    }

    @Test
    public void testEmptyBuffersAreEqual() {
        final byte[] data = "".getBytes(StandardCharsets.UTF_8);
        try (ProtonBufferAllocator allocator = createTestCaseAllocator();
             ProtonBuffer buf1 = allocator.allocate(data.length);
             ProtonBuffer buf2 = allocator.allocate(data.length)) {

            buf1.writeBytes(data);
            buf2.writeBytes(data);

            assertEquals(buf1, buf2);
            assertEquals(buf2, buf1);
        }
    }

    @Test
    public void testEqualBuffersAreEqual() {
        final byte[] data = "foo".getBytes(StandardCharsets.UTF_8);
        try (ProtonBufferAllocator allocator = createTestCaseAllocator();
             ProtonBuffer buf1 = allocator.allocate(data.length);
             ProtonBuffer buf2 = allocator.allocate(data.length)) {

            buf1.writeBytes(data);
            buf2.writeBytes(data);

            assertEquals(buf1, buf2);
            assertEquals(buf2, buf1);
        }
    }

    @Test
    public void testDifferentBuffersAreNotEqual() {
        byte[] data1 = "foo".getBytes(StandardCharsets.UTF_8);
        byte[] data2 = "foo1".getBytes(StandardCharsets.UTF_8);
        try (ProtonBufferAllocator allocator = createTestCaseAllocator();
             ProtonBuffer buf1 = allocator.allocate(data1.length);
             ProtonBuffer buf2 = allocator.allocate(data2.length)) {

            buf1.writeBytes(data1);
            buf2.writeBytes(data2);

            assertNotEquals(buf1, buf2);
            assertNotEquals(buf2, buf1);
        }
    }

    @Test
    public void testSameReadableContentBuffersAreEqual() {
        byte[] data1 = "foo".getBytes(StandardCharsets.UTF_8);
        byte[] data2 = "notfoomaybe".getBytes(StandardCharsets.UTF_8);
        try (ProtonBufferAllocator allocator = createTestCaseAllocator();
             ProtonBuffer buf1 = allocator.allocate(data1.length);
             ProtonBuffer buf2 = allocator.allocate(data2.length)) {

            buf1.writeBytes(data1);
            buf2.writeBytes(data2);
            buf2.advanceReadOffset(3);
            buf2.setWriteOffset(buf2.getWriteOffset() - 5);

            assertEquals(buf1, buf2);
            assertEquals(buf2, buf1);
        }
    }

    @Test
    public void testImplicitGrowthLimit() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(ProtonBufferUtils.MAX_BUFFER_CAPACITY, buf.implicitGrowthLimit());
            buf.implicitGrowthLimit(buf.capacity());
            assertEquals(buf.capacity(), buf.implicitGrowthLimit());
        }
    }

    @Test
    public void testImplicitLimitMustBeWithinBounds() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertThrows(IndexOutOfBoundsException.class, () -> buf.implicitGrowthLimit(0));
            assertThrows(IndexOutOfBoundsException.class, () -> buf.implicitGrowthLimit(buf.capacity() - 1));
            assertThrows(IndexOutOfBoundsException.class, () -> buf.implicitGrowthLimit(-1));
            assertThrows(IndexOutOfBoundsException.class, () -> buf.implicitGrowthLimit(ProtonBufferUtils.MAX_BUFFER_CAPACITY + 1));
            buf.implicitGrowthLimit(ProtonBufferUtils.MAX_BUFFER_CAPACITY);
            buf.implicitGrowthLimit(buf.capacity());
            buf.writeLong(0x0102030405060708L);
            assertThrows(IndexOutOfBoundsException.class, () -> buf.writeByte((byte) 1));
            assertEquals(8, buf.capacity());
        }
    }

    @Test
    public void testBufferFromCopyMustHaveResetImplicitCapacityLimit() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            buf.writeLong(0x0102030405060708L);
            buf.implicitGrowthLimit(buf.capacity());
            assertThrows(IndexOutOfBoundsException.class, () -> buf.writeByte((byte) 1));
            try (ProtonBuffer copy = buf.copy()) {
                assertEquals(8, copy.getReadableBytes());
                copy.writeByte((byte) 2); // No more implicit limit
            }
            try (ProtonBuffer copy = buf.copy(0, 8)) {
                assertEquals(8, copy.getReadableBytes());
                copy.writeByte((byte) 2); // No more implicit limit
            }
            try (ProtonBuffer copy = buf.copy(1, 6)) {
                assertEquals(6, copy.getReadableBytes());
                copy.writeByte((byte) 3); // No more implicit limit
            }
        }
    }

    @Test
    public void testBufferFromSplitMustHaveResetImplicitCapacityLimit() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(32)) {
            buf.implicitGrowthLimit(32);
            try (ProtonBuffer split = buf.split()) {
                split.writeLong(0).writeLong(0).writeLong(0).writeLong(0); // 32 bytes written.
                split.writeByte((byte) 0); // 33rd byte, mustn't fail.
            }
            assertEquals(32, buf.capacity());
            try (ProtonBuffer split = buf.split(2)) {
                split.writeLong(0).writeLong(0).writeLong(0).writeLong(0); // 32 bytes written.
                split.writeByte((byte) 0); // 33rd byte, mustn't fail.
            }
            assertEquals(30, buf.capacity());
            buf.writeInt(42).readInt();
            try (ProtonBuffer split = buf.readSplit(4)) {
                split.writeLong(0).writeLong(0).writeLong(0).writeLong(0); // 32 bytes written.
                split.writeByte((byte) 0); // 33rd byte, mustn't fail.
            }
            assertEquals(22, buf.capacity());
            buf.writeLong(0);
            try (ProtonBuffer split = buf.writeSplit(8)) {
                split.writeLong(0).writeLong(0).writeLong(0).writeLong(0); // 32 bytes written.
                split.writeByte((byte) 0); // 33rd byte, mustn't fail.
            }
            assertEquals(6, buf.capacity());
            buf.writeLong(0).writeLong(0).writeLong(0).writeLong(0); // 32 bytes written.
            assertThrows(IndexOutOfBoundsException.class, () -> buf.writeByte((byte) 0)); // 33rd byte, at limit.
        }
    }

    @Test
    public void testSplitDoesNotReduceImplicitCapacityLimit() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            buf.implicitGrowthLimit(8);
            buf.writeLong(0x0102030405060708L);
            buf.split().close();
            assertEquals(0, buf.capacity());
            buf.writeLong(0x0102030405060708L);
            assertThrows(IndexOutOfBoundsException.class, () -> buf.writeByte((byte) 1));
        }
    }

    @Test
    public void testEnsureWritableCanGrowBeyondImplicitCapacityLimit() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8).implicitGrowthLimit(8)) {
            buf.writeLong(0x0102030405060708L);
            buf.ensureWritable(8);
            buf.writeLong(0x0102030405060708L);
            assertThrows(IndexOutOfBoundsException.class, () -> buf.writeByte((byte) 1));
            buf.readByte();
            buf.compact(); // Now we have room and don't need to expand capacity implicitly.
            buf.writeByte((byte) 1);
            // And now there's no more room.
            assertThrows(IndexOutOfBoundsException.class, () -> buf.writeByte((byte) 1));
        }
    }

    @Test
    public void testWritesMustThrowIfSizeWouldGoBeyondImplicitCapacityLimit() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator()) {
            // Short
            try (ProtonBuffer buf = allocator.allocate(0).implicitGrowthLimit(1)) {
                assertThrows(IndexOutOfBoundsException.class, () -> buf.writeShort((short) 0));
                buf.writeByte((byte) 0);
            }
            try (ProtonBuffer buf = allocator.allocate(1).implicitGrowthLimit(2)) {
                buf.writeByte((byte) 0);
                assertThrows(IndexOutOfBoundsException.class, () -> buf.writeShort((short) 0));
                buf.writeByte((byte) 0);
            }
            try (ProtonBuffer buf = allocator.allocate(0).implicitGrowthLimit(1)) {
                assertThrows(IndexOutOfBoundsException.class, () -> buf.writeUnsignedShort(0));
                buf.writeByte((byte) 0);
            }
            try (ProtonBuffer buf = allocator.allocate(1).implicitGrowthLimit(2)) {
                buf.writeByte((byte) 0);
                assertThrows(IndexOutOfBoundsException.class, () -> buf.writeUnsignedShort(0));
                buf.writeByte((byte) 0);
            }

            // Char
            try (ProtonBuffer buf = allocator.allocate(0).implicitGrowthLimit(1)) {
                assertThrows(IndexOutOfBoundsException.class, () -> buf.writeChar('0'));
                buf.writeByte((byte) 0);
            }
            try (ProtonBuffer buf = allocator.allocate(1).implicitGrowthLimit(2)) {
                buf.writeByte((byte) 0);
                assertThrows(IndexOutOfBoundsException.class, () -> buf.writeChar('0'));
                buf.writeByte((byte) 0);
            }

            // Int
            try (ProtonBuffer buf = allocator.allocate(0).implicitGrowthLimit(3)) {
                assertThrows(IndexOutOfBoundsException.class, () -> buf.writeInt(0));
                buf.writeByte((byte) 0);
            }
            try (ProtonBuffer buf = allocator.allocate(1).implicitGrowthLimit(4)) {
                buf.writeByte((byte) 0);
                assertThrows(IndexOutOfBoundsException.class, () -> buf.writeInt(0));
                buf.writeByte((byte) 0);
            }
            try (ProtonBuffer buf = allocator.allocate(0).implicitGrowthLimit(3)) {
                assertThrows(IndexOutOfBoundsException.class, () -> buf.writeUnsignedInt(0));
                buf.writeByte((byte) 0);
            }
            try (ProtonBuffer buf = allocator.allocate(1).implicitGrowthLimit(4)) {
                buf.writeByte((byte) 0);
                assertThrows(IndexOutOfBoundsException.class, () -> buf.writeUnsignedInt(0));
                buf.writeByte((byte) 0);
            }

            // Float
            try (ProtonBuffer buf = allocator.allocate(0).implicitGrowthLimit(3)) {
                assertThrows(IndexOutOfBoundsException.class, () -> buf.writeFloat(0));
                buf.writeByte((byte) 0);
            }
            try (ProtonBuffer buf = allocator.allocate(1).implicitGrowthLimit(4)) {
                buf.writeByte((byte) 0);
                assertThrows(IndexOutOfBoundsException.class, () -> buf.writeFloat(0));
                buf.writeByte((byte) 0);
            }

            // Long
            try (ProtonBuffer buf = allocator.allocate(0).implicitGrowthLimit(7)) {
                assertThrows(IndexOutOfBoundsException.class, () -> buf.writeLong(0));
                buf.writeByte((byte) 0);
            }
            try (ProtonBuffer buf = allocator.allocate(1).implicitGrowthLimit(8)) {
                buf.writeByte((byte) 0);
                assertThrows(IndexOutOfBoundsException.class, () -> buf.writeLong(0));
                buf.writeByte((byte) 0);
            }

            // Double
            try (ProtonBuffer buf = allocator.allocate(0).implicitGrowthLimit(7)) {
                assertThrows(IndexOutOfBoundsException.class, () -> buf.writeDouble(0));
                buf.writeByte((byte) 0);
            }
            try (ProtonBuffer buf = allocator.allocate(1).implicitGrowthLimit(8)) {
                buf.writeByte((byte) 0);
                assertThrows(IndexOutOfBoundsException.class, () -> buf.writeDouble(0));
                buf.writeByte((byte) 0);
            }
        }
    }

    @Test
    public void testCopyWithoutOffsetAndSizeMustReturnReadableRegion() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            for (byte b : new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08 }) {
                buf.writeByte(b);
            }
            assertEquals(0x01, buf.readByte());
            buf.setWriteOffset(buf.getWriteOffset() - 1);
            try (ProtonBuffer copy = buf.copy()) {
                assertFalse(copy.isReadOnly());
                assertArrayEquals(new byte[] {0x02, 0x03, 0x04, 0x05, 0x06, 0x07}, ProtonBufferUtils.toByteArray(buf));
                assertEquals(0, copy.getReadOffset());
                assertEquals(6, copy.getReadableBytes());
                assertEquals(6, copy.getWriteOffset());
                assertEquals(6, copy.capacity());
                assertEquals(0x02, copy.readByte());
                assertEquals(0x03, copy.readByte());
                assertEquals(0x04, copy.readByte());
                assertEquals(0x05, copy.readByte());
                assertEquals(0x06, copy.readByte());
                assertEquals(0x07, copy.readByte());
                assertThrows(IndexOutOfBoundsException.class, copy::readByte);
            }
        }
    }

    @Test
    public void testCopyOfReadOnlyBufferNotBeReadOnly() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            buf.writeInt(42).convertToReadOnly();
            try (ProtonBuffer copy = buf.copy()) {
                assertFalse(copy.isReadOnly());
                assertEquals(42, copy.readInt());
            }
        }
    }

    @Test
    public void testCopyOfBufferMustBeReadOnlyWhenRequested() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            buf.writeInt(42);
            try (ProtonBuffer copy = buf.copy(true)) {
                assertTrue(copy.isReadOnly());
                assertEquals(42, copy.readInt());
            }
            buf.convertToReadOnly();
            try (ProtonBuffer copy = buf.copy(true)) {
                assertTrue(copy.isReadOnly());
                assertEquals(42, copy.readInt());
            }
        }
    }

    @Test
    public void testCopyOfBufferWithOffsetsMustBeReadOnlyWhenRequested() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            buf.writeInt(42);
            try (ProtonBuffer copy = buf.copy(0, 4, true)) {
                assertTrue(copy.isReadOnly());
                assertEquals(42, copy.readInt());
            }
            buf.convertToReadOnly();
            try (ProtonBuffer copy = buf.copy(0, 4, true)) {
                assertTrue(copy.isReadOnly());
                assertEquals(42, copy.readInt());
            }
        }
    }

    @Test
    public void testCopyWithOffsetAndSizeMustReturnGivenRegion() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            for (byte b : new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08 }) {
                buf.writeByte(b);
            }
            buf.setReadOffset(3); // Reader and writer offsets must be ignored.
            buf.setWriteOffset(6);
            try (ProtonBuffer copy = buf.copy(1, 6)) {
                assertFalse(copy.isReadOnly());
                assertArrayEquals(new byte[] {0x02, 0x03, 0x04, 0x05, 0x06, 0x07}, ProtonBufferUtils.toByteArray(copy));
                assertEquals(0, copy.getReadOffset());
                assertEquals(6, copy.getReadableBytes());
                assertEquals(6, copy.getWriteOffset());
                assertEquals(6, copy.capacity());
                assertEquals(0x02, copy.readByte());
                assertEquals(0x03, copy.readByte());
                assertEquals(0x04, copy.readByte());
                assertEquals(0x05, copy.readByte());
                assertEquals(0x06, copy.readByte());
                assertEquals(0x07, copy.readByte());
                assertThrows(IndexOutOfBoundsException.class, copy::readByte);
            }
        }
    }

    @Test
    public void testCopyWithOffsetAndSizeAndReadOnlyStateTrueMustReturnGivenRegion() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            for (byte b : new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08 }) {
                buf.writeByte(b);
            }
            buf.setReadOffset(3); // Reader and writer offsets must be ignored.
            buf.setWriteOffset(6);
            try (ProtonBuffer copy = buf.copy(1, 6, true)) {
                assertTrue(copy.isReadOnly());
                assertArrayEquals(new byte[] {0x02, 0x03, 0x04, 0x05, 0x06, 0x07}, ProtonBufferUtils.toByteArray(copy));
                assertEquals(0, copy.getReadOffset());
                assertEquals(6, copy.getReadableBytes());
                assertEquals(6, copy.getWriteOffset());
                assertEquals(6, copy.capacity());
                assertEquals(0x02, copy.readByte());
                assertEquals(0x03, copy.readByte());
                assertEquals(0x04, copy.readByte());
                assertEquals(0x05, copy.readByte());
                assertEquals(0x06, copy.readByte());
                assertEquals(0x07, copy.readByte());
                assertThrows(IndexOutOfBoundsException.class, copy::readByte);
            }
        }
    }

    @Test
    public void testCopyWithOffsetAndSizeAndReadOnlyStateFalseMustReturnGivenRegion() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            for (byte b : new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08 }) {
                buf.writeByte(b);
            }
            buf.setReadOffset(3); // Reader and writer offsets must be ignored.
            buf.setWriteOffset(6);
            try (ProtonBuffer copy = buf.copy(1, 6, false)) {
                assertFalse(copy.isReadOnly());
                assertArrayEquals(new byte[] {0x02, 0x03, 0x04, 0x05, 0x06, 0x07}, ProtonBufferUtils.toByteArray(copy));
                assertEquals(0, copy.getReadOffset());
                assertEquals(6, copy.getReadableBytes());
                assertEquals(6, copy.getWriteOffset());
                assertEquals(6, copy.capacity());
                assertEquals(0x02, copy.readByte());
                assertEquals(0x03, copy.readByte());
                assertEquals(0x04, copy.readByte());
                assertEquals(0x05, copy.readByte());
                assertEquals(0x06, copy.readByte());
                assertEquals(0x07, copy.readByte());
                assertThrows(IndexOutOfBoundsException.class, copy::readByte);
            }
        }
    }

    @Test
    public void testCopyWithNegativeSizeMustThrow() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertThrows(IndexOutOfBoundsException.class, () -> buf.copy(0, -1));
            assertThrows(IndexOutOfBoundsException.class, () -> buf.copy(2, -1));
            assertThrows(IndexOutOfBoundsException.class, () -> buf.copy(0, -1, true));
            assertThrows(IndexOutOfBoundsException.class, () -> buf.copy(2, -1, true));
        }
    }

    @Test
    public void testCopyWithSizeGreaterThanCapacityMustThrow() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertThrows(IndexOutOfBoundsException.class, () -> buf.copy(0, 9));
            assertThrows(IndexOutOfBoundsException.class, () -> buf.copy(0, 9, true));
            buf.copy(0, 8).close(); // This is still fine.
            assertThrows(IndexOutOfBoundsException.class, () -> buf.copy(1, 8));
            assertThrows(IndexOutOfBoundsException.class, () -> buf.copy(1, 8, true));
        }
    }

    @Test
    public void testCopyWithZeroSizeMustBeAllowed() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            buf.copy(0, 0).close();
            buf.copy(0, 0, true).close();
        }
    }

    @Test
    public void testCopyOfLastByte() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator();
             ProtonBuffer buf = allocator.allocate(8).writeLong(0x0102030405060708L);
             ProtonBuffer copy = buf.copy(7, 1)) {

            assertEquals(1, copy.capacity());
            assertEquals((byte) 0x08, copy.readByte());
        }
    }

    @Test
    public void testSplitWithNegativeOffsetMustThrow() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            buf.split(0).close();
            assertThrows(IllegalArgumentException.class, () -> buf.split(-1));
        }
    }

    @Test
    public void testSplitWithOversizedOffsetMustThrow() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertThrows(IllegalArgumentException.class, () -> buf.split(9));
            buf.split(8).close();
        }
    }

    @Test
    public void testSplitOnOffsetMustTruncateGreaterOffsets() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            buf.writeInt(0x01020304);
            buf.writeByte((byte) 0x05);
            buf.readInt();
            try (ProtonBuffer split = buf.split(2)) {
                assertEquals(2, buf.getReadOffset());
                assertEquals(3, buf.getWriteOffset());

                assertEquals(2, split.getReadOffset());
                assertEquals(2, split.getWriteOffset());
            }
        }
    }

    @Test
    public void testSplitOnOffsetMustExtendLesserOffsets() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            buf.writeInt(0x01020304);
            buf.readInt();
            try (ProtonBuffer split = buf.split(6)) {
                assertEquals(0, buf.getReadOffset());
                assertEquals(0, buf.getWriteOffset());

                assertEquals(4, split.getReadOffset());
                assertEquals(4, split.getWriteOffset());
            }
        }
    }

    @Test
    public void testSplitPartMustContainFirstHalfOfBuffer() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(16)) {
            buf.writeLong(0x0102030405060708L);
            assertEquals(0x01, buf.readByte());
            try (ProtonBuffer split = buf.split()) {
                // Original buffer:
                assertEquals(8, buf.capacity());
                assertEquals(0, buf.getReadOffset());
                assertEquals(0, buf.getWriteOffset());
                assertEquals(0, buf.getReadableBytes());
                assertThrows(IndexOutOfBoundsException.class, () -> buf.readByte());

                // Split part:
                assertEquals(8, split.capacity());
                assertEquals(1, split.getReadOffset());
                assertEquals(8, split.getWriteOffset());
                assertEquals(7, split.getReadableBytes());
                assertEquals(0x02, split.readByte());
                assertEquals(0x03040506, split.readInt());
                assertEquals(0x07, split.readByte());
                assertEquals(0x08, split.readByte());
                assertThrows(IndexOutOfBoundsException.class, () -> split.readByte());
            }

            // Split part does NOT return when closed:
            assertEquals(8, buf.capacity());
            assertEquals(0, buf.getReadOffset());
            assertEquals(0, buf.getWriteOffset());
            assertEquals(0, buf.getReadableBytes());
            assertThrows(IndexOutOfBoundsException.class, () -> buf.readByte());
        }
    }

    @Test
    public void testMustBePossibleToSplitMoreThanOnce() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            buf.writeLong(0x0102030405060708L);
            try (ProtonBuffer a = buf.split()) {
                a.setWriteOffset(4);
                try (ProtonBuffer b = a.split()) {
                    assertEquals(0x01020304, b.readInt());
                    a.setWriteOffset(4);
                    assertEquals(0x05060708, a.readInt());
                    assertThrows(IndexOutOfBoundsException.class, () -> b.readByte());
                    assertThrows(IndexOutOfBoundsException.class, () -> a.readByte());
                    buf.writeLong(0xA1A2A3A4A5A6A7A8L);
                    buf.setWriteOffset(4);
                    try (ProtonBuffer c = buf.split()) {
                        assertEquals(0xA1A2A3A4, c.readInt());
                        buf.setWriteOffset(4);
                        assertEquals(0xA5A6A7A8, buf.readInt());
                        assertThrows(IndexOutOfBoundsException.class, () -> c.readByte());
                        assertThrows(IndexOutOfBoundsException.class, () -> buf.readByte());
                    }
                }
            }
        }
    }

    @Test
    public void testMustBePossibleToSplitCopies() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            buf.writeLong(0x0102030405060708L);
            try (ProtonBuffer copy = buf.copy()) {
                buf.close();
                assertEquals(0x0102030405060708L, copy.getLong(0));
                try (ProtonBuffer split = copy.split(4)) {
                    split.clear().ensureWritable(Long.BYTES);
                    copy.clear().ensureWritable(Long.BYTES);
                    assertEquals(Long.BYTES, split.capacity());
                    assertEquals(Long.BYTES, copy.capacity());
                    assertEquals(0x01020304, split.getInt(0));
                    assertEquals(0x05060708, copy.getInt(0));
                }
            }
        }
    }

    @Test
    public void testEnsureWritableOnSplitBuffers() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            buf.writeLong(0x0102030405060708L);
            try (ProtonBuffer a = buf.split()) {
                assertEquals(0x0102030405060708L, a.readLong());
                a.ensureWritable(8);
                a.writeLong(0xA1A2A3A4A5A6A7A8L);
                assertEquals(0xA1A2A3A4A5A6A7A8L, a.readLong());

                buf.ensureWritable(8);
                buf.writeLong(0xA1A2A3A4A5A6A7A8L);
                assertEquals(0xA1A2A3A4A5A6A7A8L, buf.readLong());
            }
        }
    }

    @Test
    public void testEnsureWritableOnSplitBuffersWithOddOffsets() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(10)) {
            buf.writeLong(0x0102030405060708L);
            buf.writeByte((byte) 0x09);
            buf.readByte();
            try (ProtonBuffer a = buf.split()) {
                assertEquals(0x0203040506070809L, a.readLong());
                a.ensureWritable(8);
                a.writeLong(0xA1A2A3A4A5A6A7A8L);
                assertEquals(0xA1A2A3A4A5A6A7A8L, a.readLong());

                buf.ensureWritable(8);
                buf.writeLong(0xA1A2A3A4A5A6A7A8L);
                assertEquals(0xA1A2A3A4A5A6A7A8L, buf.readLong());
            }
        }
    }

    @Test
    public void testSplitOnEmptyCompositeBuffer() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.composite()) {
            verifySplitEmptyCompositeBuffer(buf);
        }
    }

    @Test
    public void testSplitOfReadOnlyBufferMustBeReadOnly() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(16)) {
            buf.writeLong(0x0102030405060708L);
            buf.convertToReadOnly();
            try (ProtonBuffer split = buf.split()) {
                assertTrue(split.isReadOnly());
                assertTrue(buf.isReadOnly());

                assertEquals(0x0102030405060708L, split.readLong());
                assertFalse(buf.isReadable());
            }
        }
    }

    @Test
    public void testAllocatingOnClosedAllocatorMustThrow() {
        ProtonBufferAllocator allocator = createTestCaseAllocator();
        allocator.close();
        assertThrows(IllegalStateException.class, () -> allocator.allocate(8));
    }

    @Test
    public void testReadSplit() {
        doTestReadSplit(3, 3, 1, 1);
    }

    @Test
    public void testReadSplitWriteOffsetLessThanCapacity() {
        doTestReadSplit(5, 4, 2, 1);
    }

    @Test
    public void testReadSplitOffsetZero() {
        doTestReadSplit(3, 3, 1, 0);
    }

    @Test
    public void testReadSplitOffsetToWriteOffset() {
        doTestReadSplit(3, 3, 1, 2);
    }

    private void doTestReadSplit(int capacity, int writeBytes, int readBytes, int offset) {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(capacity)) {
            writeRandomBytes(buf, writeBytes);
            assertEquals(writeBytes, buf.getWriteOffset());

            for (int i = 0; i < readBytes; i++) {
                buf.readByte();
            }
            assertEquals(readBytes, buf.getReadOffset());

            try (ProtonBuffer split = buf.readSplit(offset)) {
                assertEquals(readBytes + offset, split.capacity());
                assertEquals(split.capacity(), split.getWriteOffset());
                assertEquals(readBytes, split.getReadOffset());

                assertEquals(capacity - split.capacity(), buf.capacity());
                assertEquals(writeBytes - split.capacity(), buf.getWriteOffset());
                assertEquals(0, buf.getReadOffset());
            }
        }
    }

    @Test
    public void testWriteSplit() {
        doTestWriteSplit(5, 3, 1, 1);
    }

    @Test
    public void testWriteSplitWriteOffsetLessThanCapacity() {
        doTestWriteSplit(5, 2, 2, 2);
    }

    @Test
    public void testWriteSplitOffsetZero() {
        doTestWriteSplit(3, 3, 1, 0);
    }

    @Test
    public void testWriteSplitOffsetToCapacity() {
        doTestWriteSplit(3, 1, 1, 2);
    }

    private void doTestWriteSplit(int capacity, int writeBytes, int readBytes, int offset) {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(capacity)) {
            writeRandomBytes(buf, writeBytes);
            assertEquals(writeBytes, buf.getWriteOffset());
            for (int i = 0; i < readBytes; i++) {
                buf.readByte();
            }
            assertEquals(readBytes, buf.getReadOffset());

            try (ProtonBuffer split = buf.writeSplit(offset)) {
                assertEquals(writeBytes + offset, split.capacity());
                assertEquals(writeBytes, split.getWriteOffset());
                assertEquals(readBytes, split.getReadOffset());

                assertEquals(capacity - split.capacity(), buf.capacity());
                assertEquals(0, buf.getWriteOffset());
                assertEquals(0, buf.getReadOffset());
            }
        }
    }

    @Test
    public void testSplitPostFull() {
        doTestSplitPostFullOrRead(false);
    }

    @Test
    public void testSplitPostFullAndRead() {
        doTestSplitPostFullOrRead(true);
    }

    private void doTestSplitPostFullOrRead(boolean read) {
        final int capacity = 3;
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(capacity)) {
            writeRandomBytes(buf, capacity);
            assertEquals(buf.capacity(), buf.getWriteOffset());
            if (read) {
                for (int i = 0; i < capacity; i++) {
                    buf.readByte();
                }
            }
            assertEquals(read ? buf.capacity() : 0, buf.getReadOffset());

            try (ProtonBuffer split = buf.split()) {
                assertEquals(capacity, split.capacity());
                assertEquals(split.capacity(), split.getWriteOffset());
                assertEquals(read ? split.capacity() : 0, split.getReadOffset());

                assertEquals(0, buf.capacity());
                assertEquals(0, buf.getWriteOffset());
                assertEquals(0, buf.getReadOffset());
            }
        }
    }

    @Test
    public void testCompactMustDiscardReadBytes() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(16)) {
            buf.writeLong(0x0102030405060708L).writeInt(0x090A0B0C);
            assertEquals(0x01020304, buf.readInt());
            assertEquals(12, buf.getWriteOffset());
            assertEquals(4, buf.getReadOffset());
            assertEquals(4, buf.getWritableBytes());
            assertEquals(8, buf.getReadableBytes());
            assertEquals(16, buf.capacity());
            buf.compact();
            assertEquals(8, buf.getWriteOffset());
            assertEquals(0, buf.getReadOffset());
            assertEquals(8, buf.getWritableBytes());
            assertEquals(8, buf.getReadableBytes());
            assertEquals(16, buf.capacity());
            assertEquals(0x05060708090A0B0CL, buf.readLong());
        }
    }

    @Test
    public void testCopyOfByteArrayMustContainContents() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator()) {
            byte[] array = new byte[23];
            writeRandomBytes(array);
            try (ProtonBuffer buffer = allocator.copy(array)) {
                assertEquals(array.length, buffer.capacity());
                assertEquals(array.length, buffer.getReadableBytes());
                for (int i = 0; i < array.length; i++) {
                    byte b = array[i];
                    byte a = buffer.readByte();
                    if (b != a) {
                        fail(String.format("Wrong contents at offset %s. Expected %s but was %s.", i, b, a));
                    }
                }
            }
        }
    }

    @Test
    public void testCopyOfByteArrayMustNotBeReadOnly() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator()) {
            byte[] array = new byte[23];
            writeRandomBytes(array);
            try (ProtonBuffer buffer = allocator.copy(array)) {
                assertFalse(buffer.isReadOnly());
                buffer.ensureWritable(Long.BYTES, 1, true);
                buffer.writeLong(0x0102030405060708L);
                assertTrue(buffer.capacity() >= array.length + Long.BYTES);
                assertEquals(array.length + Long.BYTES, buffer.getReadableBytes());
                for (int i = 0; i < array.length; i++) {
                    byte b = array[i];
                    byte a = buffer.readByte();
                    if (b != a) {
                        fail(String.format("Wrong contents at offset %s. Expected %s but was %s.", i, b, a));
                    }
                }
                assertEquals(0x0102030405060708L, buffer.readLong());
            }
        }
    }

    @Test
    public void testCopyOfByteArrayMustNotReflectChangesToArray() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator()) {
            byte[] array = { 1, 1, 1, 1, 1, 1, 1, 1 };
            try (ProtonBuffer buffer = allocator.copy(array)) {
                array[2] = 2; // Change to array should not be reflected in buffer.
                assertEquals(0x0101010101010101L, buffer.readLong());
            }
        }
    }

    @Test
    public void testCopyOfEmptyByteArrayMustProduceEmptyBuffer() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator();
             ProtonBuffer buffer = allocator.copy(new byte[0])) {

            assertEquals(0, buffer.capacity());
            assertFalse(buffer.isClosed());
            buffer.ensureWritable(4);
            assertTrue(buffer.capacity() >= 4);
        }
    }

    @Test
    public void testMustThrowWhenAllocatingNegativeSizedBuffer() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator()) {
            assertThrows(IllegalArgumentException.class, () -> allocator.allocate(-1));
        }
    }

    @Test
    public void testSetReaderOffsetMustThrowOnNegativeIndex() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertThrows(IndexOutOfBoundsException.class, () -> buf.setReadOffset(-1));
        }
    }

    @Test
    public void testSetReaderOffsetMustThrowOnOversizedIndex() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertThrows(IndexOutOfBoundsException.class, () -> buf.setReadOffset(1));
            buf.writeLong(0);
            assertThrows(IndexOutOfBoundsException.class, () -> buf.setReadOffset(9));

            buf.setReadOffset(8);
            assertThrows(IndexOutOfBoundsException.class, buf::readByte);
        }
    }

    @Test
    public void testSetWriterOffsetMustThrowOutsideOfWritableRegion() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            // Writer offset cannot be negative.
            assertThrows(IndexOutOfBoundsException.class, () -> buf.setWriteOffset(-1));

            buf.setWriteOffset(4);
            buf.setReadOffset(4);

            // Cannot set writer offset before reader offset.
            assertThrows(IndexOutOfBoundsException.class, () -> buf.setWriteOffset(3));
            assertThrows(IndexOutOfBoundsException.class, () -> buf.setWriteOffset(0));

            buf.setWriteOffset(buf.capacity());

            // Cannot set writer offset beyond capacity.
            assertThrows(IndexOutOfBoundsException.class, () -> buf.setWriteOffset(buf.capacity() + 1));
        }
    }

    @Test
    public void testSetReaderOffsetMustNotThrowWithinBounds() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertSame(buf.setReadOffset(0), buf);
            buf.writeLong(0);
            assertSame(buf.setReadOffset(7), buf);
            assertSame(buf.setReadOffset(8), buf);
        }
    }

    @Test
    public void testCapacityMustBeAllocatedSize() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(8, buf.capacity());
            try (ProtonBuffer b = allocator.allocate(13)) {
                assertEquals(13, b.capacity());
            }
        }
    }

    @Test
    public void testReaderWriterOffsetUpdates() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(22)) {
            assertEquals(0, buf.getWriteOffset());
            assertSame(buf.setWriteOffset(1) , buf);
            assertEquals(1, buf.getWriteOffset());
            assertSame(buf.writeByte((byte) 7), buf);
            assertEquals(2, buf.getWriteOffset());
            assertSame(buf.writeShort((short) 3003), buf);
            assertEquals(4, buf.getWriteOffset());
            assertSame(buf.writeInt(0x5A55_BA55), buf);
            assertEquals(8, buf.getWriteOffset());
            assertSame(buf.writeLong(0x123456789ABCDEF0L), buf);
            assertEquals(16, buf.getWriteOffset());
            assertEquals(6, buf.getWritableBytes());
            assertEquals(16, buf.getReadableBytes());

            assertEquals(0, buf.getReadOffset());
            assertSame(buf.setReadOffset(1) , buf);
            assertEquals(1, buf.getReadOffset());
            assertEquals((byte) 7, buf.readByte());
            assertEquals(2, buf.getReadOffset());
            assertEquals((short) 3003, buf.readShort());
            assertEquals(4, buf.getReadOffset());
            assertEquals(0x5A55_BA55, buf.readInt());
            assertEquals(8, buf.getReadOffset());
            assertEquals(0x123456789ABCDEF0L, buf.readLong());
            assertEquals(16, buf.getReadOffset());
            assertEquals(0, buf.getReadableBytes());
        }
    }

    @Test
    public void readAndWriteBoundsChecksWithIndexUpdates() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            buf.writeLong(0);

            buf.readLong(); // Fine.
            buf.setReadOffset(1);
            assertThrows(IndexOutOfBoundsException.class, buf::readLong);

            buf.setReadOffset(4);
            buf.readInt(); // Fine.
            buf.setReadOffset(5);

            assertThrows(IndexOutOfBoundsException.class, buf::readInt);
        }
    }

    @Test
    public void testClearMustSetReaderAndWriterOffsetsToTheirInitialPositions() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            buf.writeInt(0).readShort();
            buf.clear();
            assertEquals(0, buf.getReadOffset());
            assertEquals(0, buf.getWriteOffset());
        }
    }

    @Test
    public void testReadableBytesMustMatchWhatWasWritten() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            buf.writeLong(0);
            assertEquals(Long.BYTES, buf.getReadableBytes());
            buf.readShort();
            assertEquals(Long.BYTES - Short.BYTES, buf.getReadableBytes());
        }
    }

    @Test
    public void testAdvanceReadOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(32)) {
            writeRandomBytes(buf, 16);

            for (int i = 0; i < 8; i++) {
                buf.readByte();
            }

            buf.advanceReadOffset(8);
            assertEquals(8 + 8, buf.getReadOffset());
        }
    }

    @Test
    public void testAdvanceReadOffsetNegativeMustThrow() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            buf.writeLong(1);
            buf.readInt();
            assertThrows(IllegalArgumentException.class, () -> buf.advanceReadOffset(-1));
        }
    }

    @Test
    public void testAdvanceWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(32)) {
            writeRandomBytes(buf, 16);

            buf.advanceWriteOffset(8);
            assertEquals(16 + 8, buf.getWriteOffset());
        }
    }

    @Test
    public void testAdvanceWriteOffsetNegativeMustThrow() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            buf.writeInt(1);
            assertThrows(IllegalArgumentException.class, () -> buf.advanceWriteOffset(-1));
        }
    }

    @Test
    public void testReadOnlyBufferMustPreventWriteAccess() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            ProtonBuffer b = buf.convertToReadOnly();
            assertSame(b, buf);
            verifyWriteInaccessible(buf, ProtonBufferReadOnlyException.class);
        }
    }

    @Test
    public void testClosedBuffersAreNotReadOnly() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            buf.convertToReadOnly();
            buf.close();
            assertFalse(buf.isReadOnly());
        }
    }

    @Test
    public void testReadOnlyBufferMustMustStayReadOnlyAfterRepeatedToggles() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertFalse(buf.isReadOnly());
            buf.convertToReadOnly();
            assertTrue(buf.isReadOnly());
            verifyWriteInaccessible(buf, ProtonBufferReadOnlyException.class);

            buf.convertToReadOnly();
            assertTrue(buf.isReadOnly());

            verifyWriteInaccessible(buf, ProtonBufferReadOnlyException.class);
        }
    }

    @Test
    public void testCompactOnReadOnlyBufferMustThrow() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            buf.convertToReadOnly();
            assertThrows(ProtonBufferReadOnlyException.class, () -> buf.compact());
        }
    }

    @Test
    public void testEnsureWritableOnReadOnlyBufferMustThrow() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            buf.convertToReadOnly();
            assertThrows(ProtonBufferReadOnlyException.class, () -> buf.ensureWritable(1));
        }
    }

    @Test
    public void testCopyIntoOnReadOnlyBufferMustThrow() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            buf.convertToReadOnly();
            try (ProtonBuffer src = allocator.allocate(8)) {
                assertThrows(ProtonBufferReadOnlyException.class, () -> src.copyInto(0, buf, 0, 1));
                assertThrows(ProtonBufferReadOnlyException.class, () -> src.copyInto(0, buf, 0, 0));
            }
        }
    }

    @Test
    public void testCopyIntoOnEmptyBufferFromFullyReadBuffer() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator();
             ProtonBuffer source = allocator.allocate(8);
             ProtonBuffer target = allocator.allocate(0)) {

            source.writeLong(0xFFFFFFFFL);
            source.readLong();

            assertDoesNotThrow(() -> source.copyInto(source.getReadOffset(), target, 0, 0));
        }
    }

    @Test
    public void testCopyIntoByteBufferOnEmptyBufferFromFullyReadBuffer() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator();
             ProtonBuffer source = allocator.allocate(8)) {

            final ByteBuffer target = ByteBuffer.allocate(0);

            source.writeLong(0xFFFFFFFFL);
            source.readLong();

            assertDoesNotThrow(() -> source.copyInto(source.getReadOffset(), target, 0, 0));
        }
    }

    @Test
    public void testReadOnlyBuffersCannotChangeWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator();
             ProtonBuffer buf = allocator.allocate(8).convertToReadOnly()) {

            assertThrows(ProtonBufferReadOnlyException.class, () -> buf.setWriteOffset(0));
            assertThrows(ProtonBufferReadOnlyException.class, () -> buf.setWriteOffset(4));

            assertThrows(ProtonBufferReadOnlyException.class, () -> buf.writeByte((byte) 0));
            assertEquals(0, buf.getWriteOffset());
            assertThrows(ProtonBufferReadOnlyException.class, () -> buf.writeShort((short) 0));
            assertEquals(0, buf.getWriteOffset());
            assertThrows(ProtonBufferReadOnlyException.class, () -> buf.writeChar((char) 0));
            assertEquals(0, buf.getWriteOffset());
            assertThrows(ProtonBufferReadOnlyException.class, () -> buf.writeInt(0));
            assertEquals(0, buf.getWriteOffset());
            assertThrows(ProtonBufferReadOnlyException.class, () -> buf.writeFloat(0));
            assertEquals(0, buf.getWriteOffset());
            assertThrows(ProtonBufferReadOnlyException.class, () -> buf.writeLong(0));
            assertEquals(0, buf.getWriteOffset());
            assertThrows(ProtonBufferReadOnlyException.class, () -> buf.writeDouble(0));
            assertEquals(0, buf.getWriteOffset());

            assertThrows(ProtonBufferReadOnlyException.class, () -> buf.writeUnsignedByte(0));
            assertEquals(0, buf.getWriteOffset());
            assertThrows(ProtonBufferReadOnlyException.class, () -> buf.writeUnsignedShort(0));
            assertEquals(0, buf.getWriteOffset());
            assertThrows(ProtonBufferReadOnlyException.class, () -> buf.writeUnsignedInt(0));
            assertEquals(0, buf.getWriteOffset());
        }
    }

    @Test
    public void testCopyOfReadOnlyBufferIsNotReadOnly() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator();
             ProtonBuffer buf = allocator.allocate(8).writeLong(0x0102030405060708L).convertToReadOnly();
             ProtonBuffer copy = buf.copy()) {
            assertFalse(copy.isReadOnly());
            assertEquals(buf, copy);
            assertEquals(0, copy.getReadOffset());
            copy.setLong(0, 0xA1A2A3A4A5A6A7A8L);
            assertEquals(0xA1A2A3A4A5A6A7A8L, copy.getLong(0));
        }
    }

    @Test
    public void resetOffsetsOfReadOnlyBufferOnlyChangesReadOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator();
             ProtonBuffer buf = allocator.allocate(4).writeInt(0x01020304).convertToReadOnly()) {
            assertEquals(4, buf.getReadableBytes());
            assertEquals(0x01020304, buf.readInt());
            assertEquals(0, buf.getReadableBytes());
            buf.clear();
            assertEquals(4, buf.getReadableBytes());
            assertEquals(0x01020304, buf.readInt());
        }
    }

    @Test
    public void testRelativeReadOfBooleanMustNotBoundsCheckWhenReadOffsetAndSizeIsEqualToWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(0, buf.getReadableBytes());
            assertEquals(Long.BYTES, buf.getWritableBytes());
            boolean value = true;
            buf.writeBoolean(value);
            assertEquals(1, buf.getReadableBytes());
            assertEquals(7, buf.getWritableBytes());
            assertTrue(buf.readBoolean());
            assertEquals(0, buf.getReadableBytes());
        }
    }

    @Test
    public void testRelativeReadOfBooleanMustBoundsCheckWhenReadOffsetAndSizeIsBeyondWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(0, buf.getReadableBytes());
            assertEquals(Long.BYTES, buf.getWritableBytes());
            boolean value = true;
            buf.writeBoolean(value);
            buf.setReadOffset(1);
            assertEquals(0, buf.getReadableBytes());
            assertEquals(7, buf.getWritableBytes());
            assertThrows(IndexOutOfBoundsException.class, buf::readBoolean);
            assertEquals(0, buf.getReadableBytes());
        }
    }

    @Test
    public void testRelativeReadOfByteMustNotBoundsCheckWhenReadOffsetAndSizeIsEqualToWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(0, buf.getReadableBytes());
            assertEquals(Long.BYTES, buf.getWritableBytes());
            byte value = 0x01;
            buf.writeByte(value);
            assertEquals(1, buf.getReadableBytes());
            assertEquals(7, buf.getWritableBytes());
            assertEquals(value, buf.readByte());
            assertEquals(0, buf.getReadableBytes());
        }
    }

    @Test
    public void testRelativeReadOfByteMustBoundsCheckWhenReadOffsetAndSizeIsBeyondWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(0, buf.getReadableBytes());
            assertEquals(Long.BYTES, buf.getWritableBytes());
            byte value = 0x01;
            buf.writeByte(value);
            buf.setReadOffset(1);
            assertEquals(0, buf.getReadableBytes());
            assertEquals(7, buf.getWritableBytes());
            assertThrows(IndexOutOfBoundsException.class, buf::readByte);
            assertEquals(0, buf.getReadableBytes());
        }
    }

    @Test
    public void testRelativeReadAndGetOfUnsignedByte() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            int value = Byte.MAX_VALUE * 2;
            buf.setUnsignedByte(0, value);
            assertEquals(value, buf.getUnsignedByte(0));
            buf.writeUnsignedByte(value);
            assertEquals(value, buf.readUnsignedByte());
            assertEquals(0, buf.getReadableBytes());
        }
    }

    @Test
    public void testRelativeReadOfUnsignedByteMustNotBoundsCheckWhenReadOffsetAndSizeIsEqualToWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(0, buf.getReadableBytes());
            assertEquals(Long.BYTES, buf.getWritableBytes());
            int value = 0x01;
            buf.writeUnsignedByte(value);
            assertEquals(1, buf.getReadableBytes());
            assertEquals(7, buf.getWritableBytes());
            assertEquals(value, buf.readUnsignedByte());
            assertEquals(0, buf.getReadableBytes());
        }
    }

    @Test
    public void testRelativeReadOfUnsignedByteMustBoundsCheckWhenReadOffsetAndSizeIsBeyondWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(0, buf.getReadableBytes());
            assertEquals(Long.BYTES, buf.getWritableBytes());
            int value = 0x01;
            buf.writeUnsignedByte(value);
            buf.setReadOffset(1);
            assertEquals(0, buf.getReadableBytes());
            assertEquals(7, buf.getWritableBytes());
            assertThrows(IndexOutOfBoundsException.class, () -> buf.readUnsignedByte());
            assertEquals(0, buf.getReadableBytes());
        }
    }

    @Test
    public void testRelativeReadOfUnsignedByteReadOnlyMustBoundsCheckWhenReadOffsetAndSizeIsBeyondWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(0, buf.getReadableBytes());
            assertEquals(Long.BYTES, buf.getWritableBytes());
            int value = 0x01;
            buf.writeUnsignedByte(value);
            buf.setReadOffset(1);
            assertEquals(0, buf.getReadableBytes());
            assertEquals(7, buf.getWritableBytes());
            assertThrows(IndexOutOfBoundsException.class, () -> buf.convertToReadOnly().readUnsignedByte());
            assertEquals(0, buf.getReadableBytes());
        }
    }

    @Test
    public void testRelativeWriteOfByteMustExpandCapacityWhenWriteOffsetAndSizeIsBeyondCapacity() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(Long.BYTES, buf.capacity());
            buf.setWriteOffset(8);
            byte value = 0x01;
            buf.writeByte(value);
            assertTrue(buf.capacity() > Long.BYTES);
            buf.setReadOffset(8);
            assertEquals(value, buf.readByte());
        }
    }

    @Test
    public void testRelativeWriteOfUnsignedByteMustExpandCapacityWhenWriteOffsetAndSizeIsBeyondCapacity() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(Long.BYTES, buf.capacity());
            buf.setWriteOffset(8);
            int value = 0x01;
            buf.writeUnsignedByte(value);
            assertTrue(buf.capacity() > Long.BYTES);
            buf.setReadOffset(8);
            assertEquals(value, buf.readUnsignedByte());
        }
    }

    @Test
    public void testRelativeReadOfCharMustNotBoundsCheckWhenReadOffsetAndSizeIsEqualToWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(0, buf.getReadableBytes());
            assertEquals(Long.BYTES, buf.getWritableBytes());
            char value = 0x0102;
            buf.writeChar(value);
            assertEquals(2, buf.getReadableBytes());
            assertEquals(6, buf.getWritableBytes());
            assertEquals(value, buf.readChar());
            assertEquals(0, buf.getReadableBytes());
        }
    }

    @Test
    public void testRelativeReadOfCharMustBoundsCheckWhenReadOffsetAndSizeIsBeyondWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(0, buf.getReadableBytes());
            assertEquals(Long.BYTES, buf.getWritableBytes());
            char value = 0x0102;
            buf.writeChar(value);
            buf.setReadOffset(1);
            assertEquals(1, buf.getReadableBytes());
            assertEquals(6, buf.getWritableBytes());
            assertThrows(IndexOutOfBoundsException.class, buf::readChar);
            assertEquals(1, buf.getReadableBytes());
        }
    }

    @Test
    public void testRelativeReadOfCharReadOnlyMustBoundsCheckWhenReadOffsetAndSizeIsBeyondWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(0, buf.getReadableBytes());
            assertEquals(Long.BYTES, buf.getWritableBytes());
            char value = 0x0102;
            buf.writeChar(value);
            buf.setReadOffset(1);
            assertEquals(1, buf.getReadableBytes());
            assertEquals(6, buf.getWritableBytes());
            assertThrows(IndexOutOfBoundsException.class, () -> buf.convertToReadOnly().readChar());
            assertEquals(1, buf.getReadableBytes());
        }
    }

    @Test
    public void testRelativeWriteOfCharMustExpandCapacityWhenWriteOffsetAndSizeIsBeyondCapacity() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(Long.BYTES, buf.capacity());
            buf.setWriteOffset(7);
            char value = 0x0102;
            buf.writeChar(value);
            assertTrue(buf.capacity() > Long.BYTES);
            buf.setReadOffset(7);
            assertEquals(value, buf.readChar());
        }
    }

    @Test
    public void testRelativeReadOfShortMustNotBoundsCheckWhenReadOffsetAndSizeIsEqualToWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(0, buf.getReadableBytes());
            assertEquals(Long.BYTES, buf.getWritableBytes());
            short value = 0x0102;
            buf.writeShort(value);
            assertEquals(2, buf.getReadableBytes());
            assertEquals(6, buf.getWritableBytes());
            assertEquals(value, buf.readShort());
            assertEquals(0, buf.getReadableBytes());
        }
    }

    @Test
    public void testRelativeReadOfShortMustBoundsCheckWhenReadOffsetAndSizeIsBeyondWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(0, buf.getReadableBytes());
            assertEquals(Long.BYTES, buf.getWritableBytes());
            short value = 0x0102;
            buf.writeShort(value);
            buf.setReadOffset(1);
            assertEquals(1, buf.getReadableBytes());
            assertEquals(6, buf.getWritableBytes());
            assertThrows(IndexOutOfBoundsException.class, buf::readShort);
            assertEquals(1, buf.getReadableBytes());
        }
    }

    @Test
    public void testRelativeReadOfShortReadOnlyMustBoundsCheckWhenReadOffsetAndSizeIsBeyondWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(0, buf.getReadableBytes());
            assertEquals(Long.BYTES, buf.getWritableBytes());
            short value = 0x0102;
            buf.writeShort(value);
            buf.setReadOffset(1);
            assertEquals(1, buf.getReadableBytes());
            assertEquals(6, buf.getWritableBytes());
            assertThrows(IndexOutOfBoundsException.class, () -> buf.convertToReadOnly().readShort());
            assertEquals(1, buf.getReadableBytes());
        }
    }

    @Test
    public void testRelativeReadAndGetOfUnsignedShort() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            int value = Short.MAX_VALUE * 2;
            buf.setUnsignedShort(0, value);
            assertEquals(value, buf.getUnsignedShort(0));
            buf.writeUnsignedShort(value);
            assertEquals(value, buf.readUnsignedShort());
            assertEquals(0, buf.getReadableBytes());
        }
    }

    @Test
    public void testRelativeReadOfUnsignedShortMustNotBoundsCheckWhenReadOffsetAndSizeIsEqualToWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(0, buf.getReadableBytes());
            assertEquals(Long.BYTES, buf.getWritableBytes());
            int value = 0x0102;
            buf.writeUnsignedShort(value);
            assertEquals(2, buf.getReadableBytes());
            assertEquals(6, buf.getWritableBytes());
            assertEquals(value, buf.readUnsignedShort());
            assertEquals(0, buf.getReadableBytes());
        }
    }

    @Test
    public void testRelativeReadOfUnsignedShortMustBoundsCheckWhenReadOffsetAndSizeIsBeyondWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(0, buf.getReadableBytes());
            assertEquals(Long.BYTES, buf.getWritableBytes());
            int value = 0x0102;
            buf.writeUnsignedShort(value);
            buf.setReadOffset(1);
            assertEquals(1, buf.getReadableBytes());
            assertEquals(6, buf.getWritableBytes());
            assertThrows(IndexOutOfBoundsException.class, buf::readUnsignedShort);
            assertEquals(1, buf.getReadableBytes());
        }
    }

    @Test
    public void testRelativeReadOfUnsignedShortReadOnlyMustBoundsCheckWhenReadOffsetAndSizeIsBeyondWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(0, buf.getReadableBytes());
            assertEquals(Long.BYTES, buf.getWritableBytes());
            int value = 0x0102;
            buf.writeUnsignedShort(value);
            buf.setReadOffset(1);
            assertEquals(1, buf.getReadableBytes());
            assertEquals(6, buf.getWritableBytes());
            assertThrows(IndexOutOfBoundsException.class, () -> buf.convertToReadOnly().readUnsignedShort());
            assertEquals(1, buf.getReadableBytes());
        }
    }

    @Test
    public void testRelativeWriteOfShortMustExpandCapacityWhenWriteOffsetAndSizeIsBeyondCapacity() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(Long.BYTES, buf.capacity());
            buf.setWriteOffset(7);
            short value = 0x0102;
            buf.writeShort(value);
            assertTrue(buf.capacity() > Long.BYTES);
            buf.setReadOffset(7);
            assertEquals(value, buf.readShort());
        }
    }

    @Test
    public void testRelativeWriteOfUnsignedShortMustExpandCapacityWhenWriteOffsetAndSizeIsBeyondCapacity() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(Long.BYTES, buf.capacity());
            buf.setWriteOffset(7);
            int value = 0x0102;
            buf.writeUnsignedShort(value);
            assertTrue(buf.capacity() > Long.BYTES);
            buf.setReadOffset(7);
            assertEquals(value, buf.readUnsignedShort());
        }
    }

    @Test
    public void testRelativeReadOfIntMustNotBoundsCheckWhenReadOffsetAndSizeIsEqualToWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(0, buf.getReadableBytes());
            assertEquals(Long.BYTES, buf.getWritableBytes());
            int value = 0x01020304;
            buf.writeInt(value);
            assertEquals(4, buf.getReadableBytes());
            assertEquals(4, buf.getWritableBytes());
            assertEquals(value, buf.readInt());
            assertEquals(0, buf.getReadableBytes());
        }
    }

    @Test
    public void testRelativeReadOfIntMustBoundsCheckWhenReadOffsetAndSizeIsBeyondWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(0, buf.getReadableBytes());
            assertEquals(Long.BYTES, buf.getWritableBytes());
            int value = 0x01020304;
            buf.writeInt(value);
            buf.setReadOffset(1);
            assertEquals(3, buf.getReadableBytes());
            assertEquals(4, buf.getWritableBytes());
            assertThrows(IndexOutOfBoundsException.class, buf::readInt);
            assertEquals(3, buf.getReadableBytes());
        }
    }

    @Test
    public void testRelativeReadOfIntReadOnlyMustBoundsCheckWhenReadOffsetAndSizeIsBeyondWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(0, buf.getReadableBytes());
            assertEquals(Long.BYTES, buf.getWritableBytes());
            int value = 0x01020304;
            buf.writeInt(value);
            buf.setReadOffset(1);
            assertEquals(3, buf.getReadableBytes());
            assertEquals(4, buf.getWritableBytes());
            assertThrows(IndexOutOfBoundsException.class, () -> buf.convertToReadOnly().readInt());
            assertEquals(3, buf.getReadableBytes());
        }
    }

    @Test
    public void testRelativeReadAndGetOfUnsignedInt() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            long value = Integer.MAX_VALUE * 2L;
            buf.setUnsignedInt(0, value);
            assertEquals(value, buf.getUnsignedInt(0));
            buf.writeUnsignedInt(value);
            assertEquals(value, buf.readUnsignedInt());
            assertEquals(0, buf.getReadableBytes());
        }
    }

    @Test
    public void testRelativeReadOfUnsignedIntMustNotBoundsCheckWhenReadOffsetAndSizeIsEqualToWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(0, buf.getReadableBytes());
            assertEquals(Long.BYTES, buf.getWritableBytes());
            long value = 0x01020304;
            buf.writeUnsignedInt(value);
            assertEquals(4, buf.getReadableBytes());
            assertEquals(4, buf.getWritableBytes());
            assertEquals(value, buf.readUnsignedInt());
            assertEquals(0, buf.getReadableBytes());
        }
    }

    @Test
    public void testRelativeReadOfUnsignedIntMustBoundsCheckWhenReadOffsetAndSizeIsBeyondWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(0, buf.getReadableBytes());
            assertEquals(Long.BYTES, buf.getWritableBytes());
            long value = 0x01020304;
            buf.writeUnsignedInt(value);
            buf.setReadOffset(1);
            assertEquals(3, buf.getReadableBytes());
            assertEquals(4, buf.getWritableBytes());
            assertThrows(IndexOutOfBoundsException.class, buf::readUnsignedInt);
            assertEquals(3, buf.getReadableBytes());
        }
    }

    @Test
    public void testRelativeReadOfUnsignedIntReadOnlyMustBoundsCheckWhenReadOffsetAndSizeIsBeyondWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(0, buf.getReadableBytes());
            assertEquals(Long.BYTES, buf.getWritableBytes());
            long value = 0x01020304;
            buf.writeUnsignedInt(value);
            buf.setReadOffset(1);
            assertEquals(3, buf.getReadableBytes());
            assertEquals(4, buf.getWritableBytes());
            assertThrows(IndexOutOfBoundsException.class, () -> buf.convertToReadOnly().readUnsignedInt());
            assertEquals(3, buf.getReadableBytes());
        }
    }

    @Test
    public void testRelativeWriteOfIntMustExpandCapacityWhenWriteOffsetAndSizeIsBeyondCapacity() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(Long.BYTES, buf.capacity());
            buf.setWriteOffset(5);
            int value = 0x01020304;
            buf.writeInt(value);
            assertTrue(buf.capacity() > Long.BYTES);
            buf.setReadOffset(5);
            assertEquals(value, buf.readInt());
        }
    }

    @Test
    public void testRelativeWriteOfUnsignedIntMustExpandCapacityWhenWriteOffsetAndSizeIsBeyondCapacity() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(Long.BYTES, buf.capacity());
            buf.setWriteOffset(5);
            long value = 0x01020304;
            buf.writeUnsignedInt(value);
            assertTrue(buf.capacity() > Long.BYTES);
            buf.setReadOffset(5);
            assertEquals(value, buf.readUnsignedInt());
        }
    }

    @Test
    public void testRelativeReadOfFloatMustNotBoundsCheckWhenReadOffsetAndSizeIsEqualToWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(0, buf.getReadableBytes());
            assertEquals(Long.BYTES, buf.getWritableBytes());
            float value = Float.intBitsToFloat(0x01020304);
            buf.writeFloat(value);
            assertEquals(4, buf.getReadableBytes());
            assertEquals(4, buf.getWritableBytes());
            assertEquals(value, buf.readFloat());
            assertEquals(0, buf.getReadableBytes());
        }
    }

    @Test
    public void testRelativeReadOfFloatMustBoundsCheckWhenReadOffsetAndSizeIsBeyondWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(0, buf.getReadableBytes());
            assertEquals(Long.BYTES, buf.getWritableBytes());
            float value = Float.intBitsToFloat(0x01020304);
            buf.writeFloat(value);
            buf.setReadOffset(1);
            assertEquals(3, buf.getReadableBytes());
            assertEquals(4, buf.getWritableBytes());
            assertThrows(IndexOutOfBoundsException.class, buf::readFloat);
            assertEquals(3, buf.getReadableBytes());
        }
    }

    @Test
    public void testRelativeReadOfFloatReadOnlyMustBoundsCheckWhenReadOffsetAndSizeIsBeyondWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(0, buf.getReadableBytes());
            assertEquals(Long.BYTES, buf.getWritableBytes());
            float value = Float.intBitsToFloat(0x01020304);
            buf.writeFloat(value);
            buf.setReadOffset(1);
            assertEquals(3, buf.getReadableBytes());
            assertEquals(4, buf.getWritableBytes());
            assertThrows(IndexOutOfBoundsException.class, () -> buf.convertToReadOnly().readFloat());
            assertEquals(3, buf.getReadableBytes());
        }
    }

    @Test
    public void testRelativeWriteOfFloatMustExpandCapacityWhenWriteOffsetAndSizeIsBeyondCapacity() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(Long.BYTES, buf.capacity());
            buf.setWriteOffset(5);
            float value = Float.intBitsToFloat(0x01020304);
            buf.writeFloat(value);
            assertTrue(buf.capacity() > Long.BYTES);
            buf.setReadOffset(5);
            assertEquals(value, buf.readFloat());
        }
    }

    @Test
    public void testRelativeReadOfLongMustNotBoundsCheckWhenReadOffsetAndSizeIsEqualToWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(0, buf.getReadableBytes());
            assertEquals(Long.BYTES, buf.getWritableBytes());
            long value = 0x0102030405060708L;
            buf.writeLong(value);
            assertEquals(8, buf.getReadableBytes());
            assertEquals(0, buf.getWritableBytes());
            assertEquals(value, buf.readLong());
            assertEquals(0, buf.getReadableBytes());
        }
    }

    @Test
    public void testRelativeReadOfLongMustBoundsCheckWhenReadOffsetAndSizeIsBeyondWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(0, buf.getReadableBytes());
            assertEquals(Long.BYTES, buf.getWritableBytes());
            long value = 0x0102030405060708L;
            buf.writeLong(value);
            buf.setReadOffset(1);
            assertEquals(7, buf.getReadableBytes());
            assertEquals(0, buf.getWritableBytes());
            assertThrows(IndexOutOfBoundsException.class, buf::readLong);
            assertEquals(7, buf.getReadableBytes());
        }
    }

    @Test
    public void testRelativeReadOfLongReadOnlyMustBoundsCheckWhenReadOffsetAndSizeIsBeyondWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(0, buf.getReadableBytes());
            assertEquals(Long.BYTES, buf.getWritableBytes());
            long value = 0x0102030405060708L;
            buf.writeLong(value);
            buf.setReadOffset(1);
            assertEquals(7, buf.getReadableBytes());
            assertEquals(0, buf.getWritableBytes());
            assertThrows(IndexOutOfBoundsException.class, () -> buf.convertToReadOnly().readLong());
            assertEquals(7, buf.getReadableBytes());
        }
    }

    @Test
    public void testRelativeWriteOfLongMustExpandCapacityWhenWriteOffsetAndSizeIsBeyondCapacity() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(Long.BYTES, buf.capacity());
            buf.setWriteOffset(1);
            long value = 0x0102030405060708L;
            buf.writeLong(value);
            assertTrue(buf.capacity() > Long.BYTES);
            buf.setReadOffset(1);
            assertEquals(value, buf.readLong());
        }
    }

    @Test
    public void testRelativeReadOfDoubleMustNotBoundsCheckWhenReadOffsetAndSizeIsEqualToWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(0, buf.getReadableBytes());
            assertEquals(Long.BYTES, buf.getWritableBytes());
            double value = Double.longBitsToDouble(0x0102030405060708L);
            buf.writeDouble(value);
            assertEquals(8, buf.getReadableBytes());
            assertEquals(0, buf.getWritableBytes());
            assertEquals(value, buf.readDouble());
            assertEquals(0, buf.getReadableBytes());
        }
    }

    @Test
    public void testRelativeReadOfShortMustReadWithDefaultEndianByteOrder() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(0, buf.getReadableBytes());
            assertEquals(Long.BYTES, buf.getWritableBytes());
            short value = 0x0102;
            buf.writeShort(value);
            buf.setByte(0, (byte) 0x10);
            assertEquals(2, buf.getReadableBytes());
            assertEquals(6, buf.getWritableBytes());
            assertEquals(0x1002, buf.readShort());
            assertEquals(0, buf.getReadableBytes());
        }
    }

    @Test
    public void testRelativeReadOfUnsignedShortMustReadWithDefaultEndianByteOrder() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(0, buf.getReadableBytes());
            assertEquals(Long.BYTES, buf.getWritableBytes());
            int value = 0x0102;
            buf.writeUnsignedShort(value);
            buf.setByte(0, (byte) 0x10);
            assertEquals(2, buf.getReadableBytes());
            assertEquals(6, buf.getWritableBytes());
            assertEquals(0x1002, buf.readUnsignedShort());
            assertEquals(0, buf.getReadableBytes());
        }
    }

    @Test
    public void testRelativeWriteOfShortMustHaveDefaultEndianByteOrder() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            short value = 0x0102;
            buf.writeShort(value);
            buf.setWriteOffset(Long.BYTES);
            assertEquals((byte) 0x01, buf.readByte());
            assertEquals((byte) 0x02, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
        }
    }

    @Test
    public void testRelativeWriteOfUnsignedShortMustHaveDefaultEndianByteOrder() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            int value = 0x0102;
            buf.writeUnsignedShort(value);
            buf.setWriteOffset(Long.BYTES);
            assertEquals((byte) 0x01, buf.readByte());
            assertEquals((byte) 0x02, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
        }
    }

    @Test
    public void testRelativeReadOfIntMustReadWithDefaultEndianByteOrder() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(0, buf.getReadableBytes());
            assertEquals(Long.BYTES, buf.getWritableBytes());
            int value = 0x01020304;
            buf.writeInt(value);
            buf.setByte(0, (byte) 0x10);
            assertEquals(4, buf.getReadableBytes());
            assertEquals(4, buf.getWritableBytes());
            assertEquals(0x10020304, buf.readInt());
            assertEquals(0, buf.getReadableBytes());
        }
    }

    @Test
    public void testRelativeReadOfDoubleMustBoundsCheckWhenReadOffsetAndSizeIsBeyondWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(0, buf.getReadableBytes());
            assertEquals(Long.BYTES, buf.getWritableBytes());
            double value = Double.longBitsToDouble(0x0102030405060708L);
            buf.writeDouble(value);
            buf.setReadOffset(1);
            assertEquals(7, buf.getReadableBytes());
            assertEquals(0, buf.getWritableBytes());
            assertThrows(IndexOutOfBoundsException.class, buf::readDouble);
            assertEquals(7, buf.getReadableBytes());
        }
    }

    @Test
    public void testRelativeReadOfDoubleReadOnlyMustBoundsCheckWhenReadOffsetAndSizeIsBeyondWriteOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(0, buf.getReadableBytes());
            assertEquals(Long.BYTES, buf.getWritableBytes());
            double value = Double.longBitsToDouble(0x0102030405060708L);
            buf.writeDouble(value);
            buf.setReadOffset(1);
            assertEquals(7, buf.getReadableBytes());
            assertEquals(0, buf.getWritableBytes());
            assertThrows(IndexOutOfBoundsException.class, () -> buf.convertToReadOnly().readDouble());
            assertEquals(7, buf.getReadableBytes());
        }
    }

    @Test
    public void testRelativeReadOfCharMustReadWithDefaultEndianByteOrder() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(0, buf.getReadableBytes());
            assertEquals(Long.BYTES, buf.getWritableBytes());
            char value = 0x0102;
            buf.writeChar(value);
            buf.setByte(0, (byte) 0x10);
            assertEquals(2, buf.getReadableBytes());
            assertEquals(6, buf.getWritableBytes());
            assertEquals(0x1002, buf.readChar());
            assertEquals(0, buf.getReadableBytes());
        }
    }

    @Test
    public void testRelativeWriteOfDoubleMustExpandCapacityWhenWriteOffsetAndSizeIsBeyondCapacity() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertEquals(Long.BYTES, buf.capacity());
            buf.setWriteOffset(1);
            double value = Double.longBitsToDouble(0x0102030405060708L);
            buf.writeDouble(value);
            assertTrue(buf.capacity() > Long.BYTES);
            buf.setReadOffset(1);
            assertEquals(value, buf.readDouble());
        }
    }

    @Test
    public void testRelativeWriteOfCharMustHaveDefaultEndianByteOrder() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            char value = 0x0102;
            buf.writeChar(value);
            buf.setWriteOffset(Long.BYTES);
            assertEquals((byte) 0x01, buf.readByte());
            assertEquals((byte) 0x02, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
        }
    }

    @Test
    public void testRelativeWriteOfIntMustHaveDefaultEndianByteOrder() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            int value = 0x01020304;
            buf.writeInt(value);
            buf.setWriteOffset(Long.BYTES);
            assertEquals((byte) 0x01, buf.readByte());
            assertEquals((byte) 0x02, buf.readByte());
            assertEquals((byte) 0x03, buf.readByte());
            assertEquals((byte) 0x04, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
        }
    }

    @Test
    public void testRelativeWriteOfUnsignedIntMustHaveDefaultEndianByteOrder() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            long value = 0x01020304;
            buf.writeUnsignedInt(value);
            buf.setWriteOffset(Long.BYTES);
            assertEquals((byte) 0x01, buf.readByte());
            assertEquals((byte) 0x02, buf.readByte());
            assertEquals((byte) 0x03, buf.readByte());
            assertEquals((byte) 0x04, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
        }
    }

    @Test
    public void testRelativeWriteOfFloatMustHaveDefaultEndianByteOrder() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            float value = Float.intBitsToFloat(0x01020304);
            buf.writeFloat(value);
            buf.setWriteOffset(Long.BYTES);
            assertEquals((byte) 0x01, buf.readByte());
            assertEquals((byte) 0x02, buf.readByte());
            assertEquals((byte) 0x03, buf.readByte());
            assertEquals((byte) 0x04, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
            assertEquals((byte) 0x00, buf.readByte());
        }
    }

    @Test
    public void testRelativeWriteOfLongMustHaveDefaultEndianByteOrder() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            long value = 0x0102030405060708L;
            buf.writeLong(value);
            buf.setWriteOffset(Long.BYTES);
            assertEquals((byte) 0x01, buf.readByte());
            assertEquals((byte) 0x02, buf.readByte());
            assertEquals((byte) 0x03, buf.readByte());
            assertEquals((byte) 0x04, buf.readByte());
            assertEquals((byte) 0x05, buf.readByte());
            assertEquals((byte) 0x06, buf.readByte());
            assertEquals((byte) 0x07, buf.readByte());
            assertEquals((byte) 0x08, buf.readByte());
        }
    }

    @Test
    public void testRelativeWriteOfDoubleMustHaveDefaultEndianByteOrder() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            double value = Double.longBitsToDouble(0x0102030405060708L);
            buf.writeDouble(value);
            buf.setWriteOffset(Long.BYTES);
            assertEquals((byte) 0x01, buf.readByte());
            assertEquals((byte) 0x02, buf.readByte());
            assertEquals((byte) 0x03, buf.readByte());
            assertEquals((byte) 0x04, buf.readByte());
            assertEquals((byte) 0x05, buf.readByte());
            assertEquals((byte) 0x06, buf.readByte());
            assertEquals((byte) 0x07, buf.readByte());
            assertEquals((byte) 0x08, buf.readByte());
        }
    }

    @Test
    public void testWriteBytesFromCompositeIntoContiguousBuffer() {
        try (ProtonBufferAllocator defaultAlloc = ProtonBufferAllocator.defaultAllocator();
             ProtonBufferAllocator alloc = createTestCaseAllocator()) {

            ProtonBuffer buffer1 = defaultAlloc.allocate(16);
            ProtonBuffer buffer2 = defaultAlloc.allocate(16);
            ProtonBuffer buffer3 = defaultAlloc.allocate(16);

            buffer1.fill((byte)'a').setWriteOffset(buffer1.capacity()).convertToReadOnly();
            buffer2.fill((byte)'b').setWriteOffset(buffer2.capacity()).convertToReadOnly();
            buffer3.fill((byte)'c').setWriteOffset(buffer3.capacity()).convertToReadOnly();

            final int expectedCapacity = buffer1.capacity() + buffer2.capacity() + buffer3.capacity();

            try (ProtonCompositeBuffer composite = alloc.composite(
                    new ProtonBuffer[] { buffer1.copy(true), buffer2.copy(true), buffer3.copy(true) });
                 ProtonBuffer target = alloc.allocate(composite.capacity())) {

                assertEquals(expectedCapacity, composite.capacity());

                target.writeBytes(composite);

                assertEquals(target.capacity(), target.getWriteOffset());

                for (int i = 0, j = 0; j < buffer1.capacity(); ++i, ++j) {
                    assertEquals(target.getByte(i), buffer1.getByte(j),
                        "Wrong value detected in target[" + i + "] expecting buffer1[" + j + "]");
                }

                for (int i = buffer1.capacity(), j = 0; j < buffer2.capacity(); ++i, ++j) {
                    assertEquals(target.getByte(i), buffer2.getByte(j),
                        "Wrong value detected in target[" + i + "] expecting buffer2[" + j + "]");
                }

                for (int i = buffer2.capacity() + buffer1.capacity(), j = 0; j < buffer3.capacity(); ++i, ++j) {
                    assertEquals(target.getByte(i), buffer3.getByte(j),
                        "Wrong value detected in target[" + i + "] expecting buffer3[" + j + "]");
                }
            }
        }
    }

    @Test
    public void testSplitBufferAndCloseSplitPortionLeavesOriginalPortionAccessible() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(16)) {
            buf.writeLong(0x0102030405060708l);
            buf.writeLong(0x0807060504030201l);

            final ProtonBuffer front = buf.split(8); // split the two longs

            try (front) {
                assertEquals(8, front.getReadableBytes());
                assertEquals(8, front.capacity());
                assertEquals(8, buf.getReadableBytes());
                assertEquals(8, buf.capacity());
            }

            verifyInaccessible(front);

            assertEquals(0x0807060504030201l, buf.readLong());
        }
    }

    @Test
    public void testSplitBufferAndCloseRemainingPortionLeavesTheSplitPortionAccessible() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(16)) {
            buf.writeLong(0x0102030405060708l);
            buf.writeLong(0x0807060504030201l);

            final ProtonBuffer front = buf.split(8); // split the two longs

            try (buf) {
                assertEquals(8, front.getReadableBytes());
                assertEquals(8, front.capacity());
                assertEquals(8, buf.getReadableBytes());
                assertEquals(8, buf.capacity());
            }

            verifyInaccessible(buf);

            assertEquals(0x0102030405060708l, front.readLong());
        }
    }

    @Test
    public void testTransferOwnerShipLeavesOriginalInClosedState() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(16)) {
            buf.writeLong(0x0102030405060708l);
            buf.writeLong(0x0807060504030201l);

            try (ProtonBuffer transfer = buf.transfer()) {
                verifyInaccessible(buf);

                assertEquals(16, transfer.getReadableBytes());
                assertEquals(16, transfer.capacity());

                assertEquals(0x0102030405060708l, transfer.readLong());
                assertEquals(0x0807060504030201l, transfer.readLong());
            }
        }
    }

    @Test
    public void testTransferOwnerShipLeavesOriginalReadOnlyState() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(16)) {
            buf.writeLong(0x0102030405060708l);
            buf.writeLong(0x0807060504030201l);
            buf.convertToReadOnly();

            try (ProtonBuffer transfer = buf.transfer()) {
                verifyInaccessible(buf);

                assertEquals(16, transfer.getReadableBytes());
                assertEquals(16, transfer.capacity());
                assertTrue(transfer.isReadOnly());

                assertEquals(0x0102030405060708l, transfer.readLong());
                assertEquals(0x0807060504030201l, transfer.readLong());
            }
        }
    }

    @Test
    public void testCopy() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buffer = allocator.allocate(LARGE_CAPACITY)) {
            for (int i = 0; i < buffer.capacity(); i ++) {
                byte value = (byte) random.nextInt();
                buffer.setByte(i, value);
            }

            final int readerIndex = LARGE_CAPACITY / 3;
            final int writerIndex = LARGE_CAPACITY * 2 / 3;
            buffer.setWriteOffset(writerIndex);
            buffer.setReadOffset(readerIndex);

            // Make sure all properties are copied.
            ProtonBuffer copy = buffer.copy();
            assertEquals(0, copy.getReadOffset());
            assertEquals(buffer.getReadableBytes(), copy.getWriteOffset());
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
    }

    @Test
    public void testSequentialRandomFilledBufferIndexedCopy() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buffer = allocator.allocate(LARGE_CAPACITY)) {
            byte[] value = new byte[BLOCK_SIZE];
            for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
                random.nextBytes(value);
                buffer.setWriteOffset(i);
                buffer.writeBytes(value);
            }

            random.setSeed(seed);

            for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
                final byte[] expectedValueContent = new byte[BLOCK_SIZE];
                random.nextBytes(expectedValueContent);

                final ProtonBuffer expectedValue = allocator.copy(expectedValueContent);

                ProtonBuffer copy = buffer.copy(i, BLOCK_SIZE);
                for (int j = 0; j < BLOCK_SIZE; j ++) {
                    assertEquals(expectedValue.getByte(j), copy.getByte(j));
                }
            }
        }
    }

    @Test
    public void testWriteBytesAfterMultipleBufferExpansions() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buffer = allocator.allocate(8)) {
            buffer.ensureWritable(16);
            buffer.ensureWritable(32);

            final byte[] data = new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08 };

            buffer.setWriteOffset(24);
            buffer.writeBytes(data);

            assertEquals(32, buffer.getReadableBytes());

            buffer.setReadOffset(24);

            assertEquals(0x0102030405060708l, buffer.readLong());
        }
    }

    @Test
    public void testWriteBytesFromProtonBufferAfterMultipleBufferExpansions() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator();
             ProtonBuffer buffer = allocator.allocate(8);
             ProtonBuffer input = ProtonBufferAllocator.defaultAllocator().allocate(8)) {

            input.writeLong(0x0102030405060708l);

            buffer.ensureWritable(16);
            buffer.ensureWritable(32);

            buffer.setWriteOffset(24);
            buffer.writeBytes(input);

            assertEquals(32, buffer.getReadableBytes());

            buffer.setReadOffset(24);

            assertEquals(0x0102030405060708l, buffer.readLong());
        }
    }

    @Test
    public void testTransferToMustThrowIfBufferIsClosed() throws IOException {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator()) {
            final long position = channel.position();
            final long size = channel.size();
            ProtonBuffer empty = allocator.allocate(8);
            empty.close();
            assertThrows(ProtonBufferClosedException.class, () -> empty.transferTo(channel, 8));
            assertEquals(position, channel.position());
            assertEquals(size, channel.size());
            ProtonBuffer withData = allocator.allocate(8);
            withData.writeLong(0x0102030405060708L);
            withData.close();
            assertThrows(ProtonBufferClosedException.class, () -> withData.transferTo(channel, 8));
            assertEquals(position, channel.position());
            assertEquals(size, channel.size());

            if (withData.isComposite()) {
                assertEquals(0, withData.componentCount());
            } else {
                assertEquals(1, withData.componentCount());
            }
            assertEquals(0, withData.readableComponentCount());
            assertEquals(0, withData.writableComponentCount());
        }
    }

    @Test
    public void testTransferToMustCapAtReadableBytes() throws IOException {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buffer = allocator.allocate(8)) {
            buffer.writeLong(0x0102030405060708L);
            buffer.setWriteOffset(buffer.getWriteOffset() - 5);
            long position = channel.position();
            long size = channel.size();
            int bytesWritten = buffer.transferTo(channel, 8);

            assertEquals(3, bytesWritten);
            assertEquals(position + 3, channel.position());
            assertEquals(size + 3, channel.size());
            assertEquals(5, buffer.getWritableBytes());
            assertEquals(0, buffer.getReadableBytes());
        }
    }

    @Test
    public void testTransferToMustCapAtLength() throws IOException {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buffer = allocator.allocate(8)) {
            buffer.writeLong(0x0102030405060708L);
            long position = channel.position();
            long size = channel.size();
            int bytesWritten = buffer.transferTo(channel, 3);

            assertEquals(3, bytesWritten);
            assertEquals(position + 3, channel.position());
            assertEquals(size + 3, channel.size());
            assertEquals(0, buffer.getWritableBytes());
            assertEquals(5, buffer.getReadableBytes());
        }
    }

    @Test
    public void testTransferToMustThrowIfChannelIsClosed() throws IOException {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buffer = allocator.allocate(8)) {
            buffer.writeLong(0x0102030405060708L);
            assertThrows(ClosedChannelException.class, () -> buffer.transferTo(closedChannel, 8));
            assertFalse(buffer.isClosed());
            assertEquals(8, buffer.getReadableBytes());
        }
    }

    @Test
    public void testTransferToMustThrowIfChannelIsNull() throws IOException {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buffer = allocator.allocate(8)) {
            buffer.writeLong(0x0102030405060708L);
            assertThrows(NullPointerException.class, () -> buffer.transferTo(null, 8));
            assertFalse(buffer.isClosed());
            assertEquals(8, buffer.getReadableBytes());
        }
    }

    @Test
    public void testTransferToMustThrowIfLengthIsNegative() throws IOException {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buffer = allocator.allocate(8)) {
            buffer.writeLong(0x0102030405060708L);
            assertThrows(IllegalArgumentException.class, () -> buffer.transferTo(channel, -1));
            assertEquals(8, buffer.getReadableBytes());
        }
    }

    @Test
    public void testTransferToMustIgnoreZeroLengthOperations() throws IOException {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buffer = allocator.allocate(8)) {
            long position = channel.position();
            long size = channel.size();
            buffer.writeLong(0x0102030405060708L);
            int bytesWritten = buffer.transferTo(channel, 0);

            assertEquals(0, bytesWritten);
            assertEquals(position, channel.position());
            assertEquals(size, channel.size());
            assertEquals(8, buffer.getReadableBytes());
        }
    }

    @Test
    public void testTransferToMustMoveDataToChannel() throws IOException {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buffer = allocator.allocate(8)) {
            long value = random.nextLong();
            buffer.writeLong(value);
            long position = channel.position();
            int bytesWritten = buffer.transferTo(channel, 8);
            assertEquals(8, bytesWritten);
            ByteBuffer nio = ByteBuffer.allocate(8);
            int bytesRead = channel.read(nio, position);
            assertEquals(8, bytesRead);
            nio.flip();
            assertEquals(value, nio.getLong());
        }
    }

    @Test
    public void testTransferToMustMoveCompositeDataToChannel() throws IOException {
        final int value1 = random.nextInt();
        final int value2 = random.nextInt();

        try (ProtonBufferAllocator allocator = createTestCaseAllocator();
             ProtonBuffer buffer1 = allocator.allocate(4).writeInt(value1);
             ProtonBuffer buffer2 = allocator.allocate(4).writeInt(value2);
             ProtonBuffer composite = allocator.composite(new ProtonBuffer[] { buffer1, buffer2})) {

            long position = channel.position();
            int bytesWritten = composite.transferTo(channel, 8);
            assertEquals(8, bytesWritten);

            ByteBuffer nio = ByteBuffer.allocate(8);
            int bytesRead = channel.read(nio, position);
            assertEquals(8, bytesRead);
            nio.flip();
            assertEquals(value1, nio.getInt());
            assertEquals(value2, nio.getInt());
        }
    }

    @Test
    public void testTransferToMustFunctionOnReadOnlyBuffers() throws IOException {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buffer = allocator.allocate(8)) {
            long value = random.nextLong();
            buffer.writeLong(value).convertToReadOnly();
            long position = channel.position();
            int bytesWritten = buffer.transferTo(channel, 8);
            assertEquals(8, bytesWritten);
            ByteBuffer nio = ByteBuffer.allocate(8);
            int bytesRead = channel.read(nio, position);
            assertEquals(8, bytesRead);
            nio.flip();
            assertEquals(value, nio.getLong());
        }
    }

    @Test
    public void testTransferToZeroBytesMustNotThrowOnClosedChannel() throws IOException {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator();
             ProtonBuffer empty = allocator.allocate(0);
             ProtonBuffer notEmpty = allocator.allocate(4).writeInt(42)) {

            empty.transferTo(closedChannel, 4);
            notEmpty.transferTo(closedChannel, 0);
        }
    }

    @Test
    public void testPrtialFailureOfTransferToMustKeepChannelAndBufferPositionsInSync(@TempDir Path parentDir) throws IOException {
        ProtonBufferAllocator allocator = ProtonBufferAllocator.defaultAllocator();
        Path path = parentDir.resolve("transferTo");
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
             ProtonBuffer buf = allocator.composite(
                 new ProtonBuffer[] { allocator.allocate(4).writeInt(0x01020304), allocator.allocate(4).writeInt(0x05060708) })) {

            WritableByteChannel channelWrapper = new WritableByteChannel() {
                private boolean pastFirstCall;

                @Override
                public int write(ByteBuffer src) throws IOException {
                    if (pastFirstCall) {
                        throw new IOException("boom");
                    }
                    pastFirstCall = true;
                    return channel.write(src);
                }

                @Override
                public boolean isOpen() {
                    return channel.isOpen();
                }

                @Override
                public void close() throws IOException {
                    channel.close();
                }
            };

            long position = channel.position();
            long size = channel.size();
            Exception e = assertThrows(IOException.class, () -> buf.transferTo(channelWrapper, 8));
            assertTrue(e.getMessage().contains("boom"));

            assertEquals(position + 4, channel.position());
            assertEquals(size + 4, channel.size());
            assertEquals(4, buf.getReadOffset());
            assertEquals(4, buf.getReadableBytes());
        }
    }

    @Test
    public void testTransferFromMustThrowIfBufferIsClosed() throws IOException {
        doTestTransferFromMustThrowIfBufferIsClosed(false);
    }

    @Test
    public void testTransferFromWithPositionMustThrowIfBufferIsClosed() throws IOException {
        doTestTransferFromMustThrowIfBufferIsClosed(true);
    }

    private void doTestTransferFromMustThrowIfBufferIsClosed(boolean withPosition) throws IOException {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator()) {
            ByteBuffer data = ByteBuffer.allocate(8).putLong(0x0102030405060708L).flip();
            long position = channel.position();
            assertEquals(8, channel.write(data, position));
            long size = channel.size();
            ProtonBuffer empty = allocator.allocate(0);
            empty.close();
            assertThrows(ProtonBufferClosedException.class, () -> {
                if (withPosition) {
                    empty.transferFrom(channel, 4 + position, 8);
                } else {
                    empty.transferFrom(channel, 8);
                }
            });

            assertEquals(position, channel.position());
            assertEquals(size, channel.size());

            ProtonBuffer withAvailableSpace = allocator.allocate(8);
            withAvailableSpace.close();
            assertThrows(ProtonBufferClosedException.class, () -> {
                if (withPosition) {
                    withAvailableSpace.transferFrom(channel, 4 + position, 8);
                } else {
                    withAvailableSpace.transferFrom(channel, 8);
                }
            });

            assertEquals(position, channel.position());
            assertEquals(size, channel.size());
        } finally {
            channel.position(channel.size());
        }
    }

    @Test
    public void testTransferFromMustCapAtWritableBytes() throws IOException {
        doTestTransferFromMustCapAtWritableBytes(false);
    }

    @Test
    public void testTransferFromWithPositionMustCapAtWritableBytes() throws IOException {
        doTestTransferFromMustCapAtWritableBytes(true);
    }

    private void doTestTransferFromMustCapAtWritableBytes(boolean withPosition) throws IOException {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator();
             ProtonBuffer buffer = allocator.allocate(3)) {
            ByteBuffer data = ByteBuffer.allocate(8).putLong(0x0102030405060708L).flip();
            long position = channel.position();
            assertEquals(8, channel.write(data, position));
            long size = channel.size();

            final int bytesRead = withPosition ? buffer.transferFrom(channel, 4 + position, 8) :
                                                 buffer.transferFrom(channel, 8);

            assertEquals(3, bytesRead);
            assertEquals(withPosition ? position : 3 + position, channel.position());
            assertEquals(size, channel.size());
            assertEquals(0, buffer.getWritableBytes());
            assertEquals(3, buffer.getReadableBytes());
        } finally {
            channel.position(channel.size());
        }
    }

    @Test
    public void testTransferFromMustCapAtLength() throws IOException {
        doTestTransferFromMustCapAtLength(false);
    }

    @Test
    public void testTransferFromWithPositionMustCapAtLength() throws IOException {
        doTestTransferFromMustCapAtLength(true);
    }

    private void doTestTransferFromMustCapAtLength(boolean withPosition) throws IOException {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator();
             ProtonBuffer buffer = allocator.allocate(8)) {
            ByteBuffer data = ByteBuffer.allocate(8).putLong(0x0102030405060708L).flip();
            long position = channel.position();
            assertEquals(8, channel.write(data, position));
            long size = channel.size();
            int bytesRead = withPosition ? buffer.transferFrom(channel, 4 + position, 3) :
                                           buffer.transferFrom(channel, 3);

            assertEquals(3, bytesRead);
            assertEquals(withPosition ? position : 3 + position, channel.position());
            assertEquals(size, channel.size());
            assertEquals(5, buffer.getWritableBytes());
            assertEquals(3, buffer.getReadableBytes());
        } finally {
            channel.position(channel.size());
        }
    }

    @Test
    public void testTransferFromMustThrowIfChannelIsClosed() {
        doTestTransferFromMustThrowIfChannelIsClosed(false);
    }

    @Test
    public void testTransferFromWithPositionMustThrowIfChannelIsClosed() {
        doTestTransferFromMustThrowIfChannelIsClosed(true);
    }

    private void doTestTransferFromMustThrowIfChannelIsClosed(boolean withPosition) {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator();
             ProtonBuffer buffer = allocator.allocate(8)) {

            assertThrows(ClosedChannelException.class, () -> {
                if (withPosition) {
                    buffer.transferFrom(closedChannel, 4, 8);
                } else {
                    buffer.transferFrom(closedChannel, 8);
                }
            });

            assertFalse(buffer.isClosed());
            assertEquals(8, buffer.getWritableBytes());
            assertEquals(0, buffer.getReadableBytes());
        }
    }

    @Test
    public void testTransferFromMustThrowIfChannelIsNull() {
        doTestTransferFromMustThrowIfChannelIsNull(false);
    }

    @Test
    public void testTransferFromWithPositionMustThrowIfChannelIsNull() {
        doTestTransferFromMustThrowIfChannelIsNull(true);
    }

    private void doTestTransferFromMustThrowIfChannelIsNull(boolean withPosition) {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator();
             ProtonBuffer buffer = allocator.allocate(8)) {

            assertThrows(NullPointerException.class, () -> {
                if (withPosition) {
                    buffer.transferFrom(null, 4, 8);
                } else {
                    buffer.transferFrom(null, 8);
                }
            });

            assertFalse(buffer.isClosed());
            assertEquals(8, buffer.getWritableBytes());
            assertEquals(0, buffer.getReadableBytes());
        }
    }

    @Test
    public void testTransferFromMustThrowIfLengthIsNegative() throws IOException {
        doTestTransferFromMustThrowIfLengthIsNegative(false);
    }

    @Test
    public void testTransferFromWithPositionMustThrowIfLengthIsNegative() throws IOException {
        doTestTransferFromMustThrowIfLengthIsNegative(true);
    }

    private void doTestTransferFromMustThrowIfLengthIsNegative(boolean withPosition) throws IOException {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator();
             ProtonBuffer buffer = allocator.allocate(8)) {

            long position = channel.position();
            ByteBuffer data = ByteBuffer.allocate(8).putLong(0x0102030405060708L).flip();
            assertEquals(8, channel.write(data, position));
            long size = channel.size();
            assertThrows(IllegalArgumentException.class, () -> {
                if (withPosition) {
                    buffer.transferFrom(channel, 4 + position, -1);
                } else {
                    buffer.transferFrom(channel, -1);
                }
            });
            assertFalse(buffer.isClosed());
            assertEquals(8, buffer.getWritableBytes());
            assertEquals(0, buffer.getReadableBytes());
            assertEquals(position, channel.position());
            assertEquals(size, channel.size());
        } finally {
            channel.position(channel.size());
        }
    }

    @Test
    public void testTransferFromMustIgnoreZeroLengthOperations() throws IOException {
        doTestTransferFromMustIgnoreZeroLengthOperations(false);
    }

    @Test
    public void testTransferFromWithPositionMustIgnoreZeroLengthOperations() throws IOException {
        doTestTransferFromMustIgnoreZeroLengthOperations(true);
    }

    private void doTestTransferFromMustIgnoreZeroLengthOperations(boolean withPosition) throws IOException {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator();
             ProtonBuffer buffer = allocator.allocate(8)) {

            long position = channel.position();
            ByteBuffer data = ByteBuffer.allocate(8).putLong(0x0102030405060708L).flip();
            assertEquals(8, channel.write(data, position));
            long size = channel.size();
            int bytesRead = withPosition ? buffer.transferFrom(channel, 4 + position, 0) : buffer.transferFrom(channel, 0);

            assertFalse(buffer.isClosed());
            assertEquals(0, bytesRead);

            assertEquals(8, buffer.getWritableBytes());
            assertEquals(0, buffer.getReadableBytes());
            assertEquals(position, channel.position());
            assertEquals(size, channel.size());
        } finally {
            channel.position(channel.size());
        }
    }

    @Test
    public void testTransferFromMustMoveDataFromChannel() throws IOException {
        doTestTransferFromMustMoveDataFromChannel(false);
    }

    @Test
    public void testTransferFromWithPositionMustMoveDataFromChannel() throws IOException {
        doTestTransferFromMustMoveDataFromChannel(true);
    }

    private void doTestTransferFromMustMoveDataFromChannel(boolean withPosition) throws IOException {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator();
             ProtonBuffer buffer = allocator.allocate(8)) {

            long value = random.nextLong();
            ByteBuffer data = ByteBuffer.allocate(8).putLong(value).flip();
            long position = channel.position();
            assertEquals(8, channel.write(data, position));

            long size = channel.size();
            int bytesRead = withPosition ? buffer.transferFrom(channel, position, 8) : buffer.transferFrom(channel, 8);
            assertEquals(8, bytesRead);

            assertEquals(withPosition ? position : 8 + position,  channel.position());
            assertEquals(size, channel.size());

            assertEquals(0, buffer.getWritableBytes());
            assertEquals(8, buffer.getReadableBytes());

            assertEquals(value, buffer.readLong());
        } finally {
            channel.position(channel.size());
        }
    }

    @Test
    public void testTransferFromMustNotReadBeyondEndOfChannel() throws IOException {
        doTestTransferFromMustNotReadBeyondEndOfChannel(false);
    }

    @Test
    public void testTransferFromWithPositionMustNotReadBeyondEndOfChannel() throws IOException {
        doTestTransferFromMustNotReadBeyondEndOfChannel(true);
    }

    private void doTestTransferFromMustNotReadBeyondEndOfChannel(boolean withPosition) throws IOException {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator();
             ProtonBuffer buffer = allocator.allocate(8)) {

            ByteBuffer data = ByteBuffer.allocate(8).putInt(0x01020304).flip();
            long position = channel.position();

            assertEquals(4, channel.write(data, position));
            long size = channel.size();
            int bytesRead = withPosition ? buffer.transferFrom(channel, position, 8) : buffer.transferFrom(channel, 8);

            assertEquals(4, bytesRead);
            assertEquals(4, buffer.getReadableBytes());

            assertEquals(withPosition ? position : 4 + position,  channel.position());
            assertEquals(size, channel.size());
        } finally {
            channel.position(channel.size());
        }
    }

    @Test
    public void testTransferFromMustReturnMinusOneForEndOfStream() throws IOException {
        doTestTransferFromMustReturnMinusOneForEndOfStream(false);
    }

    @Test
    public void testTransferFromWithPositionMustReturnMinusOneForEndOfStream() throws IOException {
        doTestTransferFromMustReturnMinusOneForEndOfStream(true);
    }

    private void doTestTransferFromMustReturnMinusOneForEndOfStream(boolean withPosition) throws IOException {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator();
             ProtonBuffer buffer = allocator.allocate(8)) {

            final long position = channel.position();
            final long size = channel.size();
            final int bytesRead = withPosition ? buffer.transferFrom(channel, position, 8) : buffer.transferFrom(channel, 8);

            assertEquals(-1, bytesRead);
            assertEquals(8, buffer.getWritableBytes());
            assertEquals(0, buffer.getReadableBytes());

            assertEquals(position, channel.position());
            assertEquals(size, channel.size());
        } finally {
            channel.position(channel.size());
        }
    }

    @Test
    public void testTransferFromMustReturnMinusOneForEndOfStreamNonScattering() throws IOException {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator();
             ProtonBuffer buffer = allocator.allocate(8)) {

            final long position = channel.position();
            final long size = channel.size();
            final ReadableByteChannel nonScatteringChannel = new ReadableByteChannel() {
                @Override
                public int read(ByteBuffer dst) throws IOException {
                    return channel.read(dst);
                }

                @Override
                public boolean isOpen() {
                    return channel.isOpen();
                }

                @Override
                public void close() throws IOException {
                    channel.close();
                }
            };

            int bytesRead = buffer.transferFrom(nonScatteringChannel, 8);
            assertEquals(-1, bytesRead);
            assertEquals(8, buffer.getWritableBytes());
            assertEquals(0, buffer.getReadableBytes());
            assertEquals(position, channel.position());
            assertEquals(size, channel.size());
        } finally {
            channel.position(channel.size());
        }
    }

    @Test
    public void testTransferFromMustStartFromWritableOffset() throws IOException {
        doTestTransferFromMustStartFromWritableOffset(false);
    }

    @Test
    public void testTransferFromWithPositionMustStartFromWritableOffset() throws IOException {
        doTestTransferFromMustStartFromWritableOffset(true);
    }

    private void doTestTransferFromMustStartFromWritableOffset(boolean withPosition) throws IOException {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator();
             ProtonBuffer buffer = allocator.allocate(4)) {

            ByteBuffer data = ByteBuffer.allocate(4).putInt(0x01020304).flip();
            final long position = channel.position();
            assertEquals(4, channel.write(data, position));

            final long size = channel.size();
            int bytesRead = withPosition ? buffer.transferFrom(channel, position, 2) : buffer.transferFrom(channel, 2);
            bytesRead += withPosition ? buffer.transferFrom(channel, 2 + position, 2) : buffer.transferFrom(channel, 2);

            assertEquals(4, bytesRead);
            assertEquals(4, buffer.getReadableBytes());

            assertEquals(withPosition ? position : 4 + position, channel.position());
            assertEquals(size, channel.size());

            for (int i = 0; i < buffer.getReadableBytes(); i++) {
                assertEquals(data.get(i), buffer.readByte());
            }
        } finally {
            channel.position(channel.size());
        }
    }

    @Test
    public void testTransferFromMustThrowIfBufferIsReadOnly() throws IOException {
        doTestTransferFromMustThrowIfBufferIsReadOnly(false);
    }

    @Test
    public void testTransferFromWithPositionMustThrowIfBufferIsReadOnly() throws IOException {
        doTestTransferFromMustThrowIfBufferIsReadOnly(true);
    }

    private void doTestTransferFromMustThrowIfBufferIsReadOnly(boolean withPosition) throws IOException {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator();
             ProtonBuffer buffer = allocator.allocate(8).writeLong(0x0102030405060708L).convertToReadOnly()) {

            final long position = channel.position();
            final long size = channel.size();

            assertThrows(ProtonBufferReadOnlyException.class, () -> {
                if (withPosition) {
                    buffer.transferFrom(channel, 4 + position, 8);
                } else {
                    buffer.transferFrom(channel, 8);
                }
            });

            assertEquals(8, buffer.getReadableBytes());
            assertEquals(position, channel.position());
            assertEquals(size, channel.size());
        }
    }

    @Test
    public void testTransferFromZeroBytesMustNotThrowOnClosedChannel() throws IOException {
        doTestTransferFromZeroBytesMustNotThrowOnClosedChannel(false);
    }

    @Test
    public void testTransferFromWithPositionZeroBytesMustNotThrowOnClosedChannel() throws IOException {
        doTestTransferFromZeroBytesMustNotThrowOnClosedChannel(true);
    }

    private void doTestTransferFromZeroBytesMustNotThrowOnClosedChannel(boolean withPosition) throws IOException {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator();
             ProtonBuffer empty = allocator.allocate(0);
             ProtonBuffer nonEmpty = allocator.allocate(4)) {

            if (withPosition) {
                empty.transferFrom(closedChannel, 4, 4);
                nonEmpty.transferFrom(closedChannel, 4, 0);
            } else {
                empty.transferFrom(closedChannel, 4);
                nonEmpty.transferFrom(closedChannel, 0);
            }
        }
    }

    @Test
    public void testPartialFailureOfTransferToMustKeepChannelAndBufferPositionsInSync(@TempDir Path parentDir) throws IOException {
        ProtonBufferAllocator allocator = ProtonBufferAllocator.defaultAllocator();
        Path path = parentDir.resolve("transferTo");

        try (FileChannel channel = FileChannel.open(path, READ, WRITE, CREATE);
             ProtonBuffer buffer = allocator.composite(new ProtonBuffer[] {
                                        allocator.allocate(4).writeInt(0x01020304),
                                        allocator.allocate(4).writeInt(0x05060708) })) {

            WritableByteChannel channelWrapper = new WritableByteChannel() {
                private boolean pastFirstCall;

                @Override
                public int write(ByteBuffer src) throws IOException {
                    if (pastFirstCall) {
                        throw new IOException("boom");
                    }
                    pastFirstCall = true;
                    return channel.write(src);
                }

                @Override
                public boolean isOpen() {
                    return channel.isOpen();
                }

                @Override
                public void close() throws IOException {
                    channel.close();
                }
            };

            final long position = channel.position();
            final long size = channel.size();
            final Exception e = assertThrows(IOException.class, () -> buffer.transferTo(channelWrapper, 8));

            assertTrue(e.getMessage().contains("boom"));

            assertEquals(position + 4, channel.position());
            assertEquals(size + 4, channel.size());

            assertEquals(4, buffer.getReadOffset());
            assertEquals(4, buffer.getReadableBytes());
        }
    }

    @Test
    public void testPartialFailureOfTransferFromMustKeepChannelAndBufferPositionsInSync(@TempDir Path parentDir) throws IOException {
        ProtonBufferAllocator allocator = ProtonBufferAllocator.defaultAllocator();
        Path path = parentDir.resolve("transferFrom");

        try (FileChannel channel = FileChannel.open(path, READ, WRITE, CREATE);
             ProtonBuffer buffer = allocator.composite(new ProtonBuffer[] {
                     allocator.allocate(4), allocator.allocate(4) })) {

            ByteBuffer byteBuffer = ByteBuffer.allocate(8).putLong(0x0102030405060708L).flip();
            assertEquals(8, channel.write(byteBuffer));
            channel.position(0);

            ReadableByteChannel channelWrapper = new ReadableByteChannel() {
                private boolean pastFirstCall;

                @Override
                public int read(ByteBuffer dst) throws IOException {
                    if (pastFirstCall) {
                        throw new IOException("boom");
                    }
                    pastFirstCall = true;
                    return channel.read(dst);
                }

                @Override
                public boolean isOpen() {
                    return channel.isOpen();
                }

                @Override
                public void close() throws IOException {
                    channel.close();
                }
            };

            final long position = channel.position();
            final long size = channel.size();
            final Exception e = assertThrows(IOException.class, () -> buffer.transferFrom(channelWrapper, 8));

            assertTrue(e.getMessage().contains("boom"));

            assertEquals(position + 4, channel.position());
            assertEquals(size, channel.size());

            assertEquals(4, buffer.getWritableBytes());
            assertEquals(4, buffer.getReadableBytes());
        }
    }

    @Test
    public void testComponentCountOfNonCompositeBufferMustBeOne() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buffer = allocator.allocate(8)) {
            assertEquals(1, buffer.componentCount());
        }
    }

    @Test
    public void testReadableComponentCountMustBeOneIfThereAreReadableBytes() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buffer = allocator.allocate(8)) {
            assertEquals(0, buffer.readableComponentCount());
            buffer.advanceWriteOffset(1);
            assertEquals(1, buffer.readableComponentCount());
        }
    }

    @Test
    public void testWritableComponentCountMustBeOneIfThereAreWritableBytes() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buffer = allocator.allocate(8)) {
            assertEquals(1, buffer.writableComponentCount());
            buffer.advanceWriteOffset(8);
            assertEquals(0, buffer.writableComponentCount());
        }
    }

    @Test
    public void testCompositeBufferComponentCountsUpdateWithChangeAfterFlattening() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator()) {
            try (ProtonBuffer a = allocator.allocate(8);
                 ProtonBuffer b = allocator.allocate(8);
                 ProtonBuffer c = allocator.allocate(8);
                 ProtonBuffer composed = allocator.composite(new ProtonBuffer[] { b, c });
                 ProtonBuffer buffer = allocator.composite(new ProtonBuffer[] { a, composed })) {

                assertEquals(3, buffer.componentCount());
                assertEquals(0, buffer.readableComponentCount());
                assertEquals(3, buffer.writableComponentCount());
                buffer.writeInt(1);
                assertEquals(1, buffer.readableComponentCount());
                assertEquals(3, buffer.writableComponentCount());
                buffer.writeInt(1);
                assertEquals(1, buffer.readableComponentCount());
                assertEquals(2, buffer.writableComponentCount());
                buffer.writeInt(1);
                assertEquals(2, buffer.readableComponentCount());
                assertEquals(2, buffer.writableComponentCount());
                buffer.writeInt(1);
                assertEquals(2, buffer.readableComponentCount());
                assertEquals(1, buffer.writableComponentCount());
                buffer.writeInt(1);
                assertEquals(3, buffer.readableComponentCount());
                assertEquals(1, buffer.writableComponentCount());
                buffer.writeInt(1);
                assertEquals(3, buffer.readableComponentCount());
                assertEquals(0, buffer.writableComponentCount());
            }
        }
    }

    @Test
    public void testIteratingComponentOnClosedBufferMustThrow() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator()) {
            ProtonBuffer writableBuffer = allocator.allocate(8);
            writableBuffer.close();
            assertThrows(ProtonBufferClosedException.class, () -> writableBuffer.componentAccessor());

            var readableBuffer = allocator.allocate(8);
            readableBuffer.writeLong(0);
            readableBuffer.close();
            assertThrows(ProtonBufferClosedException.class, () -> readableBuffer.componentAccessor());
        }
    }

    @Test
    public void testProtonBufferProvidesAccessToAvailableComponents() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buffer = allocator.allocate(8)) {
            assertEquals(1, buffer.componentCount());
            assertEquals(0, buffer.readableComponentCount());
            assertEquals(1, buffer.writableComponentCount());

            try (ProtonBufferComponentAccessor accessor = buffer.componentAccessor()) {
                assertNull(accessor.firstReadable());
                assertNotNull(accessor.firstWritable());
                assertNotNull(accessor.first());

                ProtonBufferComponent component = accessor.first();
                assertEquals(8, component.getWritableBytes());
                assertEquals(0, component.getReadableBytes());

                ByteBuffer bb = component.getWritableBuffer();
                assertEquals(8, bb.remaining());

                for (byte i = 0; i < bb.remaining(); ++i) {
                    bb.put(i);
                }

                // Changes in the byte buffer should not reflect back other than to the underlying bytes
                assertEquals(8, component.getWritableBytes());
                assertEquals(0, component.getReadableBytes());
            }

            buffer.setWriteOffset(buffer.capacity());

            try (ProtonBufferComponentAccessor accessor = buffer.componentAccessor()) {
                assertNotNull(accessor.firstReadable());
                assertNull(accessor.firstWritable());
                assertNotNull(accessor.first());

                ProtonBufferComponent component = accessor.first();
                assertEquals(0, component.getWritableBytes());
                assertEquals(8, component.getReadableBytes());

                ByteBuffer bb = component.getReadableBuffer();
                assertEquals(8, bb.remaining());

                for (byte i = 0; i < bb.remaining(); ++i) {
                    assertEquals(i, bb.get());
                }

                // Changes in the byte buffer should not reflect back other than to the underlying bytes
                assertEquals(0, component.getWritableBytes());
                assertEquals(8, component.getReadableBytes());
            }
        }
    }

    @Test
    public void testIteratingComponentMustAllowCollectingBuffersInArray() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator()) {
            try (ProtonBuffer composite = allocator.composite(new ProtonBuffer[] {
                    allocator.allocate(4), allocator.allocate(4), allocator.allocate(4) })) {
                int i = 1;
                while (composite.isWritable()) {
                    composite.writeByte((byte) i++);
                }
                ByteBuffer[] buffers = new ByteBuffer[composite.componentCount()];
                try (ProtonBufferComponentAccessor accessor = composite.componentAccessor()) {
                    int index = 0;
                    for (ProtonBufferComponent component = accessor.first(); component != null; component = accessor.next()) {
                        buffers[index] = component.getReadableBuffer();
                        index++;
                    }
                }
                i = 1;
                assertTrue(buffers.length > 1);
                for (ByteBuffer buffer : buffers) {
                    while (buffer.hasRemaining()) {
                        assertEquals((byte) i++, buffer.get());
                    }
                }
            }

            try (ProtonBuffer single = allocator.allocate(8)) {
                ByteBuffer[] buffers = new ByteBuffer[single.componentCount()];
                try (ProtonBufferComponentAccessor accessor = single.componentAccessor()) {
                    int index = 0;
                    for (var component = accessor.first(); component != null; component = accessor.next()) {
                        buffers[index] = component.getWritableBuffer();
                        index++;
                    }
                }
                assertTrue(buffers.length == 1);
                int i = 1;
                for (ByteBuffer buffer : buffers) {
                    while (buffer.hasRemaining()) {
                        buffer.put((byte) i++);
                    }
                }
                single.setWriteOffset(single.capacity());
                i = 1;
                while (single.isReadable()) {
                    assertEquals((byte) i++, single.readByte());
                }
            }
        }
    }

    @Test
    public void testComponentAccessorMustVisitBuffer() {
        final long value = 0x0102030405060708L;
        try (ProtonBufferAllocator allocator = createTestCaseAllocator();
             ProtonBuffer readWriteBuffer = allocator.allocate(8).writeLong(value);
             ProtonBuffer readonlyBuffer = allocator.allocate(8).writeLong(value).convertToReadOnly()) {

            verifyReadingForEachSingleComponent(readWriteBuffer);
            verifyReadingForEachSingleComponent(readonlyBuffer);
        }
    }

    @Test
    public void testComponentAccessorMustVisitAllReadableConstituentBuffersInOrder() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator();
             ProtonBuffer a = allocator.allocate(4).writeInt(1);
             ProtonBuffer b = allocator.allocate(4).writeInt(2);
             ProtonBuffer c = allocator.allocate(4).writeInt(3);
             ProtonBuffer composite = allocator.composite(new ProtonBuffer[] { a, b, c })) {

            Deque<Integer> list = new LinkedList<Integer>(List.of(1, 2, 3));
            int index = 0;
            try (ProtonBufferComponentAccessor accessor = composite.componentAccessor()) {
                for (var component = accessor.first(); component != null; component = accessor.next()) {
                    var buffer = component.getReadableBuffer();
                    int bufferValue = buffer.getInt();
                    int expectedValue = list.pollFirst().intValue();
                    assertEquals(expectedValue, bufferValue);
                    assertEquals(bufferValue, index + 1);
                    assertThrows(ReadOnlyBufferException.class, () -> buffer.put(0, (byte) 0xFF));
                    index++;
                }
            }
            assertEquals(3, index);
            assertTrue(list.isEmpty());
        }
    }

    @Test
    public void testComponentAccessorMustVisitAllWritableConstituentBuffersInOrder() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator()) {
            try (ProtonBuffer a = allocator.allocate(8);
                 ProtonBuffer b = allocator.allocate(8);
                 ProtonBuffer c = allocator.allocate(8);
                 ProtonBuffer buf = allocator.composite(new ProtonBuffer[] {a, b, c})) {
                try (ProtonBufferComponentAccessor accessor = buf.componentAccessor()) {
                    int index = 0;
                    for (var component = accessor.first(); component != null; component = accessor.next()) {
                        component.getWritableBuffer().putLong(0x0102030405060708L + 0x1010101010101010L * index);
                        index++;
                    }
                }
                buf.setWriteOffset(3 * 8);
                assertEquals(0x0102030405060708L, buf.readLong());
                assertEquals(0x1112131415161718L, buf.readLong());
                assertEquals(0x2122232425262728L, buf.readLong());
            }
        }
    }

    @Test
    public void testComponentAccessorMustReturnNullFirstWhenNotReadable() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator();
             ProtonBuffer buf = allocator.allocate(0)) {
            try (ProtonBufferComponentAccessor accessor = buf.componentAccessor()) {
                // First component may or may not be null such as from the byte array based buffer
                var component = accessor.first();
                if (component != null) {
                    assertEquals(0, component.getReadableBytes());
                    assertEquals(0, component.getReadableArrayLength());
                    ByteBuffer byteBuffer = component.getReadableBuffer();
                    assertEquals(0, byteBuffer.remaining());
                    assertEquals(0, byteBuffer.capacity());
                    assertNull(accessor.next());
                }
            }

            try (ProtonBufferComponentAccessor accessor = buf.componentAccessor()) {
                assertNull(accessor.firstReadable());
            }
        }
    }

    @Test
    public void testComponentAccessorChangesMadeToByteBufferComponentMustBeReflectedInBuffer() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(9)) {
            buf.writeByte((byte) 0xFF);
            int writtenCounter = 0;
            try (ProtonBufferComponentAccessor accessor = buf.componentAccessor()) {
                for (ProtonBufferComponent component = accessor.first(); component != null; component = accessor.next()) {
                    ByteBuffer buffer = component.getWritableBuffer();
                    while (buffer.hasRemaining()) {
                        buffer.put((byte) ++writtenCounter);
                    }
                }
            }
            buf.setWriteOffset(9);
            assertEquals((byte) 0xFF, buf.readByte());
            assertEquals(0x0102030405060708L, buf.readLong());
        }
    }

    @Test
    public void testChangesMadeToByteBufferComponentsDuringAccessShouldBeReflectedInBuffer() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            int counter = 0;
            try (ProtonBufferComponentAccessor accessor = buf.componentAccessor()) {
                for (var component = accessor.first(); component != null; component = accessor.next()) {
                    ByteBuffer buffer = component.getWritableBuffer();
                    while (buffer.hasRemaining()) {
                        buffer.put((byte) ++counter);
                    }
                }
            }
            buf.setWriteOffset(buf.capacity());
            for (int i = 0; i < 8; i++) {
                assertEquals((byte) i + 1, buf.getByte(i));
            }
        }
    }

    @Test
    public void testComponentAccessorMustHaveZeroWritableBytesWhenNotWritable() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(0)) {
            try (ProtonBufferComponentAccessor accessor = buf.componentAccessor()) {
                // First component may or may not be null as with the byte based proton buffer
                ProtonBufferComponent component = accessor.first();
                if (component != null) {
                    assertEquals(0, component.getWritableBytes());
                    assertEquals(0, component.getWritableArrayLength());
                    ByteBuffer byteBuffer = component.getWritableBuffer();
                    assertEquals(0, byteBuffer.remaining());
                    assertEquals(0, byteBuffer.capacity());
                    assertNull(accessor.next());
                }
            }
            try (ProtonBufferComponentAccessor accessor = buf.componentAccessor()) {
                // First *writable* component is definitely null.
                assertNull(accessor.firstWritable());
            }
        }
    }

    @Test
    public void testAllComponentsOfReadOnlyBufferMustNotHaveAnyWritableBytes() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator();
             ProtonBuffer buf = allocator.allocate(8).convertToReadOnly()) {
            try (ProtonBufferComponentAccessor accessor = buf.componentAccessor()) {
                for (var c = accessor.first(); c != null; c = accessor.next()) {
                    assertEquals(0, c.getWritableBytes());
                    assertEquals(0, c.getWritableArrayLength());
                    ByteBuffer byteBuffer = c.getWritableBuffer();
                    assertEquals(0, byteBuffer.remaining());
                    assertEquals(0, byteBuffer.capacity());
                    assertNull(accessor.next());
                }
            }
            try (ProtonBufferComponentAccessor accessor = buf.componentAccessor()) {
                assertNull(accessor.firstWritable());
            }
        }
    }

    @Test
    public void testComponentAccessMustBeAbleToIncrementReaderOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator();
             ProtonBuffer buf = allocator.allocate(8);
             ProtonBuffer target = allocator.allocate(5)) {
            buf.writeLong(0x0102030405060708L);
            try (ProtonBufferComponentAccessor accessor = buf.componentAccessor()) {
                for (ProtonBufferComponent component = accessor.first(); component != null; component = accessor.next()) {
                    while (target.getWritableBytes() > 0 && component.getReadableBytes() > 0) {
                        ByteBuffer byteBuffer = component.getReadableBuffer();
                        byte value = byteBuffer.get();
                        byteBuffer.clear();
                        target.writeByte(value);
                        final ProtonBufferComponent cmp = component; // Capture for lambda.
                        assertThrows(IndexOutOfBoundsException.class, () -> cmp.advanceReadOffset(9));
                        component.advanceReadOffset(1);
                    }
                }
            }
            assertEquals(5, buf.getReadOffset());
            assertEquals(3, buf.getReadableBytes());
            assertEquals(5, target.getReadableBytes());

            try (ProtonBuffer expected = allocator.copy(new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05 })) {
                assertEquals(target, expected);
            }
            try (ProtonBuffer expected = allocator.copy(new byte[] { 0x06, 0x07, 0x08 })) {
                assertEquals(buf, expected);
            }
        }
    }

    @Test
    public void testComponentAccessorMustBeAbleToIncrementWriterOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator();
             ProtonBuffer buf = allocator.allocate(8).writeLong(0x0102030405060708L);
             ProtonBuffer target = buf.copy()) {
            buf.setWriteOffset(0); // Prime the buffer with data, but leave the write-offset at zero.
            try (ProtonBufferComponentAccessor accessor = buf.componentAccessor()) {
                for (ProtonBufferComponent component = accessor.first(); component != null; component = accessor.next()) {
                    while (component.getWritableBytes() > 0) {
                        ByteBuffer byteBuffer = component.getWritableBuffer();
                        byte value = byteBuffer.get();
                        byteBuffer.clear();
                        assertEquals(value, target.readByte());
                        var cmp = component; // Capture for lambda.
                        assertThrows(IndexOutOfBoundsException.class, () -> cmp.advanceWriteOffset(9));
                        component.advanceWriteOffset(1);
                    }
                }
            }
            assertEquals(8, buf.getWriteOffset());
            assertEquals(8, target.getReadOffset());
            target.setReadOffset(0);
            assertEquals(buf, target);
        }
    }

    @Test
    public void testNegativeAdvanceReadOffsetOnReadableComponentMustThrow() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            buf.writeLong(0x0102030405060708L);
            assertEquals(0x01020304, buf.readInt());
            try (ProtonBufferComponentAccessor accessor = buf.componentAccessor()) {
                for (ProtonBufferComponent component = accessor.first(); component != null; component = accessor.next()) {
                    final ProtonBufferComponent cmp = component;
                    assertThrows(IllegalArgumentException.class, () -> cmp.advanceReadOffset(-1));
                }
            }
        }
    }

    @Test
    public void testNegativeAdvanceWriteOffsetWritableComponentMustThrow() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            try (ProtonBufferComponentAccessor accessor = buf.componentAccessor()) {
                for (ProtonBufferComponent component = accessor.first(); component != null; component = accessor.next()) {
                    final ProtonBufferComponent cmp = component;
                    assertThrows(IllegalArgumentException.class, () -> cmp.advanceWriteOffset(-1));
                }
            }
        }
    }

    @Test
    public void testBufferIterationAllReadableBytes() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            {
                final ProtonBufferIterator iterator = buf.bufferIterator();
                assertFalse(iterator.hasNext());
                assertEquals(0, iterator.remaining());
                assertThrows(NoSuchElementException.class, () -> iterator.next());
            }

            buf.writeBytes(new byte[] {1, 2, 3, 4});
            int roff = buf.getReadOffset();
            int woff = buf.getWriteOffset();
            ProtonBufferIterator iterator = buf.bufferIterator();
            assertEquals(4, iterator.remaining());
            assertTrue(iterator.hasNext());
            assertEquals((byte) 0x01, iterator.next());
            assertEquals(3, iterator.remaining());
            assertTrue(iterator.hasNext());
            assertEquals((byte) 0x02, iterator.next());
            assertEquals(2, iterator.remaining());
            assertTrue(iterator.hasNext());
            assertEquals((byte) 0x03, iterator.next());
            assertEquals(1, iterator.remaining());
            assertTrue(iterator.hasNext());
            assertEquals((byte) 0x04, iterator.next());
            assertEquals(0, iterator.remaining());
            assertFalse(iterator.hasNext());
            assertEquals(roff, buf.getReadOffset());
            assertEquals(woff, buf.getWriteOffset());
            assertThrows(NoSuchElementException.class, () -> iterator.next());
        }
    }

    @Test
    public void testBufferIterationWithingBounds() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertThrows(IllegalArgumentException.class, () -> buf.bufferIterator(-1, 1));
            assertThrows(IllegalArgumentException.class, () -> buf.bufferIterator(1, -1));
            assertThrows(IndexOutOfBoundsException.class, () -> buf.bufferIterator(buf.capacity(), 1));
            assertThrows(IndexOutOfBoundsException.class, () -> buf.bufferIterator(buf.capacity() - 1, 2));
            assertThrows(IndexOutOfBoundsException.class, () -> buf.bufferIterator(buf.capacity() - 2, 3));

            {
                final ProtonBufferIterator iterator = buf.bufferIterator();
                assertFalse(iterator.hasNext());
                assertEquals(0, iterator.remaining());
                assertThrows(NoSuchElementException.class, () -> iterator.next());
            }

            buf.writeBytes(new byte[] {1, 2, 3, 4, 5, 6});
            int roff = buf.getReadOffset();
            int woff = buf.getWriteOffset();
            ProtonBufferIterator iterator = buf.bufferIterator(buf.getReadOffset() + 1, buf.getReadableBytes() - 2);
            assertEquals(4, iterator.remaining());
            assertTrue(iterator.hasNext());
            assertEquals((byte) 2, iterator.next());
            assertEquals(3, iterator.remaining());
            assertTrue(iterator.hasNext());
            assertEquals((byte) 3, iterator.next());
            assertEquals(2, iterator.remaining());
            assertTrue(iterator.hasNext());
            assertEquals((byte) 4, iterator.next());
            assertEquals(1, iterator.remaining());
            assertTrue(iterator.hasNext());
            assertEquals((byte) 5, iterator.next());
            assertEquals(0, iterator.remaining());

            iterator = buf.bufferIterator(buf.getReadOffset() + 1, 2);
            assertEquals(2, iterator.remaining());
            assertTrue(iterator.hasNext());
            assertEquals((byte) 2, iterator.next());
            assertEquals(1, iterator.remaining());
            assertTrue(iterator.hasNext());
            assertEquals((byte) 3, iterator.next());
            assertEquals(0, iterator.remaining());
            assertFalse(iterator.hasNext());
            assertEquals(roff, buf.getReadOffset());
            assertEquals(woff, buf.getWriteOffset());
        }
    }

    @Test
    public void testReverseBufferIterationAllReadableBytes() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {

            {
                final ProtonBufferIterator iterator = buf.bufferReverseIterator();
                assertFalse(iterator.hasNext());
                assertEquals(0, iterator.remaining());
                assertThrows(NoSuchElementException.class, () -> iterator.next());
            }

            buf.writeBytes(new byte[] {1, 2, 3, 4});
            int roff = buf.getReadOffset();
            int woff = buf.getWriteOffset();
            ProtonBufferIterator iterator = buf.bufferReverseIterator();
            assertEquals(4, iterator.remaining());
            assertTrue(iterator.hasNext());
            assertEquals((byte) 4, iterator.next());
            assertEquals(3, iterator.remaining());
            assertTrue(iterator.hasNext());
            assertEquals((byte) 3, iterator.next());
            assertEquals(2, iterator.remaining());
            assertTrue(iterator.hasNext());
            assertEquals((byte) 2, iterator.next());
            assertEquals(1, iterator.remaining());
            assertTrue(iterator.hasNext());
            assertEquals((byte) 1, iterator.next());
            assertEquals(0, iterator.remaining());
            assertFalse(iterator.hasNext());
            assertEquals(roff, buf.getReadOffset());
            assertEquals(woff, buf.getWriteOffset());
        }
    }

    @Test
    public void testReverseBufferIterationWithinBounds() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator(); ProtonBuffer buf = allocator.allocate(8)) {
            assertThrows(IllegalArgumentException.class, () -> buf.bufferReverseIterator(-1, 0));
            assertThrows(IllegalArgumentException.class, () -> buf.bufferReverseIterator(0, -1));
            assertThrows(IndexOutOfBoundsException.class, () -> buf.bufferReverseIterator(0, 2));
            assertThrows(IndexOutOfBoundsException.class, () -> buf.bufferReverseIterator(1, 3));
            assertThrows(IndexOutOfBoundsException.class, () -> buf.bufferReverseIterator(buf.capacity(), 0));

            {
                ProtonBufferIterator iterator = buf.bufferReverseIterator(1, 0);
                assertFalse(iterator.hasNext());
                assertEquals(0, iterator.remaining());
                assertThrows(NoSuchElementException.class, () -> iterator.next());
            }

            buf.writeBytes(new byte[] {1, 2, 3, 4, 5, 6, 7});
            int roff = buf.getReadOffset();
            int woff = buf.getWriteOffset();
            ProtonBufferIterator iterator = buf.bufferReverseIterator(buf.getWriteOffset() - 2, buf.getReadableBytes() - 2);
            assertEquals(5, iterator.remaining());
            assertTrue(iterator.hasNext());
            assertEquals((byte) 6, iterator.next());
            assertEquals(4, iterator.remaining());
            assertTrue(iterator.hasNext());
            assertEquals((byte) 5, iterator.next());
            assertEquals(3, iterator.remaining());
            assertTrue(iterator.hasNext());
            assertEquals((byte) 4, iterator.next());
            assertEquals(2, iterator.remaining());
            assertTrue(iterator.hasNext());
            assertEquals((byte) 3, iterator.next());
            assertEquals(1, iterator.remaining());
            assertTrue(iterator.hasNext());
            assertEquals((byte) 2, iterator.next());
            assertEquals(0, iterator.remaining());
            assertFalse(iterator.hasNext());

            iterator = buf.bufferReverseIterator(buf.getReadOffset() + 2, 2);
            assertEquals(2, iterator.remaining());
            assertTrue(iterator.hasNext());
            assertEquals((byte) 3, iterator.next());
            assertEquals(1, iterator.remaining());
            assertTrue(iterator.hasNext());
            assertEquals((byte) 2, iterator.next());
            assertEquals(0, iterator.remaining());
            assertFalse(iterator.hasNext());
            assertEquals(roff, buf.getReadOffset());
            assertEquals(woff, buf.getWriteOffset());
        }
    }

    @Test
    public void testIndeOfByteMustThrowOnClosedBuffer() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator()) {
            ProtonBuffer buffer = allocator.allocate(8);
            buffer.writeLong(0x0102030405060708L);
            assertEquals(2, buffer.indexOf((byte) 0x03));
            ProtonBuffer transferred = buffer.transfer();
            assertThrows(IllegalStateException.class, () -> buffer.indexOf((byte) 0));
            assertEquals(2, transferred.indexOf((byte) 0x03));
            transferred.close();
            assertThrows(IllegalStateException.class, () -> transferred.indexOf((byte) 0));
        }
    }

    @Test
    public void testIndexOfByteMustFindNeedleAtReadOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator()) {
            ProtonBuffer buffer = allocator.allocate(128);
            writeRandomBytes(buffer);
            byte needle = (byte) 0xA5;
            buffer.setByte(3, needle);
            buffer.advanceReadOffset(3);
            assertEquals(3, buffer.indexOf(needle));
        }
    }

    @Test
    public void testIndexOfByteMustFindNeedleCloseToEndOfReadableBytes() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator()) {
            ProtonBuffer buffer = allocator.allocate(128);
            writeRandomBytes(buffer);
            final byte needle = (byte) 0xA5;
            final int indexOf = buffer.capacity() - 2;

            buffer.setByte(indexOf, needle);

            // As the read offset increases the location remains the same
            while (buffer.getReadableBytes() > 1) {
                assertEquals(indexOf, buffer.indexOf(needle));
                buffer.advanceReadOffset(1);
            }
        }
    }

    @Test
    public void testIndexOfByteMustFindNeedlePriorToEndOffset() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator()) {
            ProtonBuffer buffer = allocator.allocate(128);
            writeRandomBytes(buffer);

            final byte needle = (byte) 0xA5;
            final int indexOf = buffer.capacity() - 1;

            buffer.setByte(indexOf, needle);

            while (buffer.getReadableBytes() > 1) {
                assertEquals(indexOf, buffer.indexOf(needle));
                buffer.advanceReadOffset(1);
            }
        }
    }

    @Test
    public void testIndexOfByteMustNotFindNeedleOutsideReadableRange() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator()) {
            ProtonBuffer buffer = allocator.allocate(128);
            writeRandomBytes(buffer);

            final byte needle = (byte) 0xA5;

            buffer.setByte(buffer.capacity() - 1, needle);

            // Pull the write-offset down by one, leaving needle just outside readable range.
            buffer.setWriteOffset(buffer.getWriteOffset() - 1);

            while (buffer.getReadableBytes() > 1) {
                assertEquals(-1, buffer.indexOf(needle));
                buffer.advanceReadOffset(1);
            }
        }
    }

    @Test
    public void testIndexOfByteOnEmptyBufferReturnsNoResult() {
        try (ProtonBufferAllocator allocator = createTestCaseAllocator()) {
            ProtonBuffer buffer = allocator.allocate(128);
            assertEquals(-1, buffer.indexOf((byte) 0));
        }
    }

    protected static void verifyInaccessible(ProtonBuffer buf) {
        verifyReadInaccessible(buf);

        verifyWriteInaccessible(buf, ProtonBufferClosedException.class);

        try (ProtonBufferAllocator allocator = ProtonBufferAllocator.defaultAllocator();
             ProtonBuffer target = allocator.allocate(24)) {
            assertThrows(ProtonBufferClosedException.class, () -> buf.copyInto(0, target, 0, 1));
            assertThrows(ProtonBufferClosedException.class, () -> buf.copyInto(0, target, 0, 0));
            assertThrows(ProtonBufferClosedException.class, () -> buf.copyInto(0, new byte[1], 0, 1));
            assertThrows(ProtonBufferClosedException.class, () -> buf.copyInto(0, new byte[1], 0, 0));
            assertThrows(ProtonBufferClosedException.class, () -> buf.copyInto(0, ByteBuffer.allocate(1), 0, 1));
            assertThrows(ProtonBufferClosedException.class, () -> buf.copyInto(0, ByteBuffer.allocate(1), 0, 0));
            if (ProtonCompositeBuffer.isComposite(buf)) {
                assertThrows(ProtonBufferClosedException.class, () -> ((ProtonCompositeBuffer) buf).append(target));
            }
        }

        assertThrows(ProtonBufferClosedException.class, () -> buf.split());
        assertThrows(ProtonBufferClosedException.class, () -> buf.copy());
        assertThrows(ProtonBufferClosedException.class, () -> buf.bufferIterator());
        assertThrows(ProtonBufferClosedException.class, () -> buf.bufferIterator(0, 0));
        assertThrows(ProtonBufferClosedException.class, () -> buf.bufferReverseIterator());
        assertThrows(ProtonBufferClosedException.class, () -> buf.bufferReverseIterator(0, 0));
    }

    protected static void verifyReadInaccessible(ProtonBuffer buf) {
        assertThrows(ProtonBufferClosedException.class, () -> buf.readBoolean());
        assertThrows(ProtonBufferClosedException.class, () -> buf.readByte());
        assertThrows(ProtonBufferClosedException.class, () -> buf.readUnsignedByte());
        assertThrows(ProtonBufferClosedException.class, () -> buf.readChar());
        assertThrows(ProtonBufferClosedException.class, () -> buf.readShort());
        assertThrows(ProtonBufferClosedException.class, () -> buf.readUnsignedShort());
        assertThrows(ProtonBufferClosedException.class, () -> buf.readInt());
        assertThrows(ProtonBufferClosedException.class, () -> buf.readUnsignedInt());
        assertThrows(ProtonBufferClosedException.class, () -> buf.readFloat());
        assertThrows(ProtonBufferClosedException.class, () -> buf.readLong());
        assertThrows(ProtonBufferClosedException.class, () -> buf.readDouble());

        assertThrows(ProtonBufferClosedException.class, () -> buf.getBoolean(0));
        assertThrows(ProtonBufferClosedException.class, () -> buf.getByte(0));
        assertThrows(ProtonBufferClosedException.class, () -> buf.getUnsignedByte(0));
        assertThrows(ProtonBufferClosedException.class, () -> buf.getChar(0));
        assertThrows(ProtonBufferClosedException.class, () -> buf.getShort(0));
        assertThrows(ProtonBufferClosedException.class, () -> buf.getUnsignedShort(0));
        assertThrows(ProtonBufferClosedException.class, () -> buf.getInt(0));
        assertThrows(ProtonBufferClosedException.class, () -> buf.getUnsignedInt(0));
        assertThrows(ProtonBufferClosedException.class, () -> buf.getFloat(0));
        assertThrows(ProtonBufferClosedException.class, () -> buf.getLong(0));
        assertThrows(ProtonBufferClosedException.class, () -> buf.getDouble(0));
    }

    protected static void verifyWriteInaccessible(ProtonBuffer buf, Class<? extends RuntimeException> expected) {
        assertThrows(expected, () -> buf.writeByte((byte) 32));
        assertThrows(expected, () -> buf.writeUnsignedByte(32));
        assertThrows(expected, () -> buf.writeChar('3'));
        assertThrows(expected, () -> buf.writeShort((short) 32));
        assertThrows(expected, () -> buf.writeUnsignedShort(32));
        assertThrows(expected, () -> buf.writeInt(32));
        assertThrows(expected, () -> buf.writeUnsignedInt(32));
        assertThrows(expected, () -> buf.writeFloat(3.2f));
        assertThrows(expected, () -> buf.writeLong(32));
        assertThrows(expected, () -> buf.writeDouble(32));

        assertThrows(expected, () -> buf.setByte(0, (byte) 32));
        assertThrows(expected, () -> buf.setUnsignedByte(0, 32));
        assertThrows(expected, () -> buf.setChar(0, '3'));
        assertThrows(expected, () -> buf.setShort(0, (short) 32));
        assertThrows(expected, () -> buf.setUnsignedShort(0, 32));
        assertThrows(expected, () -> buf.setInt(0, 32));
        assertThrows(expected, () -> buf.setUnsignedInt(0, 32));
        assertThrows(expected, () -> buf.setFloat(0, 3.2f));
        assertThrows(expected, () -> buf.setLong(0, 32));
        assertThrows(expected, () -> buf.setDouble(0, 32));

        assertThrows(expected, () -> buf.ensureWritable(1));
        assertThrows(expected, () -> buf.fill((byte) 0));
        assertThrows(expected, () -> buf.compact());
        try (ProtonBufferAllocator allocator = ProtonBufferAllocator.defaultAllocator();
             ProtonBuffer source = allocator.allocate(8)) {
            assertThrows(expected, () -> source.copyInto(0, buf, 0, 1));
            if (expected == ProtonBufferClosedException.class) {
                assertThrows(expected, () -> buf.copyInto(0, source, 0, 1));
            }
        }
    }

    protected static void verifySplitEmptyCompositeBuffer(ProtonBuffer buf) {
        try (ProtonBuffer a = buf.split()) {
            a.ensureWritable(4);
            buf.ensureWritable(4);
            a.writeInt(1);
            buf.writeInt(2);
            assertEquals(1, a.readInt());
            assertEquals(2, buf.readInt());
        }
    }

    public static void verifyReadingForEachSingleComponent(ProtonBuffer buf) {
        try (ProtonBufferComponentAccessor accessor = buf.componentAccessor()) {
            for (ProtonBufferComponent component = accessor.first(); component != null; component = accessor.next()) {
                ByteBuffer buffer = component.getReadableBuffer();
                assertEquals(0, buffer.position());
                assertEquals(8, buffer.limit());
                assertEquals(8, buffer.capacity());
                assertEquals(0x0102030405060708L, buffer.getLong());

                if (component.hasReadbleArray()) {
                    byte[] array = component.getReadableArray();
                    byte[] arrayCopy = new byte[component.getReadableArrayLength()];
                    System.arraycopy(array, component.getReadableArrayOffset(), arrayCopy, 0, arrayCopy.length);
                    assertArrayEquals(new byte[] {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}, arrayCopy);
                }

                assertThrows(ReadOnlyBufferException.class, () -> buffer.put(0, (byte) 0xFF));
            }
        }
    }

    protected static void writeRandomBytes(ProtonBuffer buf, int length) {
        byte[] data = new byte[length];
        Random random = new Random(42);
        random.nextBytes(data);
        buf.writeBytes(data);
    }

    protected static void writeRandomBytes(byte[] array) {
        Random random = new Random(42);
        random.nextBytes(array);
    }

    protected static void writeRandomBytes(ProtonBuffer buffer) {
        Random random = new Random(42);
        int len = buffer.capacity() / Long.BYTES;
        for (int i = 0; i < len; i++) {
            // Random bytes, but lower nibble is zeroed to avoid accidentally matching our needle.
            buffer.writeLong(random.nextLong() & 0xF0F0F0F0_F0F0F0F0L);
        }
    }

    protected void assertContentEquals(ProtonBuffer buffer, byte array[], int offset, int length) {
        for (int i = 0; i < length; i++) {
            assertEquals(buffer.getByte(i), array[offset + i]);
        }
    }

    private static FileChannel tempFileChannel(Path parentDirectory) throws IOException {
        Path path = Files.createTempFile(parentDirectory, "ProtonBufferTestsChannelTest", "txt");
        return FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.DELETE_ON_CLOSE);
    }
}
