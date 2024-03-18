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

package org.apache.qpid.protonj2.buffer.netty;

import static java.nio.ByteOrder.BIG_ENDIAN;
import static java.nio.file.StandardOpenOption.DELETE_ON_CLOSE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferAllocator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import io.netty5.buffer.Buffer;
import io.netty5.buffer.BufferAllocator;
import io.netty5.buffer.BufferClosedException;
import io.netty5.buffer.BufferReadOnlyException;
import io.netty5.buffer.ByteCursor;
import io.netty5.buffer.CompositeBuffer;
import io.netty5.buffer.internal.InternalBufferUtils;
import io.netty5.util.AsciiString;

/**
 * Test the wrapper that adapts any {@link ProtonBuffer} to a Netty 5 {@link Buffer}
 */
@Timeout(20)
public class ProtonBufferToNetty5AdapterTest {

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

    //----- Tests for basic buffer lifecycle APIs

    @Test
    public void testBufferShouldNotBeAccessibleAfterClose() {
        try (Buffer buf = createProtonNetty5Buffer(24)) {
            buf.writeLong(42);
            buf.close();
            verifyInaccessible(buf);
        }
    }

    //----- Tests for access to buffer bytes by index

    @Test
    public void testOffsettedGetOfByteMustBoundsCheckOnNegativeOffset() {
        try (Buffer buf = createProtonNetty5Buffer(8)) {
            assertThrows(IndexOutOfBoundsException.class, () -> buf.getByte(-1));
        }
    }

    @Test
    public void testOffsettedGetOfByteReadOnlyMustBoundsCheckOnNegativeOffset() {
        try (Buffer buf = createProtonNetty5Buffer(8)) {
            assertThrows(IndexOutOfBoundsException.class, () -> buf.makeReadOnly().getByte(-1));
        }
    }

    @Test
    public void testOffsettedGetOfByteMustNotBoundsCheckWhenReadOffsetAndSizeIsEqualToWriteOffset() {
        try (Buffer buf = createProtonNetty5Buffer(8)) {
            byte value = 0x01;
            buf.writeByte(value);
            assertEquals(value, buf.getByte(0));
        }
    }

    @Test
    public void testOffsettedGetOfByteMustReadWithDefaultEndianByteOrder() {
        try (Buffer buf = createProtonNetty5Buffer(8)) {
            byte value = 0x01;
            buf.writeByte(value);
            buf.setByte(0, (byte) 0x10);
            assertEquals(0x10, buf.getByte(0));
        }
    }

    @Test
    public void testOffsettedGetOfByteMustBoundsCheckWhenReadOffsetAndSizeIsGreaterThanWriteOffset() {
        try (Buffer buf = createProtonNetty5Buffer(8)) {
            byte value = 0x01;
            buf.writeByte(value);
            assertThrows(IndexOutOfBoundsException.class, () -> buf.getByte(-1));
        }
    }

    @Test
    public void testOffsettedGetOfByteReadOnlyMustBoundsCheckWhenReadOffsetAndSizeIsGreaterThanWriteOffset() {
        try (Buffer buf = createProtonNetty5Buffer(8)) {
            byte value = 0x01;
            buf.writeByte(value);
            assertThrows(IndexOutOfBoundsException.class, () -> buf.makeReadOnly().getByte(-1));
        }
    }

    @Test
    public void testOffsettedGetOfByteMustNotBoundsCheckWhenReadOffsetIsGreaterThanWriteOffset() {
        try (Buffer buf = createProtonNetty5Buffer(8)) {
            buf.getByte(0);
        }
    }

    @Test
    public void testOffsettedGetOfByteMustBoundsCheckWhenReadOffsetIsGreaterThanCapacity() {
        try (Buffer buf = createProtonNetty5Buffer(8)) {
            assertThrows(IndexOutOfBoundsException.class, () -> buf.getByte(8));
        }
    }

    @Test
    public void testOffsettedGetOfByteReadOnlyMustNotBoundsCheckWhenReadOffsetIsGreaterThanWriteOffset() {
        try (Buffer buf = createProtonNetty5Buffer(8)) {
            buf.makeReadOnly().getByte(0);
        }
    }

    @Test
    public void testOffsettedGetOfByteReadOnlyMustBoundsCheckWhenReadOffsetIsGreaterThanCapacity() {
        try (Buffer buf = createProtonNetty5Buffer(8)) {
            assertThrows(IndexOutOfBoundsException.class, () -> buf.makeReadOnly().getByte(8));
        }
    }

    @Test
    public void testOffsettedGetOfUnsignedByteMustBoundsCheckOnNegativeOffset() {
        try (Buffer buf = createProtonNetty5Buffer(8)) {
            assertThrows(IndexOutOfBoundsException.class, () -> buf.getUnsignedByte(-1));
        }
    }

    @Test
    public void testOffsettedGetOfUnsignedByteReadOnlyMustBoundsCheckOnNegativeOffset() {
        try (Buffer buf = createProtonNetty5Buffer(8)) {
            assertThrows(IndexOutOfBoundsException.class, () -> buf.makeReadOnly().getUnsignedByte(-1));
        }
    }

    @Test
    public void testOffsettedGetOfUnsignedByteMustNotBoundsCheckWhenReadOffsetAndSizeIsEqualToWriteOffset() {
        try (Buffer buf = createProtonNetty5Buffer(8)) {
            int value = 0x01;
            buf.writeUnsignedByte(value);
            assertEquals(value, buf.getUnsignedByte(0));
        }
    }

    @Test
    public void testOffsettedGetOfUnsignedByteMustReadWithDefaultEndianByteOrder() {
        try (Buffer buf = createProtonNetty5Buffer(8)) {
            int value = 0x01;
            buf.writeUnsignedByte(value);
            buf.setByte(0, (byte) 0x10);
            assertEquals(0x10, buf.getUnsignedByte(0));
        }
    }

    @Test
    public void testOffsettedGetOfUnsignedByteMustNotBoundsCheckWhenReadOffsetAndSizeIsGreaterThanWriteOffset() {
        try (Buffer buf = createProtonNetty5Buffer(8)) {
            int value = 0x01;
            buf.writeUnsignedByte(value);
            buf.getUnsignedByte(1);
        }
    }

    @Test
    public void testOffsettedGetOfUnsignedByteMustBoundsCheckWhenReadOffsetAndSizeIsGreaterThanCapacity() {
        try (Buffer buf = createProtonNetty5Buffer(8)) {
            assertThrows(IndexOutOfBoundsException.class, () -> buf.getUnsignedByte(8));
        }
    }

    @Test
    public void testOffsettedGetOfUnsignedByteReadOnlyMustNotBoundsCheckWhenReadOffsetAndSizeIsGreaterThanWriteOffset() {
        try (Buffer buf = createProtonNetty5Buffer(8)) {
            int value = 0x01;
            buf.writeUnsignedByte(value);
            buf.makeReadOnly().getUnsignedByte(1);
        }
    }

    @Test
    public void testOffsettedGetOfUnsignedByteReadOnlyMustBoundsCheckWhenReadOffsetAndSizeIsGreaterThanCapacity() {
        try (Buffer buf = createProtonNetty5Buffer(8)) {
            assertThrows(IndexOutOfBoundsException.class, () -> buf.makeReadOnly().getUnsignedByte(8));
        }
    }

    @Test
    public void testOffsettedGetOfUnsignedByteMustNotBoundsCheckWhenReadOffsetIsGreaterThanWriteOffset() {
        try (Buffer buf = createProtonNetty5Buffer(8)) {
            buf.getUnsignedByte(0);
        }
    }

    @Test
    public void testOffsettedGetOfUnsignedByteMustBoundsCheckWhenReadOffsetIsGreaterThanCapacity() {
        try (Buffer buf = createProtonNetty5Buffer(8)) {
            assertThrows(IndexOutOfBoundsException.class, () -> buf.getUnsignedByte(8));
        }
    }

    @Test
    public void testOffsettedGetOfUnsignedByteReadOnlyMustNotBoundsCheckWhenReadOffsetIsGreaterThanWriteOffset() {
        try (Buffer buf = createProtonNetty5Buffer(8)) {
            buf.makeReadOnly().getUnsignedByte(0);
        }
    }

    @Test
    public void testOffsettedGetOfUnsignedByteReadOnlyMustBoundsCheckWhenReadOffsetIsGreaterThanCapacity() {
        try (Buffer buf = createProtonNetty5Buffer(8)) {
            assertThrows(IndexOutOfBoundsException.class, () -> buf.makeReadOnly().getUnsignedByte(8));
        }
    }

    @SuppressWarnings("resource")
    @Test
    public void testOffsettedSetOfByteMustBoundsCheckWhenWriteOffsetIsNegative() {
        try (Buffer buf = createProtonNetty5Buffer(8).fill((byte) 0)) {
            assertEquals(Long.BYTES, buf.capacity());
            byte value = 0x01;
            assertThrows(IndexOutOfBoundsException.class, () -> buf.setByte(-1, value));
            buf.writerOffset(Long.BYTES);
            // Verify contents are unchanged.
            assertEquals(0, buf.readLong());
        }
    }

    @SuppressWarnings("resource")
    @Test
    public void testOffsettedSetOfByteMustBoundsCheckWhenWriteOffsetAndSizeIsBeyondCapacity() {
        try (Buffer buf = createProtonNetty5Buffer(8).fill((byte) 0)) {
            assertEquals(Long.BYTES, buf.capacity());
            byte value = 0x01;
            assertThrows(IndexOutOfBoundsException.class, () -> buf.setByte(8, value));
            buf.writerOffset(Long.BYTES);
            // Verify contents are unchanged.
            assertEquals(0, buf.readLong());
        }
    }

    @Test
    public void testOffsettedSetOfByteMustHaveDefaultEndianByteOrder() {
        try (Buffer buf = createProtonNetty5Buffer(8).fill((byte) 0)) {
            byte value = 0x01;
            buf.setByte(0, value);
            buf.writerOffset(Long.BYTES);
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

    @SuppressWarnings("resource")
    @Test
    public void testOffsettedSetOfUnsignedByteMustBoundsCheckWhenWriteOffsetIsNegative() {
        try (Buffer buf = createProtonNetty5Buffer(8).fill((byte) 0)) {
            assertEquals(Long.BYTES, buf.capacity());
            int value = 0x01;
            assertThrows(IndexOutOfBoundsException.class, () -> buf.setUnsignedByte(-1, value));
            buf.writerOffset(Long.BYTES);
            // Verify contents are unchanged.
            assertEquals(0, buf.readLong());
        }
    }

    @SuppressWarnings("resource")
    @Test
    public void testOffsettedSetOfUnsignedByteMustBoundsCheckWhenWriteOffsetAndSizeIsBeyondCapacity() {
        try (Buffer buf = createProtonNetty5Buffer(8).fill((byte) 0)) {
            assertEquals(Long.BYTES, buf.capacity());
            int value = 0x01;
            assertThrows(IndexOutOfBoundsException.class, () -> buf.setUnsignedByte(8, value));
            buf.writerOffset(Long.BYTES);
            // Verify contents are unchanged.
            assertEquals(0, buf.readLong());
        }
    }

    @Test
    public void testOffsettedSetOfUnsignedByteMustHaveDefaultEndianByteOrder() {
        try (Buffer buf = createProtonNetty5Buffer(8).fill((byte) 0)) {
            int value = 0x01;
            buf.setUnsignedByte(0, value);
            buf.writerOffset(Long.BYTES);
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

    //----- Test char sequence API

    @Test
    public void testReadCharSequence() {
        try (Buffer buf = createProtonNetty5Buffer(32)) {
            String data = "Hello World";
            buf.writeBytes(data.getBytes(StandardCharsets.US_ASCII));
            assertEquals(data.length(), buf.writerOffset());
            assertEquals(0, buf.readerOffset());

            final CharSequence charSequence = buf.readCharSequence(data.length(), StandardCharsets.US_ASCII);
            assertEquals(data, charSequence.toString());
            assertEquals(data.length(), buf.writerOffset());
            assertEquals(data.length(), buf.readerOffset());
        }
    }

    @Test
    public void testWriteCharSequence() {
        try (Buffer buf = createProtonNetty5Buffer(32)) {
            AsciiString data = new AsciiString("Hello world".getBytes(StandardCharsets.US_ASCII));
            buf.writeCharSequence(data, StandardCharsets.US_ASCII);
            assertEquals(data.length(), buf.writerOffset());
            assertEquals(0, buf.readerOffset());

            final byte[] read = readByteArray(buf);
            assertEquals(data.toString(), new String(read, StandardCharsets.US_ASCII));
            assertEquals(data.length(), buf.writerOffset());
            assertEquals(data.length(), buf.readerOffset());
        }
    }

    @Test
    public void testWriteCharSequenceMustExpandCapacityIfBufferIsTooSmall() {
        try (Buffer buffer = createProtonNetty5Buffer(4)) {
            String string = "Hello World";
            buffer.writeCharSequence(string, StandardCharsets.US_ASCII);
            assertTrue(buffer.capacity() >= string.length());
        }
    }

    @Test
    public void testReadAndWriteCharSequence() {
        try (Buffer buf = createProtonNetty5Buffer(32)) {
            AsciiString data = new AsciiString("Hello world".getBytes(StandardCharsets.US_ASCII));
            buf.writeCharSequence(data, StandardCharsets.US_ASCII);
            assertEquals(data.length(), buf.writerOffset());
            assertEquals(0, buf.readerOffset());

            final CharSequence read = buf.readCharSequence(data.length(), StandardCharsets.US_ASCII);
            assertEquals(data.toString(), read.toString());
            assertEquals(data.length(), buf.writerOffset());
            assertEquals(data.length(), buf.readerOffset());
        }
    }

    //----- Tests for the buffer component iteration API

    @Test
    public void testFill() {
        try (Buffer buf = createProtonNetty5Buffer(16)) {
            assertSame(buf, buf.fill((byte) 0xA5));
            buf.writerOffset(16);
            assertEquals(0xA5A5A5A5_A5A5A5A5L, buf.readLong());
            assertEquals(0xA5A5A5A5_A5A5A5A5L, buf.readLong());
        }
    }

    @Test
    public void testComponentCountOfNonCompositeBufferMustBeOne() {
        try (Buffer buf = createProtonNetty5Buffer(8)) {
            assertEquals(1, buf.countComponents());
        }
    }

    @Test
    public void testReadableComponentCountMustBeOneIfThereAreReadableBytes() {
        try (Buffer buf = createProtonNetty5Buffer(8)) {
            assertEquals(0, buf.countReadableComponents());
            buf.writeByte((byte) 1);
            assertEquals(1, buf.countReadableComponents());
        }
    }

    @Test
    public void testWritableComponentCountMustBeOneIfThereAreWritableBytes() {
        try (Buffer buf = createProtonNetty5Buffer(8)) {
            assertEquals(1, buf.countWritableComponents());
            buf.writeLong(1);
            assertEquals(0, buf.countWritableComponents());
        }
    }

    @Test
    public void compositeBufferComponentCountMustBeTransitiveSum() {
        try (ProtonBufferAllocator allocator = ProtonBufferAllocator.defaultAllocator()) {
            try (ProtonBuffer a = allocator.allocate(8);
                 ProtonBuffer b = allocator.allocate(8);
                 ProtonBuffer c = allocator.allocate(8);
                 ProtonBuffer x = allocator.composite(new ProtonBuffer[] { b, c });
                 ProtonBuffer composite = allocator.composite(new ProtonBuffer[] { a, x });
                 ProtonBufferToNetty5Adapter buf = new ProtonBufferToNetty5Adapter(composite)) {

                assertEquals(3, buf.countComponents());
                assertEquals(0, buf.countReadableComponents());
                assertEquals(3, buf.countWritableComponents());
                buf.writeInt(1);
                assertEquals(1, buf.countReadableComponents());
                assertEquals(3, buf.countWritableComponents());
                buf.writeInt(1);
                assertEquals(1, buf.countReadableComponents());
                assertEquals(2, buf.countWritableComponents());
                buf.writeInt(1);
                assertEquals(2, buf.countReadableComponents());
                assertEquals(2, buf.countWritableComponents());
                buf.writeInt(1);
                assertEquals(2, buf.countReadableComponents());
                assertEquals(1, buf.countWritableComponents());
                buf.writeInt(1);
                assertEquals(3, buf.countReadableComponents());
                assertEquals(1, buf.countWritableComponents());
                buf.writeInt(1);
                assertEquals(3, buf.countReadableComponents());
                assertEquals(0, buf.countWritableComponents());
            }
        }
    }

    @Test
    public void testForEachComponentMustVisitBuffer() {
        final long value = 0x0102030405060708L;
        try (Buffer bufBERW = createProtonNetty5Buffer(8).writeLong(value);
             Buffer bufBERO = createProtonNetty5Buffer(8).writeLong(value).makeReadOnly()) {

            verifyReadingForEachSingleComponent(bufBERW);
            verifyReadingForEachSingleComponent(bufBERO);
        }
    }

    @Test
    public void testForEachComponentMustVisitAllReadableConstituentBuffersInOrder() {
        try (ProtonBufferAllocator allocator = ProtonBufferAllocator.defaultAllocator();
             ProtonBuffer a = allocator.allocate(4).writeInt(1);
             ProtonBuffer b = allocator.allocate(4).writeInt(2);
             ProtonBuffer c = allocator.allocate(4).writeInt(3);
             ProtonBuffer protonComposite = allocator.composite(new ProtonBuffer[] { a, b, c });
             ProtonBufferToNetty5Adapter composite = new ProtonBufferToNetty5Adapter(protonComposite)) {

            LinkedList<Integer> list = new LinkedList<Integer>(List.of(1, 2, 3));
            int index = 0;
            try (var iterator = composite.forEachComponent()) {
                for (var component = iterator.first(); component != null; component = component.next()) {
                    var buffer = component.readableBuffer();
                    int bufferValue = buffer.getInt();
                    int expectedValue = list.pollFirst().intValue();
                    assertEquals(expectedValue, bufferValue);
                    assertEquals(bufferValue, index + 1);
                    assertThrows(ReadOnlyBufferException.class, () -> buffer.put(0, (byte) 0xFF));
                    var writableBuffer = InternalBufferUtils.tryGetWritableBufferFromReadableComponent(component);
                    if (writableBuffer != null) {
                        int pos = writableBuffer.position();
                        bufferValue = writableBuffer.getInt();
                        assertEquals(expectedValue, bufferValue);
                        assertEquals(bufferValue, index + 1);
                        writableBuffer.put(pos, (byte) 0xFF);
                        assertEquals((byte) 0xFF, writableBuffer.get(pos));
                    }
                    index++;
                }
            }
            assertEquals(3, index);
            assertTrue(list.isEmpty());
        }
    }

    @Test
    public void testForEachComponentOnClosedBufferMustThrow() {
        try (Buffer writableBuffer = createProtonNetty5Buffer(8);
             Buffer readableBuffer = createProtonNetty5Buffer(8)) {

            writableBuffer.close();
            assertThrows(BufferClosedException.class, () -> writableBuffer.forEachComponent());

            readableBuffer.writeLong(0);
            readableBuffer.close();
            assertThrows(BufferClosedException.class, () -> readableBuffer.forEachComponent());
        }
    }

    @Test
    public void testForEachComponentMustAllowCollectingBuffersInArray() {
        try (ProtonBufferAllocator allocator = ProtonBufferAllocator.defaultAllocator()) {
            try (ProtonBuffer a = allocator.allocate(4);
                 ProtonBuffer b = allocator.allocate(4);
                 ProtonBuffer c = allocator.allocate(4);
                 ProtonBuffer protonComposite = allocator.composite(new ProtonBuffer[] { a, b, c });
                 Buffer composite = new ProtonBufferToNetty5Adapter(protonComposite)) {

                int i = 1;
                while (composite.writableBytes() > 0) {
                    composite.writeByte((byte) i++);
                }
                ByteBuffer[] buffers = new ByteBuffer[composite.countComponents()];
                try (var iterator = composite.forEachComponent()) {
                    int index = 0;
                    for (var component = iterator.first(); component != null; component = component.next()) {
                        buffers[index] = component.readableBuffer();
                        index++;
                    }
                }
                i = 1;
                assertTrue(buffers.length >= 1);
                for (ByteBuffer buffer : buffers) {
                    while (buffer.hasRemaining()) {
                        assertEquals((byte) i++, buffer.get());
                    }
                }
            }

            try (ProtonBuffer singleBuffer = allocator.allocate(4);
                 Buffer single = new ProtonBufferToNetty5Adapter(singleBuffer)) {
                ByteBuffer[] buffers = new ByteBuffer[single.countComponents()];
                try (var iterator = single.forEachComponent()) {
                    int index = 0;
                    for (var component = iterator.first(); component != null; component = component.next()) {
                        buffers[index] = component.writableBuffer();
                        index++;
                    }
                }
                assertTrue(buffers.length >= 1);
                int i = 1;
                for (ByteBuffer buffer : buffers) {
                    while (buffer.hasRemaining()) {
                        buffer.put((byte) i++);
                    }
                }
                single.writerOffset(single.capacity());
                i = 1;
                while (single.readableBytes() > 0) {
                    assertEquals((byte) i++, single.readByte());
                }
            }
        }
    }

    @Test
    public void testForEachComponentMustExposeByteCursors() {
        try (ProtonBufferAllocator allocator = ProtonBufferAllocator.defaultAllocator();
             ProtonBuffer inner = allocator.allocate(20);
             Buffer buf = new ProtonBufferToNetty5Adapter(inner)) {

            buf.writeLong(0x0102030405060708L);
            buf.writeLong(0x1112131415161718L);
            assertEquals(0x01020304, buf.readInt());
            try (ProtonBuffer innerActual = allocator.allocate(buf.readableBytes());
                 Buffer actualData = new ProtonBufferToNetty5Adapter(innerActual);
                 ProtonBuffer innerExpected = allocator.allocate(12);
                 Buffer expectedData = new ProtonBufferToNetty5Adapter(innerExpected)) {

                expectedData.writeInt(0x05060708);
                expectedData.writeInt(0x11121314);
                expectedData.writeInt(0x15161718);

                try (var iterator = buf.forEachComponent()) {
                    for (var component = iterator.first(); component != null; component = component.next()) {
                        ByteCursor forward = component.openCursor();
                        while (forward.readByte()) {
                            actualData.writeByte(forward.getByte());
                        }
                    }
                }

                assertEquals(expectedData.readableBytes(), actualData.readableBytes());
                while (expectedData.readableBytes() > 0) {
                    assertEquals(expectedData.readByte(), actualData.readByte());
                }
            }
        }
    }

    @Test
    public void testForEachComponentMustExposeByteCursorsPartial() {
        try (ProtonBufferAllocator allocator = ProtonBufferAllocator.defaultAllocator();
             ProtonBuffer inner = allocator.allocate(32);
             Buffer buf = new ProtonBufferToNetty5Adapter(inner)) {

            buf.writeLong(0x0102030405060708L);
            buf.writeLong(0x1112131415161718L);
            assertEquals(0x01020304, buf.readInt());
            try (ProtonBuffer innerActual = allocator.allocate(buf.readableBytes());
                 Buffer actualData = new ProtonBufferToNetty5Adapter(innerActual);
                 ProtonBuffer innerExpected = allocator.allocate(12);
                 Buffer expectedData = new ProtonBufferToNetty5Adapter(innerExpected)) {

                expectedData.writeInt(0x05060708);
                expectedData.writeInt(0x11121314);
                expectedData.writeInt(0x15161718);

                try (var iterator = buf.forEachComponent()) {
                    for (var component = iterator.first(); component != null; component = component.next()) {
                        ByteCursor forward = component.openCursor();
                        while (forward.readByte()) {
                            actualData.writeByte(forward.getByte());
                        }
                    }
                }

                assertEquals(expectedData.readableBytes(), actualData.readableBytes());
                while (expectedData.readableBytes() > 0) {
                    assertEquals(expectedData.readByte(), actualData.readByte());
                }
            }
        }
    }

    @Test
    public void testForEachComponentMustReturnNullFirstWhenNotReadable() {
        try (ProtonBufferAllocator allocator = ProtonBufferAllocator.defaultAllocator();
             ProtonBuffer inner = allocator.allocate(0);
             Buffer buf = new ProtonBufferToNetty5Adapter(inner)) {

            try (var iterator = buf.forEachComponent()) {
                // First component may or may not be null.
                var component = iterator.first();
                if (component != null) {
                    assertEquals(0, component.readableBytes());
                    assertEquals(0, component.readableArrayLength());
                    ByteBuffer byteBuffer = component.readableBuffer();
                    assertEquals(0, byteBuffer.remaining());
                    assertEquals(0, byteBuffer.capacity());
                    assertNull(component.next());
                }
            }
            try (var iterator = buf.forEachComponent()) {
                // First *readable* component is definitely null.
                assertNull(iterator.firstReadable());
            }
        }
    }

    @Test
    public void testForEachWritableMustVisitBuffer() {
        try (Buffer bufBERW = createProtonNetty5Buffer(8)) {
            verifyWritingForEachSingleComponent(bufBERW);
        }
    }

    @Test
    public void testForEachComponentMustVisitAllWritableConstituentBuffersInOrder() {
        try (ProtonBufferAllocator allocator = ProtonBufferAllocator.defaultAllocator();
             ProtonBuffer a = allocator.allocate(8);
             ProtonBuffer b = allocator.allocate(8);
             ProtonBuffer c = allocator.allocate(8);
             ProtonBuffer protonComposite = allocator.composite(new ProtonBuffer[] { a, b, c });
             ProtonBufferToNetty5Adapter buf = new ProtonBufferToNetty5Adapter(protonComposite)) {

            try (var iterator = buf.forEachComponent()) {
                int index = 0;
                for (var component = iterator.first(); component != null; component = component.next()) {
                    component.writableBuffer().putLong(0x0102030405060708L + 0x1010101010101010L * index);
                    index++;
                }
            }
            buf.writerOffset(3 * 8);
            assertEquals(0x0102030405060708L, buf.readLong());
            assertEquals(0x1112131415161718L, buf.readLong());
            assertEquals(0x2122232425262728L, buf.readLong());
        }
    }

    @Test
    public void testForEachComponentChangesMadeToByteBufferComponentMustBeReflectedInBuffer() {
        try (Buffer buf = createProtonNetty5Buffer(9)) {
            buf.writeByte((byte) 0xFF);
            AtomicInteger writtenCounter = new AtomicInteger();
            try (var iterator = buf.forEachComponent()) {
                for (var component = iterator.first(); component != null; component = component.next()) {
                    ByteBuffer buffer = component.writableBuffer();
                    while (buffer.hasRemaining()) {
                        buffer.put((byte) writtenCounter.incrementAndGet());
                    }
                }
            }
            buf.writerOffset(9);
            assertEquals((byte) 0xFF, buf.readByte());
            assertEquals(0x0102030405060708L, buf.readLong());
        }
    }

    @Test
    public void testForEachComponentMustHaveZeroWritableBytesWhenNotWritable() {
        try (Buffer buf = createProtonNetty5Buffer(0)) {
            try (var iterator = buf.forEachComponent()) {
                // First component may or may not be null.
                var component = iterator.first();
                if (component != null) {
                    assertEquals(0, component.writableBytes());
                    assertEquals(0, component.writableArrayLength());
                    ByteBuffer byteBuffer = component.writableBuffer();
                    assertEquals(0, byteBuffer.remaining());
                    assertEquals(0, byteBuffer.capacity());
                    assertNull(component.next());
                }
            }
            try (var iterator = buf.forEachComponent()) {
                // First *writable* component is definitely null.
                assertNull(iterator.firstWritable());
            }
        }
    }

    @Test
    public void testChangesMadeToByteBufferComponentsInIterationShouldBeReflectedInBuffer() {
        try (Buffer buf = createProtonNetty5Buffer(8)) {
            AtomicInteger counter = new AtomicInteger();
            try (var iterator = buf.forEachComponent()) {
                for (var component = iterator.first(); component != null; component = component.next()) {
                    ByteBuffer buffer = component.writableBuffer();
                    while (buffer.hasRemaining()) {
                        buffer.put((byte) counter.incrementAndGet());
                    }
                }
            }
            buf.writerOffset(buf.capacity());
            for (int i = 0; i < 8; i++) {
                assertEquals((byte) i + 1, buf.getByte(i));
            }
        }
    }

    @Test
    public void testComponentsOfReadOnlyBufferMustNotHaveAnyWritableBytes() {
        try (Buffer buf = createProtonNetty5Buffer(8).makeReadOnly()) {
            try (var iteration = buf.forEachComponent()) {
                for (var c = iteration.first(); c != null; c = c.next()) {
                    assertEquals(0, c.writableBytes());
                    assertEquals(0, c.writableArrayLength());
                    ByteBuffer byteBuffer = c.writableBuffer();
                    assertEquals(0, byteBuffer.remaining());
                    assertEquals(0, byteBuffer.capacity());
                }
            }
            try (var iteration = buf.forEachComponent()) {
                assertNull(iteration.firstWritable());
            }
        }
    }

    @Test
    public void forEachComponentMustBeAbleToIncrementReaderOffset() {
        try (Buffer buf = createProtonNetty5Buffer(8);
             Buffer target = createProtonNetty5Buffer(5)) {

            buf.writeLong(0x0102030405060708L);
            try (var iterator = buf.forEachComponent()) {
                for (var component = iterator.first(); component != null; component = component.next()) {
                    while (target.writableBytes() > 0 && component.readableBytes() > 0) {
                        ByteBuffer byteBuffer = component.readableBuffer();
                        byte value = byteBuffer.get();
                        byteBuffer.clear();
                        target.writeByte(value);
                        var cmp = component; // Capture for lambda.
                        assertThrows(IndexOutOfBoundsException.class, () -> cmp.skipReadableBytes(9));
                        component.skipReadableBytes(0);
                        component.skipReadableBytes(1);
                    }
                }
            }

            assertEquals(5, buf.readerOffset());
            assertEquals(3, buf.readableBytes());
            assertEquals(5, target.readableBytes());

            try (Buffer expected = BufferAllocator.onHeapUnpooled().copyOf(new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05 })) {
                assertEquals(target, expected);
            }
            try (Buffer expected = BufferAllocator.onHeapUnpooled().copyOf(new byte[] { 0x06, 0x07, 0x08 })) {
                assertEquals(buf, expected);
            }
        }
    }

    @Test
    public void testForEachComponentMustBeAbleToIncrementWriterOffset() {
        try (Buffer buf = createProtonNetty5Buffer(8).writeLong(0x0102030405060708L);
             Buffer target = buf.copy()) {

            buf.writerOffset(0); // Prime the buffer with data, but leave the write-offset at zero.
            try (var iterator = buf.forEachComponent()) {
                for (var component = iterator.first(); component != null; component = component.next()) {
                    while (component.writableBytes() > 0) {
                        ByteBuffer byteBuffer = component.writableBuffer();
                        byte value = byteBuffer.get();
                        byteBuffer.clear();
                        assertEquals(value, target.readByte());
                        var cmp = component; // Capture for lambda.
                        assertThrows(IndexOutOfBoundsException.class, () -> cmp.skipWritableBytes(9));
                        component.skipWritableBytes(0);
                        component.skipWritableBytes(1);
                    }
                }
            }

            assertEquals(8, buf.writerOffset());
            assertEquals(8, target.readerOffset());
            target.readerOffset(0);
            assertEquals(buf, target);
        }
    }

    @Test
    public void negativeSkipReadableOnReadableComponentMustThrow() {
        try (Buffer buf = createProtonNetty5Buffer(8)) {
            buf.writeLong(0x0102030405060708L);
            assertEquals(0x01020304, buf.readInt());
            try (var iterator = buf.forEachComponent()) {
                for (var component = iterator.first(); component != null; component = component.next()) {
                    var cmp = component; // Capture for lambda.
                    assertThrows(IllegalArgumentException.class, () -> cmp.skipReadableBytes(-1));
                }
            }
        }
    }

    @Test
    public void negativeSkipWritableOnWritableComponentMustThrow() {
        try (Buffer buf = createProtonNetty5Buffer(8)) {
            try (var iterator = buf.forEachComponent()) {
                for (var component = iterator.first(); component != null; component = component.next()) {
                    var cmp = component; // Capture for lambda.
                    assertThrows(IllegalArgumentException.class, () -> cmp.skipWritableBytes(-1));
                }
            }
        }
    }

    //----- Test for channel API methods

    @Test
    public void testTransferToMustThrowIfBufferIsClosed() throws IOException {
        long position = channel.position();
        long size = channel.size();
        Buffer empty = createProtonNetty5Buffer(8);
        empty.close();
        assertThrows(BufferClosedException.class, () -> empty.transferTo(channel, 8));
        assertEquals(position, channel.position());
        assertEquals(size, channel.size());
        Buffer withData = createProtonNetty5Buffer(8);
        withData.writeLong(0x0102030405060708L);
        withData.close();
        assertThrows(BufferClosedException.class, () -> withData.transferTo(channel, 8));
        assertEquals(position, channel.position());
        assertEquals(size, channel.size());
    }

    @Test
    public void testTransferToMustCapAtReadableBytes() throws IOException {
        try (Buffer buf = createProtonNetty5Buffer(8)) {
            buf.writeLong(0x0102030405060708L);
            buf.writerOffset(buf.writerOffset() - 5);
            long position = channel.position();
            long size = channel.size();
            int bytesWritten = buf.transferTo(channel, 8);

            assertEquals(3, bytesWritten);
            assertEquals(3 + position, channel.position());
            assertEquals(3 + size, channel.size());
            assertEquals(5, buf.writableBytes());
            assertEquals(0, buf.readableBytes());
        }
    }

    @Test
    public void testTransferToMustCapAtLength() throws IOException {
        try (Buffer buf = createProtonNetty5Buffer(8)) {
            buf.writeLong(0x0102030405060708L);
            long position = channel.position();
            long size = channel.size();
            int bytesWritten = buf.transferTo(channel, 3);
            assertEquals(3, bytesWritten);
            assertEquals(3 + position, channel.position());
            assertEquals(3 + size, channel.size());
            assertEquals(5, buf.readableBytes());
        }
    }

    @Test
    public void testTransferToMustThrowIfChannelIsClosed() throws IOException {
        try (Buffer buf = createProtonNetty5Buffer(8)) {
            buf.writeLong(0x0102030405060708L);
            assertThrows(ClosedChannelException.class, () -> buf.transferTo(closedChannel, 8));
            assertTrue(buf.isAccessible());
            assertEquals(8, buf.readableBytes());
        }
    }

    @Test
    public void testTransferToMustThrowIfChannelIsNull() throws IOException {
        try (Buffer buf = createProtonNetty5Buffer(8)) {
            buf.writeLong(0x0102030405060708L);
            assertThrows(NullPointerException.class, () -> buf.transferTo(null, 8));
            assertTrue(buf.isAccessible());
            assertEquals(8, buf.readableBytes());
        }
    }

    @Test
    public void testTransferToMustThrowIfLengthIsNegative() throws IOException {
        try (Buffer buf = createProtonNetty5Buffer(8)) {
            buf.writeLong(0x0102030405060708L);
            assertThrows(IllegalArgumentException.class, () -> buf.transferTo(channel, -1));
            assertEquals(8, buf.readableBytes());
        }
    }

    @Test
    public void transferToMustIgnoreZeroLengthOperations() throws IOException {
        try (Buffer buf = createProtonNetty5Buffer(8)) {
            long position = channel.position();
            long size = channel.size();
            buf.writeLong(0x0102030405060708L);
            int bytesWritten = buf.transferTo(channel, 0);
            assertEquals(8, buf.readableBytes());
            assertEquals(0, bytesWritten);
            assertEquals(position, channel.position());
            assertEquals(size, channel.size());
        }
    }

    @Test
    public void testTransferToMustMoveDataToChannel() throws IOException {
        try (Buffer buf = createProtonNetty5Buffer(8)) {
            long value = ThreadLocalRandom.current().nextLong();
            buf.writeLong(value);
            long position = channel.position();
            int bytesWritten = buf.transferTo(channel, 8);
            assertEquals(8, bytesWritten);
            ByteBuffer buffer = ByteBuffer.allocate(8);
            final int bytesRead = channel.read(buffer, position);
            assertEquals(8, bytesRead);
            buffer.flip();
            assertEquals(value, buffer.getLong());
        }
    }

    @Test
    public void testTransferToMustMoveReadOnlyDataToChannel() throws IOException {
        try (Buffer buf = createProtonNetty5Buffer(8)) {
            long value = ThreadLocalRandom.current().nextLong();
            buf.writeLong(value).makeReadOnly();
            long position = channel.position();
            int bytesWritten = buf.transferTo(channel, 8);
            assertEquals(8, bytesWritten);
            ByteBuffer buffer = ByteBuffer.allocate(8);
            int bytesRead = channel.read(buffer, position);
            assertEquals(8, bytesRead);
            buffer.flip();
            assertEquals(value, buffer.getLong());
        }
    }

    @Test
    public void testTransferToZeroBytesMustNotThrowOnClosedChannel() throws IOException {
        try (Buffer empty = createProtonNetty5Buffer(0);
             Buffer notEmpty = createProtonNetty5Buffer(4).writeInt(42)) {

            empty.transferTo(closedChannel, 4);
            notEmpty.transferTo(closedChannel, 0);
        }
    }

    @Test
    public void testTransferFromMustThrowIfBufferIsClosed() throws IOException {
        doTransferFromMustThrowIfBufferIsClosed(false);
    }

    @Test
    public void testTransferFromWithPositionMustThrowIfBufferIsClosed() throws IOException {
        doTransferFromMustThrowIfBufferIsClosed(true);
    }

    private static void doTransferFromMustThrowIfBufferIsClosed(boolean withPosition) throws IOException {
        try {
            ByteBuffer data = ByteBuffer.allocate(8).putLong(0x0102030405060708L).flip();
            long position = channel.position();
            assertEquals(8, channel.write(data, position));
            long size = channel.size();
            Buffer empty = createProtonNetty5Buffer(0);
            empty.close();
            assertThrows(BufferClosedException.class, () -> {
                if (withPosition) {
                    empty.transferFrom(channel, 4 + position, 8);
                } else {
                    empty.transferFrom(channel, 8);
                }
            });
            assertEquals(position, channel.position());
            assertEquals(size, channel.size());
            Buffer withAvailableSpace = createProtonNetty5Buffer(8);
            withAvailableSpace.close();
            assertThrows(BufferClosedException.class, () -> {
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
        doTransferFromMustCapAtWritableBytes(false);
    }

    @Test
    public void testTransferFromWithPositionMustCapAtWritableBytes() throws IOException {
        doTransferFromMustCapAtWritableBytes(true);
    }

    private static void doTransferFromMustCapAtWritableBytes(boolean withPosition) throws IOException {
        try (Buffer buf = createProtonNetty5Buffer(3)) {
            ByteBuffer data = ByteBuffer.allocate(8).putLong(0x0102030405060708L).flip();
            long position = channel.position();
            assertEquals(8, channel.write(data, position));
            long size = channel.size();
            int bytesRead = withPosition ? buf.transferFrom(channel, 4 + position, 8) : buf.transferFrom(channel, 8);
            assertEquals(3, bytesRead);
            assertEquals(withPosition ? position : 3 + position, channel.position());
            assertEquals(size, channel.size());
            assertEquals(0, buf.writableBytes());
            assertEquals(3, buf.readableBytes());
        } finally {
            channel.position(channel.size());
        }
    }

    @Test
    public void testTransferFromMustCapAtLength() throws IOException {
        doTransferFromMustCapAtLength(false);
    }

    @Test
    public void testTransferFromWithPositionMustCapAtLength() throws IOException {
        doTransferFromMustCapAtLength(true);
    }

    private static void doTransferFromMustCapAtLength(boolean withPosition) throws IOException {
        try (Buffer buf = createProtonNetty5Buffer(8)) {
            ByteBuffer data = ByteBuffer.allocate(8).putLong(0x0102030405060708L).flip();
            long position = channel.position();
            assertEquals(8, channel.write(data, position));
            long size = channel.size();
            int bytesRead = withPosition ? buf.transferFrom(channel, 4 + position, 3) : buf.transferFrom(channel, 3);
            assertEquals(3, bytesRead);
            assertEquals(withPosition ? position : 3 + position, channel.position());
            assertEquals(size, channel.size());
            assertEquals(5, buf.writableBytes());
            assertEquals(3, buf.readableBytes());
        } finally {
            channel.position(channel.size());
        }
    }

    @Test
    public void testTransferFromMustThrowIfChannelIsClosed() {
        doTransferFromMustThrowIfChannelIsClosed(false);
    }

    @Test
    public void testTransferFromWithPositionMustThrowIfChannelIsClosed() {
        doTransferFromMustThrowIfChannelIsClosed(true);
    }

    private static void doTransferFromMustThrowIfChannelIsClosed(boolean withPosition) {
        try (Buffer buf = createProtonNetty5Buffer(8)) {
            assertThrows(ClosedChannelException.class, () -> {
                if (withPosition) {
                    buf.transferFrom(closedChannel, 4, 8);
                } else {
                    buf.transferFrom(closedChannel, 8);
                }
            });
            assertTrue(buf.isAccessible());
            assertEquals(8, buf.writableBytes());
        }
    }

    @Test
    public void testTransferFromMustThrowIfChannelIsNull() {
        doTransferFromMustThrowIfChannelIsNull(false);
    }

    @Test
    public void testTransferFromWithPositionMustThrowIfChannelIsNull() {
        doTransferFromMustThrowIfChannelIsNull(true);
    }

    private static void doTransferFromMustThrowIfChannelIsNull(boolean withPosition) {
        try (Buffer buf = createProtonNetty5Buffer(8)) {
            assertThrows(NullPointerException.class, () -> {
                if (withPosition) {
                    buf.transferFrom(null, 4, 8);
                } else {
                    buf.transferFrom(null, 8);
                }
            });
            assertTrue(buf.isAccessible());
            assertEquals(8, buf.writableBytes());
        }
    }

    @Test
    public void testTransferFromMustThrowIfLengthIsNegative() throws IOException {
        doTransferFromMustThrowIfLengthIsNegative(false);
    }

    @Test
    public void testTransferFromWithPositionMustThrowIfLengthIsNegative() throws IOException {
        doTransferFromMustThrowIfLengthIsNegative(true);
    }

    private static void doTransferFromMustThrowIfLengthIsNegative(boolean withPosition) throws IOException {
        try (Buffer buf = createProtonNetty5Buffer(8)) {
            long position = channel.position();
            ByteBuffer data = ByteBuffer.allocate(8).putLong(0x0102030405060708L).flip();
            assertEquals(8, channel.write(data, position));
            long size = channel.size();
            assertThrows(IllegalArgumentException.class, () -> {
                if (withPosition) {
                    buf.transferFrom(channel, 4 + position, -1);
                } else {
                    buf.transferFrom(channel, -1);
                }
            });
            assertTrue(buf.isAccessible());
            assertEquals(8, buf.writableBytes());
            assertEquals(position, channel.position());
            assertEquals(size, channel.size());
        } finally {
            channel.position(channel.size());
        }
    }

    @Test
    public void testTransferFromMustIgnoreZeroLengthOperations() throws IOException {
        doTransferFromMustIgnoreZeroLengthOperations(false);
    }

    @Test
    public void testTransferFromWithPositionMustIgnoreZeroLengthOperations() throws IOException {
        doTransferFromMustIgnoreZeroLengthOperations(true);
    }

    private static void doTransferFromMustIgnoreZeroLengthOperations(boolean withPosition) throws IOException {
        try (Buffer buf = createProtonNetty5Buffer(8)) {
            long position = channel.position();
            ByteBuffer data = ByteBuffer.allocate(8).putLong(0x0102030405060708L).flip();
            assertEquals(8, channel.write(data, position));
            long size = channel.size();
            int bytesRead = withPosition ? buf.transferFrom(channel, 4 + position, 0) : buf.transferFrom(channel, 0);
            assertEquals(0, bytesRead);
            assertEquals(0, buf.readableBytes());
            assertEquals(8, buf.writableBytes());
            assertEquals(position, channel.position());
            assertEquals(size, channel.size());
        } finally {
            channel.position(channel.size());
        }
    }

    @Test
    public void testTransferFromMustMoveDataFromChannel() throws IOException {
        doTransferFromMustMoveDataFromChannel(false);
    }

    @Test
    public void testTransferFromWithPositionMustMoveDataFromChannel() throws IOException {
        doTransferFromMustMoveDataFromChannel(true);
    }

    private static void doTransferFromMustMoveDataFromChannel(boolean withPosition) throws IOException {
        try (Buffer buf = createProtonNetty5Buffer(8)) {
            long value = ThreadLocalRandom.current().nextLong();
            ByteBuffer data = ByteBuffer.allocate(8).putLong(value).flip();
            long position = channel.position();
            assertEquals(8, channel.write(data, position));
            long size = channel.size();
            int bytesRead = withPosition ? buf.transferFrom(channel, position, 8) : buf.transferFrom(channel, 8);
            assertEquals(8, bytesRead);
            assertEquals(8, buf.readableBytes());
            assertEquals(0, buf.writableBytes());
            assertEquals(withPosition ? position : 8 + position, channel.position());
            assertEquals(size, channel.size());
            assertEquals(value, buf.readLong());
        } finally {
            channel.position(channel.size());
        }
    }

    @Test
    public void testTransferFromMustNotReadBeyondEndOfChannel() throws IOException {
        doTransferFromMustNotReadBeyondEndOfChannel(false);
    }

    @Test
    public void testTransferFromWithPositionMustNotReadBeyondEndOfChannel() throws IOException {
        doTransferFromMustNotReadBeyondEndOfChannel(true);
    }

    private static void doTransferFromMustNotReadBeyondEndOfChannel(boolean withPosition) throws IOException {
        try (Buffer buf = createProtonNetty5Buffer(8)) {
            ByteBuffer data = ByteBuffer.allocate(8).putInt(0x01020304).flip();
            long position = channel.position();
            assertEquals(4, channel.write(data, position));
            long size = channel.size();
            int bytesRead = withPosition ? buf.transferFrom(channel, position, 8) : buf.transferFrom(channel, 8);
            assertEquals(4, bytesRead);
            assertEquals(4, buf.readableBytes());
            assertEquals(withPosition ? position : 4 + position, channel.position());
            assertEquals(size, channel.size());
        } finally {
            channel.position(channel.size());
        }
    }

    @Test
    public void testTransferFromMustReturnMinusOneForEndOfStream() throws IOException {
        doTransferFromMustReturnMinusOneForEndOfStream(false);
    }

    @Test
    public void testTransferFromWithPositionMustReturnMinusOneForEndOfStream() throws IOException {
        doTransferFromMustReturnMinusOneForEndOfStream(true);
    }

    private static void doTransferFromMustReturnMinusOneForEndOfStream(boolean withPosition) throws IOException {
        try (Buffer buf = createProtonNetty5Buffer(8)) {
            long position = channel.position();
            long size = channel.size();
            int bytesRead = withPosition ? buf.transferFrom(channel, position, 8) : buf.transferFrom(channel, 8);
            assertEquals(-1, bytesRead);
            assertEquals(0, buf.readableBytes());
            assertEquals(position, channel.position());
            assertEquals(size, channel.size());
        } finally {
            channel.position(channel.size());
        }
    }

    @Test
    public void testTransferFromMustReturnMinusOneForEndOfStreamNonScattering() throws IOException {
        try (Buffer buf = createProtonNetty5Buffer(8)) {
            long position = channel.position();
            long size = channel.size();
            ReadableByteChannel nonScatteringChannel = new ReadableByteChannel() {
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
            final int bytesRead = buf.transferFrom(nonScatteringChannel, 8);
            assertEquals(-1, bytesRead);
            assertEquals(0, buf.readableBytes());
            assertEquals(position, channel.position());
            assertEquals(size, channel.size());
        } finally {
            channel.position(channel.size());
        }
    }

    @Test
    public void testTransferFromMustStartFromWritableOffset() throws IOException {
        doTransferFromMustStartFromWritableOffset(false);
    }

    @Test
    public void testTransferFromWithPositionMustStartFromWritableOffset() throws IOException {
        doTransferFromMustStartFromWritableOffset(true);
    }

    private static void doTransferFromMustStartFromWritableOffset(boolean withPosition) throws IOException {
        try (Buffer buf = createProtonNetty5Buffer(4)) {
            ByteBuffer data = ByteBuffer.allocate(4).putInt(0x01020304).flip();
            long position = channel.position();
            assertEquals(4, channel.write(data, position));
            long size = channel.size();
            int bytesRead = withPosition ? buf.transferFrom(channel, position, 2) : buf.transferFrom(channel, 2);
            bytesRead += withPosition ? buf.transferFrom(channel, 2 + position, 2) : buf.transferFrom(channel, 2);
            assertEquals(4, bytesRead);
            assertEquals(4, buf.readableBytes());
            assertEquals(withPosition ? position : 4 + position, channel.position());
            assertEquals(size, channel.size());
            for (int i = 0; i < buf.readableBytes(); i++) {
                assertEquals(data.get(i), buf.readByte());
            }
        } finally {
            channel.position(channel.size());
        }
    }

    @Test
    public void testTransferFromMustThrowIfBufferIsReadOnly() throws IOException {
        doTransferFromMustThrowIfBufferIsReadOnly(false);
    }

    @Test
    public void testTransferFromWithPositionMustThrowIfBufferIsReadOnly() throws IOException {
        doTransferFromMustThrowIfBufferIsReadOnly(true);
    }

    private static void doTransferFromMustThrowIfBufferIsReadOnly(boolean withPosition) throws IOException {
        try (Buffer buf = createProtonNetty5Buffer(8).writeLong(0x0102030405060708L).makeReadOnly()) {
            long position = channel.position();
            long size = channel.size();
            assertThrows(BufferReadOnlyException.class, () -> {
                if (withPosition) {
                    buf.transferFrom(channel, 4 + position, 8);
                } else {
                    buf.transferFrom(channel, 8);
                }
            });
            assertEquals(8, buf.readableBytes());
            assertEquals(position, channel.position());
            assertEquals(size, channel.size());
        }
    }

    @Test
    public void testTransferFromZeroBytesMustNotThrowOnClosedChannel() throws IOException {
        doTransferFromZeroBytesMustNotThrowOnClosedChannel(false);
    }

    @Test
    public void testTransferFromWithPositionZeroBytesMustNotThrowOnClosedChannel() throws IOException {
        doTransferFromZeroBytesMustNotThrowOnClosedChannel(true);
    }

    private static void doTransferFromZeroBytesMustNotThrowOnClosedChannel(boolean withPosition) throws IOException {
        try (Buffer empty = createProtonNetty5Buffer(0);
             Buffer notEmpty = createProtonNetty5Buffer(4)) {

            if (withPosition) {
                empty.transferFrom(closedChannel, 4, 4);
                notEmpty.transferFrom(closedChannel, 4, 0);
            } else {
                empty.transferFrom(closedChannel, 4);
                notEmpty.transferFrom(closedChannel, 0);
            }
        }
    }

    private static void verifyReadingForEachSingleComponent(Buffer buf) {
        try (var iterator = buf.forEachComponent()) {
            for (var component = iterator.first(); component != null; component = component.next()) {
                ByteBuffer buffer = component.readableBuffer();
                assertEquals(0, buffer.position());
                assertEquals(8, buffer.limit());
                assertEquals(8, buffer.capacity());

                assertEquals(0x0102030405060708L, buffer.getLong());

                if (component.hasReadableArray()) {
                    byte[] array = component.readableArray();
                    byte[] arrayCopy = new byte[component.readableArrayLength()];
                    System.arraycopy(array, component.readableArrayOffset(), arrayCopy, 0, arrayCopy.length);
                    if (buffer.order() == BIG_ENDIAN) {
                        assertArrayEquals(arrayCopy, new byte[] {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08});
                    } else {
                        assertArrayEquals(arrayCopy, new byte[] {0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x0});
                    }
                }

                assertThrows(ReadOnlyBufferException.class, () -> buffer.put(0, (byte) 0xFF));
            }
        }
    }

    public static void verifyWritingForEachSingleComponent(Buffer buf) {
        buf.fill((byte) 0);
        try (var iterator = buf.forEachComponent()) {
            for (var component = iterator.first(); component != null; component = component.next()) {
                ByteBuffer buffer = component.writableBuffer();
                assertEquals(0, buffer.position());
                assertEquals(8, buffer.limit());
                assertEquals(8, buffer.capacity());
                buffer.putLong(0x0102030405060708L);
                buffer.flip();
                assertEquals(0x0102030405060708L, buffer.getLong());
                buf.writerOffset(8);
                assertEquals(0x0102030405060708L, buf.getLong(0));

                assertEquals(0, component.writableNativeAddress());

                buf.writerOffset(0);
                if (component.hasWritableArray()) {
                    byte[] array = component.writableArray();
                    int offset = component.writableArrayOffset();
                    byte[] arrayCopy = new byte[component.writableArrayLength()];
                    System.arraycopy(array, offset, arrayCopy, 0, arrayCopy.length);
                    if (buffer.order() == BIG_ENDIAN) {
                        assertArrayEquals(arrayCopy, new byte[] {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08});
                    } else {
                        assertArrayEquals(arrayCopy, new byte[] {0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x0});
                    }
                }

                buffer.put(0, (byte) 0xFF);
                assertEquals((byte) 0xFF, buffer.get(0));
                assertEquals((byte) 0xFF, buf.getByte(0));
            }
        }
    }
    private static Buffer createProtonNetty5Buffer(int capacity) {
        ProtonBuffer protonBuffer = ProtonBufferAllocator.defaultAllocator().allocate(capacity);
        ProtonBufferToNetty5Adapter wrapper = new ProtonBufferToNetty5Adapter(protonBuffer);

        return wrapper;
    }

    private static FileChannel tempFileChannel(Path parentDirectory) throws IOException {
        Path path = Files.createTempFile(parentDirectory, "BufferAndChannelTest", "txt");
        return FileChannel.open(path, READ, WRITE, DELETE_ON_CLOSE);
    }

    public static byte[] readByteArray(Buffer buf) {
        byte[] bs = new byte[buf.readableBytes()];
        buf.copyInto(buf.readerOffset(), bs, 0, bs.length);
        buf.readerOffset(buf.writerOffset());
        return bs;
    }

    @SuppressWarnings("resource")
    private static void verifyInaccessible(Buffer buf) {
        verifyReadInaccessible(buf);

        verifyWriteInaccessible(buf, BufferClosedException.class);

        try (BufferAllocator allocator = BufferAllocator.onHeapUnpooled();
             Buffer target = allocator.allocate(24)) {
            assertThrows(BufferClosedException.class, () -> buf.copyInto(0, target, 0, 1));
            assertThrows(BufferClosedException.class, () -> buf.copyInto(0, target, 0, 0));
            assertThrows(BufferClosedException.class, () -> buf.copyInto(0, new byte[1], 0, 1));
            assertThrows(BufferClosedException.class, () -> buf.copyInto(0, new byte[1], 0, 0));
            assertThrows(BufferClosedException.class, () -> buf.copyInto(0, ByteBuffer.allocate(1), 0, 1));
            assertThrows(BufferClosedException.class, () -> buf.copyInto(0, ByteBuffer.allocate(1), 0, 0));
            if (CompositeBuffer.isComposite(buf)) {
                assertThrows(BufferClosedException.class, () -> ((CompositeBuffer) buf).extendWith(target.send()));
            }
        }

        assertThrows(BufferClosedException.class, () -> buf.split());
        assertThrows(BufferClosedException.class, () -> buf.send());
        assertThrows(BufferClosedException.class, () -> buf.copy());
        assertThrows(BufferClosedException.class, () -> buf.openCursor());
        assertThrows(BufferClosedException.class, () -> buf.openCursor(0, 0));
        assertThrows(BufferClosedException.class, () -> buf.openReverseCursor());
        assertThrows(BufferClosedException.class, () -> buf.openReverseCursor(0, 0));
    }

    private static void verifyReadInaccessible(Buffer buf) {
        assertThrows(BufferClosedException.class, () -> buf.readBoolean());
        assertThrows(BufferClosedException.class, () -> buf.readByte());
        assertThrows(BufferClosedException.class, () -> buf.readUnsignedByte());
        assertThrows(BufferClosedException.class, () -> buf.readChar());
        assertThrows(BufferClosedException.class, () -> buf.readShort());
        assertThrows(BufferClosedException.class, () -> buf.readUnsignedShort());
        assertThrows(BufferClosedException.class, () -> buf.readMedium());
        assertThrows(BufferClosedException.class, () -> buf.readUnsignedMedium());
        assertThrows(BufferClosedException.class, () -> buf.readInt());
        assertThrows(BufferClosedException.class, () -> buf.readUnsignedInt());
        assertThrows(BufferClosedException.class, () -> buf.readFloat());
        assertThrows(BufferClosedException.class, () -> buf.readLong());
        assertThrows(BufferClosedException.class, () -> buf.readDouble());

        assertThrows(BufferClosedException.class, () -> buf.getBoolean(0));
        assertThrows(BufferClosedException.class, () -> buf.getByte(0));
        assertThrows(BufferClosedException.class, () -> buf.getUnsignedByte(0));
        assertThrows(BufferClosedException.class, () -> buf.getChar(0));
        assertThrows(BufferClosedException.class, () -> buf.getShort(0));
        assertThrows(BufferClosedException.class, () -> buf.getUnsignedShort(0));
        assertThrows(BufferClosedException.class, () -> buf.getMedium(0));
        assertThrows(BufferClosedException.class, () -> buf.getUnsignedMedium(0));
        assertThrows(BufferClosedException.class, () -> buf.getInt(0));
        assertThrows(BufferClosedException.class, () -> buf.getUnsignedInt(0));
        assertThrows(BufferClosedException.class, () -> buf.getFloat(0));
        assertThrows(BufferClosedException.class, () -> buf.getLong(0));
        assertThrows(BufferClosedException.class, () -> buf.getDouble(0));
    }

    private static void verifyWriteInaccessible(Buffer buf, Class<? extends RuntimeException> expected) {
        assertThrows(expected, () -> buf.writeByte((byte) 32));
        assertThrows(expected, () -> buf.writeUnsignedByte(32));
        assertThrows(expected, () -> buf.writeChar('3'));
        assertThrows(expected, () -> buf.writeShort((short) 32));
        assertThrows(expected, () -> buf.writeUnsignedShort(32));
        assertThrows(expected, () -> buf.writeMedium(32));
        assertThrows(expected, () -> buf.writeUnsignedMedium(32));
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
        assertThrows(expected, () -> buf.setMedium(0, 32));
        assertThrows(expected, () -> buf.setUnsignedMedium(0, 32));
        assertThrows(expected, () -> buf.setInt(0, 32));
        assertThrows(expected, () -> buf.setUnsignedInt(0, 32));
        assertThrows(expected, () -> buf.setFloat(0, 3.2f));
        assertThrows(expected, () -> buf.setLong(0, 32));
        assertThrows(expected, () -> buf.setDouble(0, 32));

        assertThrows(expected, () -> buf.ensureWritable(1));
        assertThrows(expected, () -> buf.fill((byte) 0));
        assertThrows(expected, () -> buf.compact());
        try (BufferAllocator allocator = BufferAllocator.onHeapUnpooled();
             Buffer source = allocator.allocate(8)) {
            assertThrows(expected, () -> source.copyInto(0, buf, 0, 1));
            if (expected == BufferClosedException.class) {
                assertThrows(expected, () -> buf.copyInto(0, source, 0, 1));
            }
        }
    }
}
