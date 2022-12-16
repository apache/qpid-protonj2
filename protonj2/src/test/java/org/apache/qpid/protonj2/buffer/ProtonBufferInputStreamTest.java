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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

public class ProtonBufferInputStreamTest {

    @Test
    public void testCannotReadFromClosedStream() throws IOException {
        byte[] payload = new byte[] { 0, 1, 2, 3, 4, 5 };

        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().copy(payload);
        ProtonBufferInputStream stream = new ProtonBufferInputStream(buffer);
        assertEquals(payload.length, stream.available());
        stream.close();

        assertThrows(IOException.class, () -> stream.read());
        assertThrows(IOException.class, () -> stream.readLine());
    }

    @Test
    public void testBufferWrappedExposesAvailableBytes() throws IOException {
        byte[] payload = new byte[] { 0, 1, 2, 3, 4, 5 };

        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().copy(payload);
        ProtonBufferInputStream stream = new ProtonBufferInputStream(buffer);
        assertEquals(payload.length, stream.available());

        stream.close();
    }

    @Test
    public void testReadFully() throws IOException {
        byte[] payload = new byte[] { 0, 1, 2, 3, 4, 5 };

        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().copy(payload);
        ProtonBufferInputStream stream = new ProtonBufferInputStream(buffer);
        assertEquals(payload.length, stream.available());

        final byte[] target = new byte[payload.length];
        stream.readFully(target);
        assertEquals(0, stream.available());

        assertArrayEquals(payload, target);

        assertThrows(IOException.class, () -> stream.readFully(target, 1, 1));
        assertThrows(NullPointerException.class, () -> stream.readFully(null));

        stream.close();
    }

    @Test
    public void testReadReturnsMinusOneAfterAllBytesRead() throws IOException {
        byte[] payload = new byte[] { 0, 1, 2, 3, 4, 5 };

        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().copy(payload);
        ProtonBufferInputStream stream = new ProtonBufferInputStream(buffer);
        assertEquals(payload.length, stream.available());

        for (int i = 0; i < payload.length; ++i) {
            assertEquals(stream.read(), payload[i]);
        }

        assertEquals(-1, stream.read());

        stream.close();
    }

    @Test
    public void testReadArrayReturnsMinusOneAfterAllBytesRead() throws IOException {
        byte[] payload = new byte[] { 0, 1, 2, 3, 4, 5 };

        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().copy(payload);
        ProtonBufferInputStream stream = new ProtonBufferInputStream(buffer);
        assertEquals(payload.length, stream.available());
        final byte[] target = new byte[payload.length];
        assertEquals(payload.length, stream.read(target));
        assertEquals(-1, stream.read(target));

        stream.close();
    }

    @Test
    public void testReadDataFromByteWrittenWithJavaStreams() throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);

        dos.writeInt(1024);
        dos.write(new byte[] { 0, 1, 2, 3 });
        dos.writeBoolean(false);
        dos.writeBoolean(true);
        dos.writeByte(255);
        dos.writeByte(254);
        dos.writeChar(65535);
        dos.writeShort(32765);
        dos.writeLong(Long.MAX_VALUE);
        dos.writeFloat(3.14f);
        dos.writeDouble(3.14);

        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().copy(bos.toByteArray());
        ProtonBufferInputStream stream = new ProtonBufferInputStream(buffer);

        final byte[] sink = new byte[4];

        assertEquals(1024, stream.readInt());
        stream.read(sink);
        assertArrayEquals(new byte[] { 0, 1, 2, 3 }, sink);
        assertEquals(false, stream.readBoolean());
        assertEquals(true, stream.readBoolean());
        assertEquals(255, stream.read());
        assertEquals(254, stream.readUnsignedByte());
        assertEquals(65535, stream.readChar());
        assertEquals(32765, stream.readShort());
        assertEquals(Long.MAX_VALUE, stream.readLong());
        assertEquals(3.14f, stream.readFloat(), 0.01f);
        assertEquals(3.14, stream.readDouble(), 0.01);

        stream.close();
    }

    @Test
    public void testReadUTF8StringFromDataOutputWrite() throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);

        dos.writeUTF("Hello World");

        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().copy(bos.toByteArray());
        ProtonBufferInputStream stream = new ProtonBufferInputStream(buffer);

        assertEquals("Hello World", stream.readUTF());

        stream.close();
    }

    @Test
    public void testMarkReadIndex() throws IOException {
        byte[] payload = new byte[] { 0, 1, 2, 3, 4, 5 };

        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().copy(payload);
        ProtonBufferInputStream stream = new ProtonBufferInputStream(buffer);
        assertTrue(stream.markSupported());
        assertEquals(payload.length, stream.available());
        assertEquals(payload[0], stream.readByte());

        stream.mark(100);

        assertEquals(payload[1], stream.readByte());

        stream.reset();

        assertEquals(payload[1], stream.readByte());
        assertEquals(payload[2], stream.readByte());

        stream.close();
    }

    @Test
    public void testGetBytesRead() throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);

        dos.writeInt(1024);
        dos.write(new byte[] { 0, 1, 2, 3 });
        dos.writeBoolean(false);
        dos.writeBoolean(true);
        dos.writeByte(255);
        dos.writeChar(65535);
        dos.writeLong(Long.MAX_VALUE);
        dos.writeFloat(3.14f);
        dos.writeDouble(3.14);

        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().copy(bos.toByteArray());
        ProtonBufferInputStream stream = new ProtonBufferInputStream(buffer);

        final byte[] sink = new byte[4];

        assertEquals(0, stream.getBytesRead());
        assertEquals(1024, stream.readInt());
        assertEquals(4, stream.getBytesRead());
        stream.read(sink);
        assertArrayEquals(new byte[] { 0, 1, 2, 3 }, sink);
        assertEquals(8, stream.getBytesRead());
        assertEquals(false, stream.readBoolean());
        assertEquals(9, stream.getBytesRead());
        assertEquals(true, stream.readBoolean());
        assertEquals(10, stream.getBytesRead());
        assertEquals(255, stream.read());
        assertEquals(11, stream.getBytesRead());
        assertEquals(65535, stream.readChar());
        assertEquals(13, stream.getBytesRead());
        assertEquals(Long.MAX_VALUE, stream.readLong());
        assertEquals(21, stream.getBytesRead());
        assertEquals(3.14f, stream.readFloat(), 0.01f);
        assertEquals(25, stream.getBytesRead());
        assertEquals(3.14, stream.readDouble(), 0.01);
        assertEquals(33, stream.getBytesRead());

        stream.close();
    }

    @Test
    public void testSkip() throws IOException {
        byte[] payload = new byte[] { 0, 1, 2, 3, 4, 5 };

        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().copy(payload);
        ProtonBufferInputStream stream = new ProtonBufferInputStream(buffer);
        assertEquals(payload.length, stream.available());
        assertEquals(payload.length, stream.skip(Integer.MAX_VALUE));
        assertEquals(0, stream.available());

        stream.close();
    }

    @Test
    public void testSkipLong() throws IOException {
        byte[] payload = new byte[] { 0, 1, 2, 3, 4, 5 };

        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().copy(payload);
        ProtonBufferInputStream stream = new ProtonBufferInputStream(buffer);
        assertEquals(payload.length, stream.available());
        assertEquals(payload.length, stream.skip(Long.MAX_VALUE));
        assertEquals(0, stream.available());

        stream.close();
    }

    @Test
    public void testReadLineWhenNoneAvailable() throws IOException {
        final String input = new String("Hello World\n");
        final byte[] payload = input.getBytes(StandardCharsets.UTF_8);

        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().copy(payload);
        ProtonBufferInputStream stream = new ProtonBufferInputStream(buffer);
        stream.skip(payload.length);
        assertEquals(0, stream.available());
        assertNull(stream.readLine());

        stream.close();
    }

    @Test
    public void testReadLines() throws IOException {
        final String input = new String("Hello World\nThis is a test\n");
        final String expected1 = new String("Hello World");
        final String expected2 = new String("This is a test");
        final byte[] payload = input.getBytes(StandardCharsets.UTF_8);

        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().copy(payload);
        ProtonBufferInputStream stream = new ProtonBufferInputStream(buffer);
        assertEquals(payload.length, stream.available());
        assertEquals(expected1, stream.readLine());
        assertEquals(expected2, stream.readLine());

        stream.close();
    }

    @Test
    public void testReadLine() throws IOException {
        final String input = new String("Hello World\n");
        final String expected = new String("Hello World");
        final byte[] payload = input.getBytes(StandardCharsets.UTF_8);

        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().copy(payload);
        ProtonBufferInputStream stream = new ProtonBufferInputStream(buffer);
        assertEquals(payload.length, stream.available());
        assertEquals(expected, stream.readLine());

        stream.close();
    }

    @Test
    public void testReadLineOnlyNewLine() throws IOException {
        final String input = new String("\n");
        final String expected = new String("");
        final byte[] payload = input.getBytes(StandardCharsets.UTF_8);

        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().copy(payload);
        ProtonBufferInputStream stream = new ProtonBufferInputStream(buffer);
        assertEquals(payload.length, stream.available());
        assertEquals(expected, stream.readLine());

        stream.close();
    }

    @Test
    public void testReadLineOnlyCRLF() throws IOException {
        final String input = new String("\r\n");
        final String expected = new String("");
        final byte[] payload = input.getBytes(StandardCharsets.UTF_8);

        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().copy(payload);
        ProtonBufferInputStream stream = new ProtonBufferInputStream(buffer);
        assertEquals(payload.length, stream.available());
        assertEquals(expected, stream.readLine());

        stream.close();
    }

    @Test
    public void testReadLineWhenNoNewLineAndNoMoreAvailable() throws IOException {
        final String input = new String("Hello World");
        final String expected = new String("Hello World");
        final byte[] payload = input.getBytes(StandardCharsets.UTF_8);

        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().copy(payload);
        ProtonBufferInputStream stream = new ProtonBufferInputStream(buffer);
        assertEquals(payload.length, stream.available());
        assertEquals(expected, stream.readLine());

        stream.close();
    }

    @Test
    public void testReadLineWhenCRIsInStringAlone() throws IOException {
        final String input = new String("Hello World\rABC\r");
        final String expected1 = new String("Hello World");
        final String expected2 = new String("ABC");
        final byte[] payload = input.getBytes(StandardCharsets.UTF_8);

        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().copy(payload);
        ProtonBufferInputStream stream = new ProtonBufferInputStream(buffer);
        assertEquals(payload.length, stream.available());
        assertEquals(expected1, stream.readLine());
        assertEquals(expected2, stream.readLine());

        stream.close();
    }

    @Test
    public void testReadLineCarriageReturnAndLineFeed() throws IOException {
        final String input = new String("Hello World\r\n");
        final String expected = new String("Hello World");
        final byte[] payload = input.getBytes(StandardCharsets.UTF_8);

        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().copy(payload);
        ProtonBufferInputStream stream = new ProtonBufferInputStream(buffer);
        assertEquals(payload.length, stream.available());
        assertEquals(expected, stream.readLine());

        stream.close();
    }

    @Test
    public void testReadLineCarriageReturnAndLineFeedThenEmptyLine() throws IOException {
        final String input = new String("Hello World\r\n\r\n");
        final String expected = new String("Hello World");
        final byte[] payload = input.getBytes(StandardCharsets.UTF_8);

        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().copy(payload);
        ProtonBufferInputStream stream = new ProtonBufferInputStream(buffer);
        assertEquals(payload.length, stream.available());
        assertEquals(expected, stream.readLine());
        assertEquals("", stream.readLine());

        stream.close();
    }
}
