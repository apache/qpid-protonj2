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
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

public class ProtonBufferOutputStreamTest {

    @Test
    public void testBufferWrappedExposesWrittenBytes() throws IOException {
        byte[] payload = new byte[] { 0, 1, 2, 3, 4, 5 };

        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate(payload.length);
        ProtonBufferOutputStream stream = new ProtonBufferOutputStream(buffer);
        assertEquals(0, stream.getBytesWritten());

        stream.write(payload);

        assertEquals(payload.length, stream.getBytesWritten());

        stream.close();
    }

    @Test
    public void testBufferWritesGivenArrayBytes() throws IOException {
        byte[] payload = new byte[] { 0, 1, 2, 3, 4, 5 };

        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate(payload.length);
        ProtonBufferOutputStream stream = new ProtonBufferOutputStream(buffer);
        assertEquals(0, stream.getBytesWritten());

        stream.write(payload);

        for (int i = 0; i < payload.length; ++i) {
            assertEquals(payload[i], buffer.getByte(i));
        }

        stream.close();
    }

    @Test
    public void testZeroLengthWriteBytesDoesNotWriteOrThrow() throws IOException {
        byte[] payload = new byte[] { 0, 1, 2, 3, 4, 5 };

        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate(payload.length);
        ProtonBufferOutputStream stream = new ProtonBufferOutputStream(buffer);

        stream.write(payload, 0, 0);

        assertEquals(0, stream.getBytesWritten());

        stream.close();
    }

    @Test
    public void testCannotWriteToClosedStream() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        ProtonBufferOutputStream stream = new ProtonBufferOutputStream(buffer);

        stream.close();
        assertThrows(IOException.class, () -> stream.writeInt(1024));
    }

    @Test
    public void testWriteValuesAndReadWithDataInputStream() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        ProtonBufferOutputStream stream = new ProtonBufferOutputStream(buffer);

        stream.write(32);
        stream.writeInt(1024);
        stream.write(new byte[] { 0, 1, 2, 3 });
        stream.writeBoolean(false);
        stream.writeBoolean(true);
        stream.writeByte(255);
        stream.writeShort(32767);
        stream.writeLong(Long.MAX_VALUE);
        stream.writeChar(65535);
        stream.writeFloat(3.14f);
        stream.writeDouble(3.14);

        final byte[] array = buffer.toByteBuffer().array();
        ByteArrayInputStream bis = new ByteArrayInputStream(array, 0, buffer.getReadableBytes());
        DataInputStream dis = new DataInputStream(bis);

        final byte[] sink = new byte[4];

        assertEquals(32, dis.read());
        assertEquals(1024, dis.readInt());
        dis.read(sink);
        assertArrayEquals(new byte[] { 0, 1, 2, 3 }, sink);
        assertEquals(false, dis.readBoolean());
        assertEquals(true, dis.readBoolean());
        assertEquals(255, dis.read());
        assertEquals(32767, dis.readShort());
        assertEquals(Long.MAX_VALUE, dis.readLong());
        assertEquals(65535, dis.readChar());
        assertEquals(3.14f, dis.readFloat(), 0.01f);
        assertEquals(3.14, dis.readDouble(), 0.01);

        stream.close();
    }

    @Test
    public void testWriteChars() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        ProtonBufferOutputStream stream = new ProtonBufferOutputStream(buffer);
        String expected = "Hello World";

        stream.writeChars(expected);

        final byte[] array = buffer.toByteBuffer().array();
        ByteArrayInputStream bis = new ByteArrayInputStream(array, 0, buffer.getReadableBytes());
        DataInputStream dis = new DataInputStream(bis);

        for (char letter : expected.toCharArray()) {
            assertEquals(letter, dis.readChar());
        }

        stream.close();
    }

    @Test
    public void testWriteUtf8StringAndReadWithDataInputStream() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        ProtonBufferOutputStream stream = new ProtonBufferOutputStream(buffer);

        stream.writeUTF("Hello World");
        stream.writeUTF("Hello World Again");

        final byte[] array = buffer.toByteBuffer().array();
        ByteArrayInputStream bis = new ByteArrayInputStream(array, 0, buffer.getReadableBytes());
        DataInputStream dis = new DataInputStream(bis);

        assertEquals("Hello World", dis.readUTF());
        assertEquals("Hello World Again", dis.readUTF());

        stream.close();
    }

    @Test
    public void testWriteBytesFromString() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        ProtonBufferOutputStream stream = new ProtonBufferOutputStream(buffer);

        stream.writeBytes("Hello World");

        final byte[] array = buffer.toByteBuffer().array();
        ByteArrayInputStream bis = new ByteArrayInputStream(array, 0, buffer.getReadableBytes());
        DataInputStream dis = new DataInputStream(bis);

        byte[] result = dis.readAllBytes();

        assertArrayEquals(result, "Hello World".getBytes(StandardCharsets.US_ASCII));

        stream.close();
    }
}
