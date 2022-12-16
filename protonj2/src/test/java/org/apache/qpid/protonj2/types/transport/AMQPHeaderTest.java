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
package org.apache.qpid.protonj2.types.transport;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferAllocator;
import org.apache.qpid.protonj2.types.transport.AMQPHeader.HeaderHandler;
import org.junit.jupiter.api.Test;

public class AMQPHeaderTest {

    @Test
    public void testDefaultCreate() {
        AMQPHeader header = new AMQPHeader();

        assertEquals(AMQPHeader.getAMQPHeader(), header);
        assertFalse(header.isSaslHeader());
        assertEquals(0, header.getProtocolId());
        assertEquals(1, header.getMajor());
        assertEquals(0, header.getMinor());
        assertEquals(0, header.getRevision());
        assertTrue(header.hasValidPrefix());
    }

    @Test
    public void testToArray() {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().copy(new byte[] {'A', 'M', 'Q', 'P', 0, 1, 0, 0});
        AMQPHeader header = new AMQPHeader(buffer);
        byte[] array = header.toArray();

        assertEquals(buffer, ProtonBufferAllocator.defaultAllocator().copy(array));
    }

    @Test
    public void testToByteBuffer() {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().copy(new byte[] {'A', 'M', 'Q', 'P', 0, 1, 0, 0});
        AMQPHeader header = new AMQPHeader(buffer);
        ByteBuffer byteBuffer = header.toByteBuffer();
        ByteBuffer bufferView = ByteBuffer.allocate(buffer.getReadableBytes());
        buffer.readBytes(bufferView);

        assertArrayEquals(bufferView.array(), byteBuffer.array());
    }

    @Test
    public void testCreateFromBufferWithoutValidation() {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().copy(new byte[] {'A', 'M', 'Q', 'P', 4, 1, 0, 0});
        AMQPHeader invalid = new AMQPHeader(buffer, false);

        assertEquals(4, invalid.getByteAt(4));
        assertEquals(4, invalid.getProtocolId());
    }

    @Test
    public void testCreateFromBufferWithoutValidationDoesNotFailWithToLargeInput() {
        ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().copy(new byte[] {'A', 'M', 'Q', 'P', 4, 1, 0, 0, 0});
        assertDoesNotThrow(() -> new AMQPHeader(buffer, false));
    }

    @Test
    public void testGetBuffer() {
        AMQPHeader header = new AMQPHeader();

        assertNotNull(header.getBuffer());
        ProtonBuffer buffer = header.getBuffer();
        assertTrue(buffer.isReadOnly());
        buffer = header.getBuffer().copy();

        buffer.setByte(0, (byte) 'B');

        assertEquals('A', header.getByteAt(0));
    }

    @Test
    public void testHashCode() {
        AMQPHeader defaultCtor = new AMQPHeader();
        AMQPHeader byteCtor = new AMQPHeader(new byte[] {'A', 'M', 'Q', 'P', 0, 1, 0, 0});
        AMQPHeader byteCtorSasl = new AMQPHeader(new byte[] {'A', 'M', 'Q', 'P', 3, 1, 0, 0});

        assertEquals(defaultCtor.hashCode(), byteCtor.hashCode());
        assertEquals(defaultCtor.hashCode(), AMQPHeader.getAMQPHeader().hashCode());
        assertEquals(byteCtor.hashCode(), AMQPHeader.getAMQPHeader().hashCode());
        assertEquals(byteCtorSasl.hashCode(), AMQPHeader.getSASLHeader().hashCode());
        assertNotEquals(byteCtor.hashCode(), AMQPHeader.getSASLHeader().hashCode());
        assertNotEquals(defaultCtor.hashCode(), AMQPHeader.getSASLHeader().hashCode());
        assertEquals(byteCtorSasl.hashCode(), AMQPHeader.getSASLHeader().hashCode());
    }

    @Test
    public void testIsTypeMethods() {
        AMQPHeader defaultCtor = new AMQPHeader();
        AMQPHeader byteCtor = new AMQPHeader(new byte[] {'A', 'M', 'Q', 'P', 0, 1, 0, 0});
        AMQPHeader byteCtorSasl = new AMQPHeader(new byte[] {'A', 'M', 'Q', 'P', 3, 1, 0, 0});

        assertFalse(defaultCtor.isSaslHeader());
        assertFalse(byteCtor.isSaslHeader());
        assertTrue(byteCtorSasl.isSaslHeader());
        assertFalse(AMQPHeader.getAMQPHeader().isSaslHeader());
        assertTrue(AMQPHeader.getSASLHeader().isSaslHeader());
    }

    @SuppressWarnings("unlikely-arg-type")
    @Test
    public void testEquals() {
        AMQPHeader defaultCtor = new AMQPHeader();
        AMQPHeader byteCtor = new AMQPHeader(new byte[] {'A', 'M', 'Q', 'P', 0, 1, 0, 0});
        AMQPHeader byteCtorSasl = new AMQPHeader(new byte[] {'A', 'M', 'Q', 'P', 3, 1, 0, 0});

        assertEquals(defaultCtor, defaultCtor);
        assertEquals(defaultCtor, byteCtor);
        assertEquals(byteCtor, byteCtor);
        assertEquals(defaultCtor, AMQPHeader.getAMQPHeader());
        assertEquals(byteCtor, AMQPHeader.getAMQPHeader());
        assertEquals(byteCtorSasl, AMQPHeader.getSASLHeader());
        assertNotEquals(byteCtor, AMQPHeader.getSASLHeader());
        assertNotEquals(defaultCtor, AMQPHeader.getSASLHeader());
        assertEquals(byteCtorSasl, AMQPHeader.getSASLHeader());

        assertFalse(AMQPHeader.getSASLHeader().equals(null));
        assertFalse(AMQPHeader.getSASLHeader().equals(Boolean.TRUE));
    }

    @Test
    public void testToStringOnDefault() {
        AMQPHeader header = new AMQPHeader();
        assertTrue(header.toString().startsWith("AMQP"));
    }

    @Test
    public void testValidateByteWithValidHeaderBytes() {
        ProtonBuffer buffer = AMQPHeader.getAMQPHeader().getBuffer();

        for (int i = 0; i < AMQPHeader.HEADER_SIZE_BYTES; ++i) {
            AMQPHeader.validateByte(i, buffer.getByte(i));
        }
    }

    @Test
    public void testValidateByteWithInvalidHeaderBytes() {
        byte[] bytes = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 };

        for (int i = 0; i < AMQPHeader.HEADER_SIZE_BYTES; ++i) {
            try {
                AMQPHeader.validateByte(i, bytes[i]);
                fail("Should throw IllegalArgumentException as bytes are invalid");
            } catch (IllegalArgumentException iae) {
                // Expected
            }
        }
    }

    @Test
    public void testCreateWithNullBuffer() {
        assertThrows(NullPointerException.class, () -> new AMQPHeader((ProtonBuffer) null));
    }

    @Test
    public void testCreateWithNullByte() {
        assertThrows(NullPointerException.class, () -> new AMQPHeader((byte[]) null));
    }

    @Test
    public void testCreateWithEmptyBuffer() {
        assertThrows(IllegalArgumentException.class, () -> new AMQPHeader(ProtonBufferAllocator.defaultAllocator().allocate()));
    }

    @Test
    public void testCreateWithOversizedBuffer() {
        assertThrows(IllegalArgumentException.class, () -> new AMQPHeader(new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13}));
    }

    @Test
    public void testCreateWithInvalidHeaderPrefix() {
        assertThrows(IllegalArgumentException.class, () -> new AMQPHeader(new byte[] {'A', 'M', 'Q', 0, 0, 1, 0, 0}));
    }

    @Test
    public void testCreateWithInvalidHeaderProtocol() {
        assertThrows(IllegalArgumentException.class, () -> new AMQPHeader(new byte[] {'A', 'M', 'Q', 'P', 4, 1, 0, 0}));
    }

    @Test
    public void testCreateWithInvalidHeaderMajor() {
        assertThrows(IllegalArgumentException.class, () -> new AMQPHeader(new byte[] {'A', 'M', 'Q', 'P', 0, 2, 0, 0}));
    }

    @Test
    public void testCreateWithInvalidHeaderMinor() {
        assertThrows(IllegalArgumentException.class, () -> new AMQPHeader(new byte[] {'A', 'M', 'Q', 'P', 0, 1, 1, 0}));
    }

    @Test
    public void testCreateWithInvalidHeaderRevision() {
        assertThrows(IllegalArgumentException.class, () -> new AMQPHeader(new byte[] {'A', 'M', 'Q', 'P', 0, 1, 0, 1}));
    }

    @Test
    public void testValidateHeaderByte0WithInvalidValue() {
        assertThrows(IllegalArgumentException.class, () -> AMQPHeader.validateByte(0, (byte) 85));
    }

    @Test
    public void testValidateHeaderByte1WithInvalidValue() {
        assertThrows(IllegalArgumentException.class, () -> AMQPHeader.validateByte(1, (byte) 85));
    }

    @Test
    public void testValidateHeaderByte2WithInvalidValue() {
        assertThrows(IllegalArgumentException.class, () -> AMQPHeader.validateByte(2, (byte) 85));
    }

    @Test
    public void testValidateHeaderByte3WithInvalidValue() {
        assertThrows(IllegalArgumentException.class, () -> AMQPHeader.validateByte(3, (byte) 85));
    }

    @Test
    public void testValidateHeaderByte4WithInvalidValue() {
        assertThrows(IllegalArgumentException.class, () -> AMQPHeader.validateByte(4, (byte) 85));
    }

    @Test
    public void testValidateHeaderByte5WithInvalidValue() {
        assertThrows(IllegalArgumentException.class, () -> AMQPHeader.validateByte(5, (byte) 85));
    }

    @Test
    public void testValidateHeaderByte6WithInvalidValue() {
        assertThrows(IllegalArgumentException.class, () -> AMQPHeader.validateByte(6, (byte) 85));
    }

    @Test
    public void testValidateHeaderByte7WithInvalidValue() {
        assertThrows(IllegalArgumentException.class, () -> AMQPHeader.validateByte(7, (byte) 85));
    }

    @Test
    public void testValidateHeaderByteIndexOutOfBounds() {
        assertThrows(IndexOutOfBoundsException.class, () -> AMQPHeader.validateByte(9, (byte) 65));
    }

    @Test
    public void testInvokeOnAMQPHeader() {
        final AtomicBoolean amqpHeader = new AtomicBoolean();
        final AtomicBoolean saslHeader = new AtomicBoolean();
        final AtomicReference<String> captured = new AtomicReference<>();

        AMQPHeader.getAMQPHeader().invoke(new HeaderHandler<String>() {

            @Override
            public void handleAMQPHeader(AMQPHeader header, String context) {
                amqpHeader.set(true);
                captured.set(context);
            }

            @Override
            public void handleSASLHeader(AMQPHeader header, String context) {
                saslHeader.set(true);
                captured.set(context);
            }
        }, "test");

        assertTrue(amqpHeader.get());
        assertFalse(saslHeader.get());
        assertEquals("test", captured.get());
    }

    @Test
    public void testInvokeOnSASLHeader() {
        final AtomicBoolean amqpHeader = new AtomicBoolean();
        final AtomicBoolean saslHeader = new AtomicBoolean();
        final AtomicReference<String> captured = new AtomicReference<>();

        AMQPHeader.getSASLHeader().invoke(new HeaderHandler<String>() {

            @Override
            public void handleAMQPHeader(AMQPHeader header, String context) {
                amqpHeader.set(true);
                captured.set(context);
            }

            @Override
            public void handleSASLHeader(AMQPHeader header, String context) {
                saslHeader.set(true);
                captured.set(context);
            }
        }, "test");

        assertTrue(saslHeader.get());
        assertFalse(amqpHeader.get());
        assertEquals("test", captured.get());
    }
}
