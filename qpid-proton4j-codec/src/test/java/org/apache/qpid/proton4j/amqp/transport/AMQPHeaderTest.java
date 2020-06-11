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
package org.apache.qpid.proton4j.amqp.transport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.types.transport.AMQPHeader;
import org.apache.qpid.proton4j.types.transport.AMQPHeader.HeaderHandler;
import org.junit.Test;

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
    public void testCreateFromBufferWithoutValidation() {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.wrap(new byte[] {'A', 'M', 'Q', 'P', 4, 1, 0, 0});
        AMQPHeader invalid = new AMQPHeader(buffer, false);

        assertEquals(4, invalid.getByteAt(4));
        assertEquals(4, invalid.getProtocolId());
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testCreateFromBufferWithoutValidationFailsWithToLargeInput() {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.wrap(new byte[] {'A', 'M', 'Q', 'P', 4, 1, 0, 0, 0});
        new AMQPHeader(buffer, false);
    }

    @Test
    public void testGetBuffer() {
        AMQPHeader header = new AMQPHeader();

        assertNotNull(header.getBuffer());
        ProtonBuffer buffer = header.getBuffer();

        buffer.setByte(0, 'B');

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

        byte[] bytes = buffer.getArray();

        for (int i = 0; i < AMQPHeader.HEADER_SIZE_BYTES; ++i) {
            AMQPHeader.validateByte(i, bytes[i]);
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

    @Test(expected = NullPointerException.class)
    public void testCreateWithNullBuffer() {
        new AMQPHeader((ProtonBuffer) null);
    }

    @Test(expected = NullPointerException.class)
    public void testCreateWithNullByte() {
        new AMQPHeader((byte[]) null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateWithEmptyBuffer() {
        new AMQPHeader(ProtonByteBufferAllocator.DEFAULT.allocate());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateWithOversizedBuffer() {
        new AMQPHeader(new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13});
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateWithInvalidHeaderPrefix() {
        new AMQPHeader(new byte[] {'A', 'M', 'Q', 0, 0, 1, 0, 0});
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateWithInvalidHeaderProtocol() {
        new AMQPHeader(new byte[] {'A', 'M', 'Q', 'P', 4, 1, 0, 0});
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateWithInvalidHeaderMajor() {
        new AMQPHeader(new byte[] {'A', 'M', 'Q', 'P', 0, 2, 0, 0});
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateWithInvalidHeaderMinor() {
        new AMQPHeader(new byte[] {'A', 'M', 'Q', 'P', 0, 1, 1, 0});
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateWithInvalidHeaderRevision() {
        new AMQPHeader(new byte[] {'A', 'M', 'Q', 'P', 0, 1, 0, 1});
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateHeaderByte0WithInvalidValue() {
        AMQPHeader.validateByte(0, (byte) 85);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateHeaderByte1WithInvalidValue() {
        AMQPHeader.validateByte(0, (byte) 85);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateHeaderByte2WithInvalidValue() {
        AMQPHeader.validateByte(0, (byte) 85);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateHeaderByte3WithInvalidValue() {
        AMQPHeader.validateByte(0, (byte) 85);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateHeaderByte4WithInvalidValue() {
        AMQPHeader.validateByte(0, (byte) 85);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateHeaderByte5WithInvalidValue() {
        AMQPHeader.validateByte(0, (byte) 85);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateHeaderByte6WithInvalidValue() {
        AMQPHeader.validateByte(0, (byte) 85);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateHeaderByte7WithInvalidValue() {
        AMQPHeader.validateByte(0, (byte) 85);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testValidateHeaderByteIndexOutOfBounds() {
        AMQPHeader.validateByte(9, (byte) 65);
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
