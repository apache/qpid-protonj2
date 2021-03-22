/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.qpid.protonj2.codec.transactions;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.io.InputStream;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferInputStream;
import org.apache.qpid.protonj2.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.protonj2.codec.CodecTestSupport;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.transactions.TransactionStateTypeDecoder;
import org.apache.qpid.protonj2.codec.encoders.transactions.TransactionStateTypeEncoder;
import org.apache.qpid.protonj2.types.Binary;
import org.apache.qpid.protonj2.types.messaging.Accepted;
import org.apache.qpid.protonj2.types.messaging.Rejected;
import org.apache.qpid.protonj2.types.messaging.Released;
import org.apache.qpid.protonj2.types.transactions.TransactionalState;
import org.junit.jupiter.api.Test;

/**
 * Test for handling Declared serialization
 */
public class TransactionStateTypeCodecTest extends CodecTestSupport {

    @Test
    public void testTypeClassReturnsCorrectType() throws IOException {
        assertEquals(TransactionalState.class, new TransactionStateTypeDecoder().getTypeClass());
        assertEquals(TransactionalState.class, new TransactionStateTypeEncoder().getTypeClass());
    }

    @Test
    public void testDescriptors() throws Exception {
        TransactionStateTypeDecoder decoder = new TransactionStateTypeDecoder();
        TransactionStateTypeEncoder encoder = new TransactionStateTypeEncoder();

        assertEquals(TransactionalState.DESCRIPTOR_CODE, decoder.getDescriptorCode());
        assertEquals(TransactionalState.DESCRIPTOR_CODE, encoder.getDescriptorCode());
        assertEquals(TransactionalState.DESCRIPTOR_SYMBOL, decoder.getDescriptorSymbol());
        assertEquals(TransactionalState.DESCRIPTOR_SYMBOL, encoder.getDescriptorSymbol());
    }

    @Test
    public void testEncodeDecodeType() throws Exception {
        doTestEncodeDecodeType(false);
    }

    @Test
    public void testEncodeDecodeTypeFromStream() throws Exception {
        doTestEncodeDecodeType(true);
    }

    private void doTestEncodeDecodeType(boolean fromStream) throws Exception {
        final ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        final InputStream stream = new ProtonBufferInputStream(buffer);

        TransactionalState input = new TransactionalState();
        input.setTxnId(new Binary(new byte[] { 2, 4, 6, 8 }));
        input.setOutcome(Accepted.getInstance());

        encoder.writeObject(buffer, encoderState, input);

        final TransactionalState result;
        if (fromStream) {
            result = (TransactionalState) streamDecoder.readObject(stream, streamDecoderState);
        } else {
            result = (TransactionalState) decoder.readObject(buffer, decoderState);
        }

        assertSame(result.getOutcome(), Accepted.getInstance());

        assertNotNull(result.getTxnId());
        assertNotNull(result.getTxnId().getArray());

        assertArrayEquals(new byte[] { 2, 4, 6, 8 }, result.getTxnId().getArray());
    }

    @Test
    public void testSkipValue() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        TransactionalState txnState = new TransactionalState();

        txnState.setTxnId(new Binary(new byte[] {0}));
        txnState.setOutcome(Accepted.getInstance());

        for (int i = 0; i < 10; ++i) {
            encoder.writeObject(buffer, encoderState, txnState);
        }

        txnState.setTxnId(new Binary(new byte[] {1, 2}));
        txnState.setOutcome(null);

        encoder.writeObject(buffer, encoderState, txnState);

        for (int i = 0; i < 10; ++i) {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(TransactionalState.class, typeDecoder.getTypeClass());
            typeDecoder.skipValue(buffer, decoderState);
        }

        final Object result = decoder.readObject(buffer, decoderState);

        assertNotNull(result);
        assertTrue(result instanceof TransactionalState);

        TransactionalState value = (TransactionalState) result;
        assertArrayEquals(new byte[] {1, 2}, value.getTxnId().getArray());
        assertNull(value.getOutcome());
    }

    @Test
    public void testSkipValueWithInvalidMap32Type() throws IOException {
        doTestSkipValueWithInvalidMapType(EncodingCodes.MAP32);
    }

    @Test
    public void testSkipValueWithInvalidMap8Type() throws IOException {
        doTestSkipValueWithInvalidMapType(EncodingCodes.MAP8);
    }

    private void doTestSkipValueWithInvalidMapType(byte mapType) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        buffer.writeByte((byte) 0); // Described Type Indicator
        buffer.writeByte(EncodingCodes.SMALLULONG);
        buffer.writeByte(TransactionalState.DESCRIPTOR_CODE.byteValue());
        if (mapType == EncodingCodes.MAP32) {
            buffer.writeByte(EncodingCodes.MAP32);
            buffer.writeInt((byte) 0);  // Size
            buffer.writeInt((byte) 0);  // Count
        } else {
            buffer.writeByte(EncodingCodes.MAP8);
            buffer.writeByte((byte) 0);  // Size
            buffer.writeByte((byte) 0);  // Count
        }

        TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
        assertEquals(TransactionalState.class, typeDecoder.getTypeClass());

        try {
            typeDecoder.skipValue(buffer, decoderState);
            fail("Should not be able to skip type with invalid encoding");
        } catch (DecodeException ex) {}
    }

    @Test
    public void testDecodedWithInvalidMap32Type() throws IOException {
        doTestDecodeWithInvalidMapType(EncodingCodes.MAP32);
    }

    @Test
    public void testDecodeWithInvalidMap8Type() throws IOException {
        doTestDecodeWithInvalidMapType(EncodingCodes.MAP8);
    }

    private void doTestDecodeWithInvalidMapType(byte mapType) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        buffer.writeByte((byte) 0); // Described Type Indicator
        buffer.writeByte(EncodingCodes.SMALLULONG);
        buffer.writeByte(TransactionalState.DESCRIPTOR_CODE.byteValue());
        if (mapType == EncodingCodes.MAP32) {
            buffer.writeByte(EncodingCodes.MAP32);
            buffer.writeInt((byte) 0);  // Size
            buffer.writeInt((byte) 0);  // Count
        } else {
            buffer.writeByte(EncodingCodes.MAP8);
            buffer.writeByte((byte) 0);  // Size
            buffer.writeByte((byte) 0);  // Count
        }

        try {
            decoder.readObject(buffer, decoderState);
            fail("Should not decode type with invalid encoding");
        } catch (DecodeException ex) {}
    }

    @Test
    public void testEncodeDecodeArray() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        TransactionalState[] array = new TransactionalState[3];

        array[0] = new TransactionalState();
        array[1] = new TransactionalState();
        array[2] = new TransactionalState();

        array[0].setTxnId(new Binary(new byte[] {0})).setOutcome(Accepted.getInstance());
        array[1].setTxnId(new Binary(new byte[] {1})).setOutcome(Released.getInstance());
        array[2].setTxnId(new Binary(new byte[] {2})).setOutcome(new Rejected());

        encoder.writeObject(buffer, encoderState, array);

        final Object result = decoder.readObject(buffer, decoderState);

        assertTrue(result.getClass().isArray());
        assertEquals(TransactionalState.class, result.getClass().getComponentType());

        TransactionalState[] resultArray = (TransactionalState[]) result;

        for (int i = 0; i < resultArray.length; ++i) {
            assertNotNull(resultArray[i]);
            assertTrue(resultArray[i] instanceof TransactionalState);
            assertEquals(array[i].getTxnId(), resultArray[i].getTxnId());
            assertEquals(array[i].getOutcome().getClass(), resultArray[i].getOutcome().getClass());
        }
    }
}
