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
package org.apache.qpid.protonj2.codec.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.UUID;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.protonj2.codec.CodecTestSupport;
import org.apache.qpid.protonj2.codec.TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.messaging.AmqpValueTypeDecoder;
import org.apache.qpid.protonj2.codec.encoders.messaging.AmqpValueTypeEncoder;
import org.apache.qpid.protonj2.types.messaging.AmqpValue;
import org.apache.qpid.protonj2.types.messaging.Modified;
import org.junit.jupiter.api.Test;

/**
 * Test for decoder of the AmqpValue type.
 */
public class AmqpValueTypeCodecTest extends CodecTestSupport {

    @Test
    public void testTypeClassReturnsCorrectType() throws IOException {
        assertEquals(AmqpValue.class, new AmqpValueTypeDecoder().getTypeClass());
        assertEquals(AmqpValue.class, new AmqpValueTypeEncoder().getTypeClass());
    }

    @Test
    public void testDescriptors() throws IOException {
        assertEquals(AmqpValue.DESCRIPTOR_CODE, new AmqpValueTypeDecoder().getDescriptorCode());
        assertEquals(AmqpValue.DESCRIPTOR_CODE, new AmqpValueTypeEncoder().getDescriptorCode());
        assertEquals(AmqpValue.DESCRIPTOR_SYMBOL, new AmqpValueTypeDecoder().getDescriptorSymbol());
        assertEquals(AmqpValue.DESCRIPTOR_SYMBOL, new AmqpValueTypeEncoder().getDescriptorSymbol());
    }

    @Test
    public void testDecodeAmqpValueString() throws IOException {
        doTestDecodeAmqpValueSeries(1, new AmqpValue<>("test"));
    }

    @Test
    public void testDecodeAmqpValueNull() throws IOException {
        doTestDecodeAmqpValueSeries(1, new AmqpValue<>(null));
    }

    @Test
    public void testDecodeAmqpValueUUID() throws IOException {
        doTestDecodeAmqpValueSeries(1, new AmqpValue<>(UUID.randomUUID()));
    }

    @Test
    public void testDecodeSmallSeriesOfAmqpValue() throws IOException {
        doTestDecodeAmqpValueSeries(SMALL_SIZE, new AmqpValue<>("test"));
    }

    @Test
    public void testDecodeLargeSeriesOfAmqpValue() throws IOException {
        doTestDecodeAmqpValueSeries(LARGE_SIZE, new AmqpValue<>("test"));
    }

    private void doTestDecodeAmqpValueSeries(int size, AmqpValue<Object> value) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        for (int i = 0; i < size; ++i) {
            encoder.writeObject(buffer, encoderState, value);
        }

        for (int i = 0; i < size; ++i) {
            final Object result = decoder.readObject(buffer, decoderState);

            assertNotNull(result);
            assertTrue(result instanceof AmqpValue);

            @SuppressWarnings("unchecked")
            AmqpValue<Object> decoded = (AmqpValue<Object>) result;

            assertEquals(value.getValue(), decoded.getValue());
        }
    }

    @Test
    public void testDecodeAmqpValueWithEmptyValue() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        encoder.writeObject(buffer, encoderState, new AmqpValue<>(null));

        final Object result = decoder.readObject(buffer, decoderState);

        assertNotNull(result);
        assertTrue(result instanceof AmqpValue);

        AmqpValue<?> decoded = (AmqpValue<?>) result;

        assertNull(decoded.getValue());
    }

    @Test
    public void testEncodeDecodeArrayOfAmqpValue() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        @SuppressWarnings("unchecked")
        AmqpValue<Object>[] array = new AmqpValue[3];

        array[0] = new AmqpValue<>("1");
        array[1] = new AmqpValue<>("2");
        array[2] = new AmqpValue<>("3");

        encoder.writeObject(buffer, encoderState, array);

        final Object result = decoder.readObject(buffer, decoderState);

        assertTrue(result.getClass().isArray());
        assertEquals(AmqpValue.class, result.getClass().getComponentType());

        @SuppressWarnings("unchecked")
        AmqpValue<String>[] resultArray = (AmqpValue[]) result;

        for (int i = 0; i < resultArray.length; ++i) {
            assertNotNull(resultArray[i]);
            assertTrue(resultArray[i] instanceof AmqpValue);
            assertEquals(array[i].getValue(), resultArray[i].getValue());
        }
    }

    @Test
    public void testSkipValue() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        for (int i = 0; i < 10; ++i) {
            encoder.writeObject(buffer, encoderState, new AmqpValue<>("skipMe"));
        }

        encoder.writeObject(buffer, encoderState, new Modified());

        for (int i = 0; i < 10; ++i) {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(AmqpValue.class, typeDecoder.getTypeClass());
            typeDecoder.skipValue(buffer, decoderState);
        }

        final Object result = decoder.readObject(buffer, decoderState);

        assertNotNull(result);
        assertTrue(result instanceof Modified);
        Modified modified = (Modified) result;
        assertFalse(modified.isUndeliverableHere());
        assertFalse(modified.isDeliveryFailed());
    }

    @SuppressWarnings("rawtypes")
    @Test
    public void testEncodeDecodeArray() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        AmqpValue[] array = new AmqpValue[3];

        array[0] = new AmqpValue<>("1");
        array[1] = new AmqpValue<>("2");
        array[2] = new AmqpValue<>("3");

        encoder.writeObject(buffer, encoderState, array);

        final Object result = decoder.readObject(buffer, decoderState);

        assertTrue(result.getClass().isArray());
        assertEquals(AmqpValue.class, result.getClass().getComponentType());

        AmqpValue[] resultArray = (AmqpValue[]) result;

        for (int i = 0; i < resultArray.length; ++i) {
            assertNotNull(resultArray[i]);
            assertTrue(resultArray[i] instanceof AmqpValue);
            assertEquals(array[i].getValue(), resultArray[i].getValue());
        }
    }
}
