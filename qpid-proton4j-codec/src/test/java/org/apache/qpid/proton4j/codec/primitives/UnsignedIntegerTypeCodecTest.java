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
package org.apache.qpid.proton4j.codec.primitives;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.qpid.proton4j.amqp.UnsignedInteger;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.codec.CodecTestSupport;
import org.junit.Test;

public class UnsignedIntegerTypeCodecTest extends CodecTestSupport {

    @Test
    public void testEncodeDecodeUnsignedInteger() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        encoder.writeUnsignedInteger(buffer, encoderState, UnsignedInteger.valueOf(640));

        Object result = decoder.readObject(buffer, decoderState);
        assertTrue(result instanceof UnsignedInteger);
        assertEquals(640, ((UnsignedInteger) result).intValue());
    }

    @Test
    public void testEncodeDecodeInteger() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        encoder.writeUnsignedInteger(buffer, encoderState, 640);

        Object result = decoder.readObject(buffer, decoderState);
        assertTrue(result instanceof UnsignedInteger);
        assertEquals(640, ((UnsignedInteger) result).intValue());
    }

    @Test
    public void testDecodeSmallSeriesOfUnsignedIntegers() throws IOException {
        doTestDecodeUnsignedIntegerSeries(SMALL_SIZE);
    }

    @Test
    public void testDecodeLargeSeriesOfUnsignedIntegers() throws IOException {
        doTestDecodeUnsignedIntegerSeries(LARGE_SIZE);
    }

    private void doTestDecodeUnsignedIntegerSeries(int size) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        for (int i = 0; i < size; ++i) {
            encoder.writeUnsignedInteger(buffer, encoderState, i);
        }

        for (int i = 0; i < size; ++i) {
            final UnsignedInteger result = decoder.readUnsignedInteger(buffer, decoderState);

            assertNotNull(result);
            assertEquals(i, result.intValue());
        }
    }

    @Test
    public void testArrayOfUnsignedIntegerObjects() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        final int size = 10;

        UnsignedInteger[] source = new UnsignedInteger[size];
        for (int i = 0; i < size; ++i) {
            source[i] = UnsignedInteger.valueOf(i);
        }

        encoder.writeArray(buffer, encoderState, source);

        Object result = decoder.readObject(buffer, decoderState);
        assertNotNull(result);
        assertTrue(result.getClass().isArray());
        assertFalse(result.getClass().getComponentType().isPrimitive());

        UnsignedInteger[] array = (UnsignedInteger[]) result;
        assertEquals(size, array.length);

        for (int i = 0; i < size; ++i) {
            assertEquals(source[i], array[i]);
        }
    }

    @Test
    public void testZeroSizedArrayOfUnsignedIntegerObjects() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        UnsignedInteger[] source = new UnsignedInteger[0];

        encoder.writeArray(buffer, encoderState, source);

        Object result = decoder.readObject(buffer, decoderState);
        assertNotNull(result);
        assertTrue(result.getClass().isArray());
        assertFalse(result.getClass().getComponentType().isPrimitive());

        UnsignedInteger[] array = (UnsignedInteger[]) result;
        assertEquals(source.length, array.length);
    }
}
