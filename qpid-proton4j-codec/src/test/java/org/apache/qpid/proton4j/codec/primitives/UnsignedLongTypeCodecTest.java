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

import org.apache.qpid.proton4j.amqp.UnsignedLong;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.codec.CodecTestSupport;
import org.junit.Test;

public class UnsignedLongTypeCodecTest extends CodecTestSupport {

    @Test
    public void testEncodeDecodeUnsignedLong() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        encoder.writeUnsignedLong(buffer, encoderState, UnsignedLong.valueOf(640));

        Object result = decoder.readObject(buffer, decoderState);
        assertTrue(result instanceof UnsignedLong);
        assertEquals(640, ((UnsignedLong) result).intValue());
    }

    @Test
    public void testEncodeDecodePrimitive() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        encoder.writeUnsignedLong(buffer, encoderState, 640l);

        Object result = decoder.readObject(buffer, decoderState);
        assertTrue(result instanceof UnsignedLong);
        assertEquals(640, ((UnsignedLong) result).intValue());
    }

    @Test
    public void testDecodeSmallSeriesOfUnsignedLongs() throws IOException {
        doTestDecodeUnsignedLongSeries(SMALL_SIZE);
    }

    @Test
    public void testDecodeLargeSeriesOfUnsignedLongs() throws IOException {
        doTestDecodeUnsignedLongSeries(LARGE_SIZE);
    }

    private void doTestDecodeUnsignedLongSeries(int size) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        for (int i = 0; i < size; ++i) {
            encoder.writeUnsignedLong(buffer, encoderState, UnsignedLong.valueOf(i));
        }

        for (int i = 0; i < size; ++i) {
            final UnsignedLong result = decoder.readUnsignedLong(buffer, decoderState);

            assertNotNull(result);
            assertEquals(i, result.intValue());
        }
    }

    @Test
    public void testArrayOfUnsignedLongObjects() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        final int size = 10;

        UnsignedLong[] source = new UnsignedLong[size];
        for (int i = 0; i < size; ++i) {
            source[i] = UnsignedLong.valueOf(i);
        }

        encoder.writeArray(buffer, encoderState, source);

        Object result = decoder.readObject(buffer, decoderState);
        assertNotNull(result);
        assertTrue(result.getClass().isArray());
        assertFalse(result.getClass().getComponentType().isPrimitive());

        UnsignedLong[] array = (UnsignedLong[]) result;
        assertEquals(size, array.length);

        for (int i = 0; i < size; ++i) {
            assertEquals(source[i], array[i]);
        }
    }

    @Test
    public void testZeroSizedArrayOfUnsignedLongObjects() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        UnsignedLong[] source = new UnsignedLong[0];

        encoder.writeArray(buffer, encoderState, source);

        Object result = decoder.readObject(buffer, decoderState);
        assertNotNull(result);
        assertTrue(result.getClass().isArray());
        assertFalse(result.getClass().getComponentType().isPrimitive());

        UnsignedLong[] array = (UnsignedLong[]) result;
        assertEquals(source.length, array.length);
    }
}
