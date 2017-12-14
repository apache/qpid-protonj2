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
package org.apache.qpid.proton4j.codec;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.UUID;

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.messaging.Data;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.junit.Ignore;
import org.junit.Test;

public class UUIDTypeCodecTest extends CodecTestSupport {

    @Test
    public void testEncodeDecodeUUID() throws IOException {
        doTestEncodeDecodeUUIDSeries(1);
    }

    @Test
    public void testEncodeDecodeSmallSeriesOfUUIDs() throws IOException {
        doTestEncodeDecodeUUIDSeries(SMALL_SIZE);
    }

    @Test
    public void testEncodeDecodeLargeSeriesOfUUIDs() throws IOException {
        doTestEncodeDecodeUUIDSeries(LARGE_SIZE);
    }

    private void doTestEncodeDecodeUUIDSeries(int size) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Data data = new Data(new Binary(new byte[] { 1, 2, 3}));

        for (int i = 0; i < size; ++i) {
            encoder.writeObject(buffer, encoderState, data);
        }

        for (int i = 0; i < size; ++i) {
            final Object result = decoder.readObject(buffer, decoderState);

            assertNotNull(result);
            assertTrue(result instanceof Data);

            Data decoded = (Data) result;

            assertEquals(data.getValue(), decoded.getValue());
        }
    }

    @Test
    public void testDecodeSmallUUIDArray() throws IOException {
        doTestDecodeUUDIArrayType(SMALL_ARRAY_SIZE);
    }

    @Test
    public void testDecodeLargeUUDIArray() throws IOException {
        doTestDecodeUUDIArrayType(LARGE_ARRAY_SIZE);
    }

    private void doTestDecodeUUDIArrayType(int size) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        UUID[] source = new UUID[size];
        for (int i = 0; i < size; ++i) {
            source[i] = UUID.randomUUID();
        }

        encoder.writeArray(buffer, encoderState, source);

        Object result = decoder.readObject(buffer, decoderState);
        assertNotNull(result);
        assertTrue(result.getClass().isArray());

        UUID[] array = (UUID[]) result;
        assertEquals(size, array.length);

        for (int i = 0; i < size; ++i) {
            assertEquals(source[i], array[i]);
        }
    }

    @Test
    public void testWriteUUIDArrayWithMixedNullAndNotNullValues() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        UUID[] source = new UUID[2];
        source[0] = UUID.randomUUID();
        source[1] = null;

        try {
            encoder.writeArray(buffer, encoderState, source);
            fail("Should not be able to encode array with mixed null and non-null values");
        } catch (Exception e) {}

        source = new UUID[2];
        source[0] = null;
        source[1] = UUID.randomUUID();

        try {
            encoder.writeArray(buffer, encoderState, source);
            fail("Should not be able to encode array with mixed null and non-null values");
        } catch (Exception e) {}
    }

    @Test
    public void testWriteUUIDArrayWithZeroSize() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        UUID[] source = new UUID[0];
        encoder.writeArray(buffer, encoderState, source);

        Object result = decoder.readObject(buffer, decoderState);
        assertNotNull(result);
        assertTrue(result.getClass().isArray());

        UUID[] array = (UUID[]) result;
        assertEquals(0, array.length);
    }

    @Ignore("Can't currently handle generic arrays")
    @Test
    public void testObjectArrayContainingUUID() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Object[] source = new Object[10];
        for (int i = 0; i < 10; ++i) {
            source[i] = UUID.randomUUID();
        }

        encoder.writeArray(buffer, encoderState, source);

        Object result = decoder.readObject(buffer, decoderState);
        assertNotNull(result);
        assertTrue(result.getClass().isArray());

        UUID[] array = (UUID[]) result;
        assertEquals(10, array.length);

        for (int i = 0; i < 10; ++i) {
            assertEquals(source[i], array[i]);
        }
    }
}
