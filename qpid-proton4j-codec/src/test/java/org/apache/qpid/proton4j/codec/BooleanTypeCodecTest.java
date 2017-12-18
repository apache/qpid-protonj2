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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Test the BooleanTypeDecoder for correctness
 */
public class BooleanTypeCodecTest extends CodecTestSupport {

    @Test
    public void testDecodeBooleanTrue() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        encoder.writeBoolean(buffer, encoderState, true);

        Object result = decoder.readObject(buffer, decoderState);
        assertTrue(result instanceof Boolean);
        assertTrue(((Boolean) result).booleanValue());
    }

    @Test
    public void testDecodeBooleanFalse() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        encoder.writeBoolean(buffer, encoderState, false);

        Object result = decoder.readObject(buffer, decoderState);
        assertTrue(result instanceof Boolean);
        assertFalse(((Boolean) result).booleanValue());
    }

    @Test
    public void testDecodeSmallSeriesOfBooleans() throws IOException {
        doTestDecodeBooleanSeries(SMALL_SIZE);
    }

    @Test
    public void testDecodeLargeSeriesOfBooleans() throws IOException {
        doTestDecodeBooleanSeries(LARGE_SIZE);
    }

    private void doTestDecodeBooleanSeries(int size) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        for (int i = 0; i < size; ++i) {
            encoder.writeBoolean(buffer, encoderState, i % 2 == 0);
        }

        for (int i = 0; i < size; ++i) {
            final Object result = decoder.readObject(buffer, decoderState);

            assertNotNull(result);
            assertTrue(result instanceof Boolean);

            Boolean boolValue = (Boolean) result;
            assertEquals(i % 2 == 0, boolValue.booleanValue());
        }
    }

    @Test
    public void testArrayOfBooleanObjects() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        final int size = 10;

        Boolean[] source = new Boolean[size];
        for (int i = 0; i < size; ++i) {
            source[i] = i % 2 == 0;
        }

        encoder.writeArray(buffer, encoderState, source);

        Object result = decoder.readObject(buffer, decoderState);
        assertNotNull(result);
        assertTrue(result.getClass().isArray());
        assertTrue(result.getClass().getComponentType().isPrimitive());

        boolean[] array = (boolean[]) result;
        assertEquals(size, array.length);

        for (int i = 0; i < size; ++i) {
            assertEquals(source[i], array[i]);
        }
    }

    @Test
    public void testZeroSizedArrayOfBooleanObjects() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Boolean[] source = new Boolean[0];

        encoder.writeArray(buffer, encoderState, source);

        Object result = decoder.readObject(buffer, decoderState);
        assertNotNull(result);
        assertTrue(result.getClass().isArray());
        assertTrue(result.getClass().getComponentType().isPrimitive());

        boolean[] array = (boolean[]) result;
        assertEquals(source.length, array.length);
    }

    @Test
    public void testDecodeSmallBooleanArray() throws IOException {
        doTestDecodeBooleanArrayType(SMALL_ARRAY_SIZE);
    }

    @Test
    public void testDecodeLargeBooleanArray() throws IOException {
        doTestDecodeBooleanArrayType(LARGE_ARRAY_SIZE);
    }

    private void doTestDecodeBooleanArrayType(int size) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        boolean[] source = new boolean[size];
        for (int i = 0; i < size; ++i) {
            source[i] = i % 2 == 0;
        }

        encoder.writeArray(buffer, encoderState, source);

        Object result = decoder.readObject(buffer, decoderState);
        assertNotNull(result);
        assertTrue(result.getClass().isArray());
        assertTrue(result.getClass().getComponentType().isPrimitive());

        boolean[] array = (boolean[]) result;
        assertEquals(size, array.length);

        for (int i = 0; i < size; ++i) {
            assertEquals(source[i], array[i]);
        }
    }

    @Test
    public void testArrayOfPrimitiveBooleanObjects() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        final int size = 10;

        boolean[] source = new boolean[size];
        for (int i = 0; i < size; ++i) {
            source[i] = i % 2 == 0;
        }

        encoder.writeArray(buffer, encoderState, source);

        Object result = decoder.readObject(buffer, decoderState);
        assertNotNull(result);
        assertTrue(result.getClass().isArray());
        assertTrue(result.getClass().getComponentType().isPrimitive());

        boolean[] array = (boolean[]) result;
        assertEquals(size, array.length);

        for (int i = 0; i < size; ++i) {
            assertEquals(source[i], array[i]);
        }
    }

    @Test
    public void testZeroSizedArrayOfPrimitiveBooleanObjects() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        boolean[] source = new boolean[0];

        encoder.writeArray(buffer, encoderState, source);

        Object result = decoder.readObject(buffer, decoderState);
        assertNotNull(result);
        assertTrue(result.getClass().isArray());
        assertTrue(result.getClass().getComponentType().isPrimitive());

        boolean[] array = (boolean[]) result;
        assertEquals(source.length, array.length);
    }

    @Ignore("Primitive type arrays not handled yet.")
    @Test
    public void testArrayOfArraysOfPrimitiveBooleanObjects() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        final int size = 10;

        boolean[][] source = new boolean[2][size];
        for (int i = 0; i < size; ++i) {
            source[0][i] = i % 2 == 0;
            source[1][i] = i % 2 == 0;
        }

        encoder.writeArray(buffer, encoderState, source);

        Object result = decoder.readObject(buffer, decoderState);
        assertNotNull(result);
        assertTrue(result.getClass().isArray());

        Object[] resultArray = (Object[]) result;

        assertNotNull(resultArray);
        assertEquals(2, resultArray.length);

        assertTrue(resultArray[0].getClass().isArray());
        assertTrue(resultArray[1].getClass().isArray());

        for (int i = 0; i < resultArray.length; ++i) {
            boolean[] nested = (boolean[]) resultArray[i];
            assertEquals(source[i].length, nested.length);
            assertArrayEquals(source[i], nested);
        }
    }
}
