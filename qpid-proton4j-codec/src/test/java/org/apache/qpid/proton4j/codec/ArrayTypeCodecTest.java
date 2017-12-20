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

import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.junit.Test;

/**
 * Test decoding of AMQP Array types
 */
public class ArrayTypeCodecTest extends CodecTestSupport {

    @Test
    public void testWriteOfZeroSizedGenericArrayFails() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Object[] source = new Object[0];

        try {
            encoder.writeArray(buffer, encoderState, source);
            fail("Should reject attempt to write zero sized array of unknown type.");
        } catch (IllegalArgumentException iae) {
            // Expected
        }
    }

    @Test
    public void testWriteOfGenericArrayOfObjectsFails() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Object[] source = new Object[2];

        source[0] = new Object();
        source[1] = new Object();

        try {
            encoder.writeArray(buffer, encoderState, source);
            fail("Should reject attempt to write array of unknown type.");
        } catch (IllegalArgumentException iae) {
            // Expected
        }
    }

    @Test
    public void testArrayOfArraysOfMixedTypes() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        final int size = 10;

        Object[][] source = new Object[2][size];
        for (int i = 0; i < size; ++i) {
            source[0][i] = Short.valueOf((short) i);
            source[1][i] = Integer.valueOf(i);
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
    }

    @Test
    public void testArrayOfArraysOfArraysOfShortTypes() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        final int size = 10;

        Object[][][] source = new Object[2][2][size];
        for (int i = 0; i < source.length; ++i) {
            for (int j = 0; j < source[i].length; ++j) {
                for (int k = 0; k < source[i][j].length; ++k) {
                    source[i][j][k] = (short) k;
                }
             }
        }

        encoder.writeArray(buffer, encoderState, source);

        Object result = decoder.readObject(buffer, decoderState);
        assertNotNull(result);
        assertTrue(result.getClass().isArray());

        Object[] resultArray = (Object[]) result;

        assertNotNull(resultArray);
        assertEquals(2, resultArray.length);

        for (int i = 0; i < resultArray.length; ++i) {
            assertTrue(resultArray[i].getClass().isArray());

            Object[] dimension2 = (Object[]) resultArray[i];
            assertEquals(2, dimension2.length);

            for (int j = 0; j < dimension2.length; ++j) {
                short[] dimension3 = (short[]) dimension2[j];
                assertEquals(size, dimension3.length);

                for (int k = 0; k < dimension3.length; ++k) {
                    assertEquals(source[i][j][k], dimension3[k]);
                }
             }
        }
    }

    @Test
    public void testWriteArrayOfArraysStrings() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        String[][] stringArray = new String[2][1];

        stringArray[0][0] = "short-string";
        stringArray[1][0] = "long-string-entry:" + UUID.randomUUID().toString() + "," +
                                                   UUID.randomUUID().toString() + "," +
                                                   UUID.randomUUID().toString() + "," +
                                                   UUID.randomUUID().toString() + "," +
                                                   UUID.randomUUID().toString() + "," +
                                                   UUID.randomUUID().toString() + "," +
                                                   UUID.randomUUID().toString();

        encoder.writeArray(buffer, encoderState, stringArray);

        Object result = decoder.readObject(buffer, decoderState);

        assertNotNull(result);
        assertTrue(result.getClass().isArray());

        Object[] array = (Object[]) result;
        assertEquals(2, array.length);

        assertTrue(array[0] instanceof String[]);
        assertTrue(array[1] instanceof String[]);

        String[] element1Array = (String[]) array[0];
        String[] element2Array = (String[]) array[1];

        assertEquals(1, element1Array.length);
        assertEquals(1, element2Array.length);

        assertEquals(stringArray[0][0], element1Array[0]);
        assertEquals(stringArray[1][0], element2Array[0]);
    }
}
