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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.qpid.proton4j.amqp.UnknownDescribedType;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.codec.util.NoLocalType;
import org.junit.Test;

/**
 * Tests the handling of UnknownDescribedType instances.
 */
public class UnknownDescribedTypeCodecTest extends CodecTestSupport {

    @Test
    public void testDecodeUnknownDescribedType() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        encoder.writeObject(buffer, encoderState, NoLocalType.NO_LOCAL);

        Object result = decoder.readObject(buffer, decoderState);
        assertTrue(result instanceof UnknownDescribedType);
        UnknownDescribedType resultTye = (UnknownDescribedType) result;
        assertEquals(NoLocalType.NO_LOCAL.getDescriptor(), resultTye.getDescriptor());
    }

    @Test
    public void testDecodeSmallSeriesOfUnknownDescribedTypes() throws IOException {
        doTestDecodeUnknownDescribedTypeSeries(SMALL_SIZE);
    }

    @Test
    public void testDecodeLargeSeriesOfUnknownDescribedTypes() throws IOException {
        doTestDecodeUnknownDescribedTypeSeries(LARGE_SIZE);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testUnknownDescribedTypeInList() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        List<Object> listOfUnkowns = new ArrayList<>();

        listOfUnkowns.add(NoLocalType.NO_LOCAL);

        encoder.writeList(buffer, encoderState, listOfUnkowns);

        final Object result = decoder.readObject(buffer, decoderState);

        assertNotNull(result);
        assertTrue(result instanceof List);

        final List<Object> decodedList = (List<Object>) result;
        assertEquals(1, decodedList.size());

        final Object listEntry = decodedList.get(0);
        assertTrue(listEntry instanceof UnknownDescribedType);

        UnknownDescribedType resultTye = (UnknownDescribedType) listEntry;
        assertEquals(NoLocalType.NO_LOCAL.getDescriptor(), resultTye.getDescriptor());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testUnknownDescribedTypeInMap() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Map<Object, Object> mapOfUnknowns = new HashMap<>();

        mapOfUnknowns.put(NoLocalType.NO_LOCAL.getDescriptor(), NoLocalType.NO_LOCAL);

        encoder.writeMap(buffer, encoderState, mapOfUnknowns);

        final Object result = decoder.readObject(buffer, decoderState);

        assertNotNull(result);
        assertTrue(result instanceof Map);

        final Map<Object, Object> decodedMap = (Map<Object, Object>) result;
        assertEquals(1, decodedMap.size());

        final Object mapEntry = decodedMap.get(NoLocalType.NO_LOCAL.getDescriptor());
        assertTrue(mapEntry instanceof UnknownDescribedType);

        UnknownDescribedType resultTye = (UnknownDescribedType) mapEntry;
        assertEquals(NoLocalType.NO_LOCAL.getDescriptor(), resultTye.getDescriptor());
    }

    @Test
    public void testUnknownDescribedTypeInArray() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        NoLocalType[] arrayOfUnknown = new NoLocalType[1];

        arrayOfUnknown[0] = NoLocalType.NO_LOCAL;

        try {
            encoder.writeArray(buffer, encoderState, arrayOfUnknown);
            fail("Should not be able to write an array of unregistered described type");
        } catch (IllegalArgumentException iae) {}

        try {
            encoder.writeObject(buffer, encoderState, arrayOfUnknown);
            fail("Should not be able to write an array of unregistered described type");
        } catch (IllegalArgumentException iae) {}
    }

    private void doTestDecodeUnknownDescribedTypeSeries(int size) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        for (int i = 0; i < size; ++i) {
            encoder.writeObject(buffer, encoderState, NoLocalType.NO_LOCAL);
        }

        for (int i = 0; i < size; ++i) {
            final Object result = decoder.readObject(buffer, decoderState);

            assertNotNull(result);
            assertTrue(result instanceof UnknownDescribedType);

            UnknownDescribedType resultTye = (UnknownDescribedType) result;
            assertEquals(NoLocalType.NO_LOCAL.getDescriptor(), resultTye.getDescriptor());
        }
    }
}
