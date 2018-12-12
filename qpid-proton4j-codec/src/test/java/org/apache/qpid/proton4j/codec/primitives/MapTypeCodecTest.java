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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.codec.CodecTestSupport;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.junit.Test;

public class MapTypeCodecTest extends CodecTestSupport {

    @Test
    public void testDecodeSmallSeriesOfMaps() throws IOException {
        doTestDecodeMapSeries(SMALL_SIZE);
    }

    @Test
    public void testDecodeLargeSeriesOfMaps() throws IOException {
        doTestDecodeMapSeries(LARGE_SIZE);
    }

    @SuppressWarnings("unchecked")
    private void doTestDecodeMapSeries(int size) throws IOException {

        String myBoolKey = "myBool";
        boolean myBool = true;
        String myByteKey = "myByte";
        byte myByte = 4;
        String myBytesKey = "myBytes";
        byte[] myBytes = myBytesKey.getBytes();
        String myCharKey = "myChar";
        char myChar = 'd';
        String myDoubleKey = "myDouble";
        double myDouble = 1234567890123456789.1234;
        String myFloatKey = "myFloat";
        float myFloat = 1.1F;
        String myIntKey = "myInt";
        int myInt = Integer.MAX_VALUE;
        String myLongKey = "myLong";
        long myLong = Long.MAX_VALUE;
        String myShortKey = "myShort";
        short myShort = 25;
        String myStringKey = "myString";
        String myString = myStringKey;

        Map<String, Object> map = new LinkedHashMap<String, Object>();
        map.put(myBoolKey, myBool);
        map.put(myByteKey, myByte);
        map.put(myBytesKey, new Binary(myBytes));
        map.put(myCharKey, myChar);
        map.put(myDoubleKey, myDouble);
        map.put(myFloatKey, myFloat);
        map.put(myIntKey, myInt);
        map.put(myLongKey, myLong);
        map.put(myShortKey, myShort);
        map.put(myStringKey, myString);

        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        for (int i = 0; i < size; ++i) {
            encoder.writeObject(buffer, encoderState, map);
        }

        for (int i = 0; i < size; ++i) {
            final Object result = decoder.readObject(buffer, decoderState);

            assertNotNull(result);
            assertTrue(result instanceof Map);

            Map<String, Object> resultMap = (Map<String, Object>) result;

            assertEquals(map.size(), resultMap.size());
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void testArrayOfMApsOfStringToUUIDs() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Map<String, UUID>[] source = new LinkedHashMap[2];
        for (int i = 0; i < source.length; ++i) {
            source[i] = new LinkedHashMap<String, UUID>();
            source[i].put("1", UUID.randomUUID());
            source[i].put("2", UUID.randomUUID());
            source[i].put("3", UUID.randomUUID());
        }

        encoder.writeArray(buffer, encoderState, source);

        Object result = decoder.readObject(buffer, decoderState);
        assertNotNull(result);
        assertTrue(result.getClass().isArray());

        Map[] map = (Map[]) result;
        assertEquals(source.length, map.length);

        for (int i = 0; i < map.length; ++i) {
            assertEquals(source[i], map[i]);
        }
    }

    @Test
    public void testSizeToLargeValidation() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        buffer.writeByte(EncodingCodes.MAP32);
        buffer.writeInt(Integer.MAX_VALUE);
        buffer.writeInt(2);
        buffer.writeByte(EncodingCodes.STR8);
        buffer.writeByte(4);
        buffer.writeBytes("test".getBytes(StandardCharsets.UTF_8));
        buffer.writeByte(EncodingCodes.STR8);
        buffer.writeByte(5);
        buffer.writeBytes("value".getBytes(StandardCharsets.UTF_8));

        try {
            decoder.readObject(buffer, decoderState);
            fail("should throw an IllegalArgumentException");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testOddElementCountDetected() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        buffer.writeByte(EncodingCodes.MAP32);
        buffer.writeInt(17);
        buffer.writeInt(1);
        buffer.writeByte(EncodingCodes.STR8);
        buffer.writeByte(4);
        buffer.writeBytes("test".getBytes(StandardCharsets.UTF_8));
        buffer.writeByte(EncodingCodes.STR8);
        buffer.writeByte(5);
        buffer.writeBytes("value".getBytes(StandardCharsets.UTF_8));

        try {
            decoder.readObject(buffer, decoderState);
            fail("should throw an IllegalArgumentException");
        } catch (IllegalArgumentException iae) {}
    }
}
