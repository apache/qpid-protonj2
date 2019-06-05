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
package org.apache.qpid.proton4j.codec.messaging;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.qpid.proton4j.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.codec.CodecTestSupport;
import org.apache.qpid.proton4j.codec.decoders.messaging.ApplicationPropertiesTypeDecoder;
import org.apache.qpid.proton4j.codec.encoders.messaging.ApplicationPropertiesTypeEncoder;
import org.junit.Test;

public class ApplicationPropertiesTypeCodecTest extends CodecTestSupport {

    @Test
    public void testTypeClassReturnsCorrectType() throws IOException {
        assertEquals(ApplicationProperties.class, new ApplicationPropertiesTypeDecoder().getTypeClass());
        assertEquals(ApplicationProperties.class, new ApplicationPropertiesTypeEncoder().getTypeClass());
    }

    @Test
    public void testDecodeSmallSeriesOfApplicationProperties() throws IOException {
        doTestDecodeHeaderSeries(SMALL_SIZE);
    }

    @Test
    public void testDecodeLargeSeriesOfApplicationProperties() throws IOException {
        doTestDecodeHeaderSeries(LARGE_SIZE);
    }

    private void doTestDecodeHeaderSeries(int size) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Map<String, Object> propertiesMap = new LinkedHashMap<>();
        ApplicationProperties properties = new ApplicationProperties(propertiesMap);

        propertiesMap.put("key-1", "1");
        propertiesMap.put("key-2", "2");
        propertiesMap.put("key-3", "3");
        propertiesMap.put("key-4", "4");
        propertiesMap.put("key-5", "5");
        propertiesMap.put("key-6", "6");
        propertiesMap.put("key-7", "7");
        propertiesMap.put("key-8", "8");

        for (int i = 0; i < size; ++i) {
            encoder.writeObject(buffer, encoderState, properties);
        }

        for (int i = 0; i < size; ++i) {
            final Object result = decoder.readObject(buffer, decoderState);

            assertNotNull(result);
            assertTrue(result instanceof ApplicationProperties);

            ApplicationProperties decoded = (ApplicationProperties) result;

            assertEquals(8, decoded.getValue().size());
            assertTrue(decoded.getValue().equals(propertiesMap));
        }
    }

    @Test
    public void testEncodeDecodeZeroSizedArrayOfApplicationProperties() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        ApplicationProperties[] array = new ApplicationProperties[0];

        encoder.writeObject(buffer, encoderState, array);

        final Object result = decoder.readObject(buffer, decoderState);

        assertTrue(result.getClass().isArray());
        assertEquals(ApplicationProperties.class, result.getClass().getComponentType());

        ApplicationProperties[] resultArray = (ApplicationProperties[]) result;
        assertEquals(0, resultArray.length);
    }

    @Test
    public void testEncodeDecodeArrayOfApplicationProperties() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        ApplicationProperties[] array = new ApplicationProperties[3];

        array[0] = new ApplicationProperties(new HashMap<String, Object>());
        array[1] = new ApplicationProperties(new HashMap<String, Object>());
        array[2] = new ApplicationProperties(new HashMap<String, Object>());

        array[0].getValue().put("key-1", "1");
        array[1].getValue().put("key-1", "2");
        array[2].getValue().put("key-1", "3");

        encoder.writeObject(buffer, encoderState, array);

        final Object result = decoder.readObject(buffer, decoderState);

        assertTrue(result.getClass().isArray());
        assertEquals(ApplicationProperties.class, result.getClass().getComponentType());

        ApplicationProperties[] resultArray = (ApplicationProperties[]) result;

        for (int i = 0; i < resultArray.length; ++i) {
            assertNotNull(resultArray[i]);
            assertTrue(resultArray[i] instanceof ApplicationProperties);
            assertEquals(array[i].getValue(), resultArray[i].getValue());
        }
    }
}
