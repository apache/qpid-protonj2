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
import java.util.Date;

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.messaging.Properties;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.codec.CodecTestSupport;
import org.apache.qpid.proton4j.codec.decoders.messaging.PropertiesTypeDecoder;
import org.apache.qpid.proton4j.codec.encoders.messaging.PropertiesTypeEncoder;
import org.junit.Test;

/**
 * Test for decoder of AMQP Properties type.
 */
public class PropertiesTypeCodecTest extends CodecTestSupport {

    @Test
    public void testTypeClassReturnsCorrectType() throws IOException {
        assertEquals(Properties.class, new PropertiesTypeDecoder().getTypeClass());
        assertEquals(Properties.class, new PropertiesTypeEncoder().getTypeClass());
    }

    @Test
    public void testDecodeSmallSeriesOfPropertiess() throws IOException {
        doTestDecodePropertiesSeries(SMALL_SIZE);
    }

    @Test
    public void testDecodeLargeSeriesOfPropertiess() throws IOException {
        doTestDecodePropertiesSeries(LARGE_SIZE);
    }

    private void doTestDecodePropertiesSeries(int size) throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Properties properties = new Properties();

        Date timeNow = new Date(System.currentTimeMillis());

        properties.setMessageId("ID:Message-1:1:1:0");
        properties.setUserId(new Binary(new byte[1]));
        properties.setTo("queue:work");
        properties.setSubject("help");
        properties.setReplyTo("queue:temp:me");
        properties.setContentEncoding("text/UTF-8");
        properties.setContentType("text");
        properties.setCorrelationId("correlation-id");
        properties.setAbsoluteExpiryTime(timeNow.getTime());
        properties.setCreationTime(timeNow.getTime());
        properties.setGroupId("group-1");
        properties.setGroupSequence(1);
        properties.setReplyToGroupId("group-1");

        for (int i = 0; i < size; ++i) {
            encoder.writeObject(buffer, encoderState, properties);
        }

        for (int i = 0; i < size; ++i) {
            final Object result = decoder.readObject(buffer, decoderState);

            assertNotNull(result);
            assertTrue(result instanceof Properties);

            Properties decoded = (Properties) result;

            assertNotNull(decoded.getAbsoluteExpiryTime());
            assertEquals(timeNow.getTime(), decoded.getAbsoluteExpiryTime());
            assertEquals("text/UTF-8", decoded.getContentEncoding());
            assertEquals("text", decoded.getContentType());
            assertEquals("correlation-id", decoded.getCorrelationId());
            assertEquals(timeNow.getTime(), decoded.getCreationTime());
            assertEquals("group-1", decoded.getGroupId());
            assertEquals(1, decoded.getGroupSequence());
            assertEquals("ID:Message-1:1:1:0", decoded.getMessageId());
            assertEquals("queue:temp:me", decoded.getReplyTo());
            assertEquals("group-1", decoded.getReplyToGroupId());
            assertEquals("help", decoded.getSubject());
            assertEquals("queue:work", decoded.getTo());
            assertTrue(decoded.getUserId() instanceof Binary);
        }
    }
}
