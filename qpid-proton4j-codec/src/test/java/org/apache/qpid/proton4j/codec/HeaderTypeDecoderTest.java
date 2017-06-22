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

import java.io.IOException;

import org.apache.qpid.proton4j.amqp.UnsignedByte;
import org.apache.qpid.proton4j.amqp.UnsignedInteger;
import org.apache.qpid.proton4j.amqp.messaging.Header;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * Test for decoder of AMQP Header type.
 */
public class HeaderTypeDecoderTest extends CodecTestSupport {

    private final int LARGE_SIZE = 1024 * 1024;
    private final int SMALL_SIZE = 32;

    @Test
    public void testDecodeHeader() throws IOException {
        doTestDecodeHeaderSeries(1);
    }

    @Test
    public void testDecodeSmallSeriesOfHeaders() throws IOException {
        doTestDecodeHeaderSeries(SMALL_SIZE);
    }

    @Test
    public void testDecodeLargeSeriesOfHeaders() throws IOException {
        doTestDecodeHeaderSeries(LARGE_SIZE);
    }

    private void doTestDecodeHeaderSeries(int size) throws IOException {
        ByteBuf buffer = Unpooled.buffer();

        Header header = new Header();

        header.setDurable(Boolean.TRUE);
        header.setPriority(UnsignedByte.valueOf((byte) 3));
        header.setDeliveryCount(UnsignedInteger.valueOf(10));
        header.setFirstAcquirer(Boolean.FALSE);
        header.setTtl(UnsignedInteger.valueOf(500));

        for (int i = 0; i < size; ++i) {
            encoder.writeObject(buffer, encoderState, header);
        }

        for (int i = 0; i < size; ++i) {
            final Object result = decoder.readObject(buffer, decoderState);

            assertNotNull(result);
            assertTrue(result instanceof Header);

            Header decoded = (Header) result;

            assertEquals(3, decoded.getPriority().intValue());
            assertTrue(decoded.getDurable().booleanValue());
        }
    }
}
