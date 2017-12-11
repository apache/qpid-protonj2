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
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.junit.Test;

/**
 * Test for decoder of AMQP Header type.
 */
public class HeaderTypeCodecTest extends CodecTestSupport {

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
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

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

    @Test
    public void testEncodeDecodeZeroSizedArrayOfHeaders() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Header[] headerArray = new Header[0];

        encoder.writeObject(buffer, encoderState, headerArray);

        final Object result = decoder.readObject(buffer, decoderState);

        assertTrue(result.getClass().isArray());
        assertEquals(Header.class, result.getClass().getComponentType());

        Header[] resultArray = (Header[]) result;
        assertEquals(0, resultArray.length);
    }

    @Test
    public void testEncodeDecodeArrayOfHeaders() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Header[] headerArray = new Header[3];

        headerArray[0] = new Header();
        headerArray[1] = new Header();
        headerArray[2] = new Header();

        headerArray[0].setDurable(true);
        headerArray[1].setDurable(true);
        headerArray[2].setDurable(true);

        encoder.writeObject(buffer, encoderState, headerArray);

        final Object result = decoder.readObject(buffer, decoderState);

        assertTrue(result.getClass().isArray());
        assertEquals(Header.class, result.getClass().getComponentType());

        Header[] resultArray = (Header[]) result;

        for (int i = 0; i < resultArray.length; ++i) {
            assertNotNull(resultArray[i]);
            assertTrue(resultArray[i] instanceof Header);
            assertEquals(headerArray[i].getDurable(), resultArray[i].getDurable());
        }
    }
}
