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

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.messaging.Data;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.junit.Test;

public class DataTypeCodecTest extends CodecTestSupport {

    private final int LARGE_SIZE = 1024;
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
    public void testEncodeDecodeArrayOfDataSections() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Data[] dataArray = new Data[3];

        dataArray[0] = new Data(new Binary(new byte[] { 1, 2, 3}));
        dataArray[1] = new Data(new Binary(new byte[] { 4, 5, 6}));
        dataArray[2] = new Data(new Binary(new byte[] { 7, 8, 9}));

        encoder.writeObject(buffer, encoderState, dataArray);

        final Object result = decoder.readObject(buffer, decoderState);

        assertTrue(result.getClass().isArray());
        assertEquals(Data.class, result.getClass().getComponentType());

        Data[] resultArray = (Data[]) result;

        for (int i = 0; i < resultArray.length; ++i) {
            assertNotNull(resultArray[i]);
            assertTrue(resultArray[i] instanceof Data);
            assertEquals(dataArray[i].getValue(), resultArray[i].getValue());
        }
    }
}
