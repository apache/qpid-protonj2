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
package org.apache.qpid.proton4j.codec.transport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.UnsignedInteger;
import org.apache.qpid.proton4j.amqp.transport.Transfer;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.codec.CodecTestSupport;
import org.apache.qpid.proton4j.codec.TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.transport.TransferTypeDecoder;
import org.apache.qpid.proton4j.codec.encoders.transport.TransferTypeEncoder;
import org.junit.Test;

public class TransferTypeCodeTest extends CodecTestSupport {

    @Test
    public void testTypeClassReturnsCorrectType() throws IOException {
        assertEquals(Transfer.class, new TransferTypeDecoder().getTypeClass());
        assertEquals(Transfer.class, new TransferTypeEncoder().getTypeClass());
    }

    @Test
    public void testEncodeAndDecode() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Binary tag = new Binary(new byte[] {0, 1, 2});

        Transfer input = new Transfer();

        input.setHandle(UnsignedInteger.MAX_VALUE.longValue());
        input.setDeliveryId(10);
        input.setDeliveryTag(tag);
        input.setMessageFormat(0);
        input.setSettled(false);
        input.setBatchable(false);

        encoder.writeObject(buffer, encoderState, input);

        final Transfer result = (Transfer) decoder.readObject(buffer, decoderState);

        assertEquals(UnsignedInteger.MAX_VALUE.longValue(), result.getHandle());
        assertEquals(10, result.getDeliveryId());
        assertEquals(tag, result.getDeliveryTag());
        assertEquals(0, result.getMessageFormat());
        assertFalse(result.getSettled());
        assertFalse(result.getBatchable());
    }

    @Test
    public void testSkipValue() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Binary tag = new Binary(new byte[] {0, 1, 2});

        Transfer input = new Transfer();

        input.setHandle(UnsignedInteger.valueOf(2).longValue());
        input.setDeliveryId(100);
        input.setDeliveryTag(tag);
        input.setMessageFormat(1);
        input.setSettled(true);
        input.setBatchable(true);

        for (int i = 0; i < 10; ++i) {
            encoder.writeObject(buffer, encoderState, input);
        }

        input.setHandle(UnsignedInteger.MAX_VALUE.longValue());
        input.setDeliveryId(10);
        input.setDeliveryTag(tag);
        input.setMessageFormat(0);
        input.setSettled(false);
        input.setBatchable(false);

        encoder.writeObject(buffer, encoderState, input);

        for (int i = 0; i < 10; ++i) {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(Transfer.class, typeDecoder.getTypeClass());
            typeDecoder.skipValue(buffer, decoderState);
        }

        final Object result = decoder.readObject(buffer, decoderState);

        assertNotNull(result);
        assertTrue(result instanceof Transfer);

        Transfer value = (Transfer) result;
        assertEquals(UnsignedInteger.MAX_VALUE.longValue(), value.getHandle());
        assertEquals(10, value.getDeliveryId());
        assertEquals(tag, value.getDeliveryTag());
        assertEquals(0, value.getMessageFormat());
        assertFalse(value.getSettled());
        assertFalse(value.getBatchable());
    }
}
