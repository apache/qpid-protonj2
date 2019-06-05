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

import org.apache.qpid.proton4j.amqp.messaging.DeleteOnNoLinksOrMessages;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.codec.CodecTestSupport;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.apache.qpid.proton4j.codec.decoders.messaging.DeleteOnNoLinksOrMessagesTypeDecoder;
import org.apache.qpid.proton4j.codec.encoders.messaging.DeleteOnNoLinksOrMessagesTypeEncoder;
import org.junit.Test;

/**
 * Test codec handling of DeleteOnNoLinksOrMessages types.
 */
public class DeleteOnNoLinksOrMessagesCodecTest  extends CodecTestSupport {

    @Test
    public void testTypeClassReturnsCorrectType() throws IOException {
        assertEquals(DeleteOnNoLinksOrMessages.class, new DeleteOnNoLinksOrMessagesTypeDecoder().getTypeClass());
        assertEquals(DeleteOnNoLinksOrMessages.class, new DeleteOnNoLinksOrMessagesTypeEncoder().getTypeClass());
    }

    @Test
    public void TestDecodeDeleteOnNoLinksOrMessages() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        DeleteOnNoLinksOrMessages value = DeleteOnNoLinksOrMessages.getInstance();

        encoder.writeObject(buffer, encoderState, value);

        final Object result = decoder.readObject(buffer, decoderState);

        assertNotNull(result);
        assertTrue(result instanceof DeleteOnNoLinksOrMessages);
    }

    @Test
    public void TestDecodeDeleteOnNoLinksOrMessagesWithList8() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        buffer.writeByte((byte) 0); // Described Type Indicator
        buffer.writeByte(EncodingCodes.SMALLULONG);
        buffer.writeByte(DeleteOnNoLinksOrMessages.DESCRIPTOR_CODE.byteValue());
        buffer.writeByte(EncodingCodes.LIST8);
        buffer.writeByte((byte) 0);  // Size
        buffer.writeByte((byte) 0);  // Count

        final Object result = decoder.readObject(buffer, decoderState);

        assertNotNull(result);
        assertTrue(result instanceof DeleteOnNoLinksOrMessages);
    }

    @Test
    public void TestDecodeDeleteOnNoLinksOrMessagesWithList32() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        buffer.writeByte((byte) 0); // Described Type Indicator
        buffer.writeByte(EncodingCodes.SMALLULONG);
        buffer.writeByte(DeleteOnNoLinksOrMessages.DESCRIPTOR_CODE.byteValue());
        buffer.writeByte(EncodingCodes.LIST32);
        buffer.writeInt((byte) 0);  // Size
        buffer.writeInt((byte) 0);  // Count

        final Object result = decoder.readObject(buffer, decoderState);

        assertNotNull(result);
        assertTrue(result instanceof DeleteOnNoLinksOrMessages);
    }
}
