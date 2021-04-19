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
package org.apache.qpid.protonj2.codec;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.InputStream;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferInputStream;
import org.apache.qpid.protonj2.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.protonj2.types.DeliveryTag;
import org.junit.jupiter.api.Test;

public class DeliveryTagCodecTest extends CodecTestSupport {

    @Test
    public void testDecoderThrowsWhenAskedToReadWrongTypeAsThisType() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        buffer.writeByte(EncodingCodes.UINT);

        try {
            decoder.readDeliveryTag(buffer, decoderState);
            fail("Should not allow read of integer type as this type");
        } catch (DecodeException e) {}
    }

    @Test
    public void testDecoderThrowsWhenAskedToReadWrongTypeAsThisTypeFromStream() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        InputStream stream = new ProtonBufferInputStream(buffer);

        buffer.writeByte(EncodingCodes.UINT);

        try {
            streamDecoder.readDeliveryTag(stream, streamDecoderState);
            fail("Should not allow read of integer type as this type");
        } catch (DecodeException e) {}
    }

    @Test
    public void testReadDeliveryTagsFromBinaryEncodedValues() throws Exception {
        testReadDeliveryTagsFromBinaryEncodedValues(false);
    }

    @Test
    public void testReadDeliveryTagsFromBinaryEncodedValuesFS() throws Exception {
        testReadDeliveryTagsFromBinaryEncodedValues(true);
    }

    public void testReadDeliveryTagsFromBinaryEncodedValues(boolean fromStream) throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate(32, 32);
        InputStream stream = new ProtonBufferInputStream(buffer);

        final byte[] tagBytes = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };

        buffer.writeByte(EncodingCodes.NULL);

        if (fromStream) {
            assertNull(streamDecoder.readDeliveryTag(stream, streamDecoderState));
        } else {
            assertNull(decoder.readDeliveryTag(buffer, decoderState));
        }

        buffer.writeByte(EncodingCodes.VBIN8);
        buffer.writeByte(tagBytes.length);
        buffer.writeBytes(tagBytes);

        buffer.writeByte(EncodingCodes.VBIN32);
        buffer.writeInt(tagBytes.length);
        buffer.writeBytes(tagBytes);

        final DeliveryTag tag1;
        final DeliveryTag tag2;

        if (fromStream) {
            tag1 = streamDecoder.readDeliveryTag(stream, streamDecoderState);
            tag2 = streamDecoder.readDeliveryTag(stream, streamDecoderState);
        } else {
            tag1 = decoder.readDeliveryTag(buffer, decoderState);
            tag2 = decoder.readDeliveryTag(buffer, decoderState);
        }

        assertNotSame(tag1, tag2);
        assertArrayEquals(tag1.tagBytes(), tag2.tagBytes());
        assertArrayEquals(tagBytes, tag1.tagBytes());
        assertArrayEquals(tagBytes, tag2.tagBytes());
    }
}
