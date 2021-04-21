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
package org.apache.qpid.protonj2.codec.legacy;

import java.nio.ByteBuffer;

import org.apache.qpid.proton.codec.AMQPDefinedTypes;
import org.apache.qpid.proton.codec.DecoderImpl;
import org.apache.qpid.proton.codec.EncoderImpl;
import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonByteBufferAllocator;

/**
 * Adapter to allow using the legacy proton-j codec in tests for new proton library.
 */
public final class LegacyCodecAdapter {

    private final DecoderImpl decoder = new DecoderImpl();
    private final EncoderImpl encoder = new EncoderImpl(decoder);

    /**
     * Create a codec adapter instance.
     */
    public LegacyCodecAdapter() {
        AMQPDefinedTypes.registerAllTypes(decoder, encoder);
    }

    /**
     * Encode the given type using the legacy codec's Encoder implementation and then
     * transfer the encoded bytes into a {@link ProtonBuffer} which can be used for
     * decoding using the new codec.
     *
     * Usually this method should be passed a legacy type or other primitive value.
     *
     * @param value
     *      The value to be encoded in a {@link ProtonBuffer}.
     *
     * @return a {@link ProtonBuffer} with the encoded bytes ready for reading.
     */
    public ProtonBuffer encodeUsingLegacyEncoder(Object value) {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        ByteBuffer byteBuffer = ByteBuffer.allocate(8192);

        try {
            encoder.setByteBuffer(byteBuffer);
            encoder.writeObject(CodecToLegacyType.convertToLegacyType(value));
            byteBuffer.flip();
        } finally {
            encoder.setByteBuffer((ByteBuffer) null);
        }

        buffer.writeBytes(byteBuffer);

        return buffer;
    }

    /**
     * Decode a proper legacy type from the given buffer and return it inside a type
     * adapter that can be used for comparison.
     *
     * @param buffer
     *      The buffer containing the encoded type.
     *
     * @return a decoded version of the legacy encoded type that can compare against the new version of the type.
     */
    public Object decodeLegacyType(ProtonBuffer buffer) {
        ByteBuffer byteBuffer = buffer.toByteBuffer();
        Object result = null;

        try {
            decoder.setByteBuffer(byteBuffer);
            result = decoder.readObject();
        } finally {
            decoder.setBuffer(null);
        }

        return LegacyToCodecType.convertToCodecType(result);
    }
}
