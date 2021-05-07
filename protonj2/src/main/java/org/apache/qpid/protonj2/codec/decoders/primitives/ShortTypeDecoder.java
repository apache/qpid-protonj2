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
package org.apache.qpid.protonj2.codec.decoders.primitives;

import java.io.InputStream;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.DecoderState;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.StreamDecoderState;
import org.apache.qpid.protonj2.codec.decoders.AbstractPrimitiveTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.ProtonStreamUtils;

/**
 * Decode AMQP Short values from a byte stream
 */
public final class ShortTypeDecoder extends AbstractPrimitiveTypeDecoder<Short> {

    @Override
    public boolean isJavaPrimitive() {
        return true;
    }

    @Override
    public Class<Short> getTypeClass() {
        return Short.class;
    }

    @Override
    public int getTypeCode() {
        return EncodingCodes.SHORT & 0xff;
    }

    @Override
    public Short readValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        return buffer.readShort();
    }

    @Override
    public Short readValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        return Short.valueOf(ProtonStreamUtils.readShort(stream));
    }

    /**
     * Reads the primitive value from the given {@link ProtonBuffer} and returns it.
     *
     * @param buffer
     * 		The {@link ProtonBuffer} where the primitive value should be read from.
     * @param state
     * 		The {@link DecoderState} that can be used during decode of the value.
     *
     * @return the decoded primitive value.
     *
     * @throws DecodeException if an error occurs while reading the encoded value.
     */
    public short readPrimitiveValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        return buffer.readShort();
    }

    /**
     * Reads the primitive value from the given {@link InputStream} and returns it.
     *
     * @param stream
     * 		The {@link InputStream} where the primitive value should be read from.
     * @param state
     * 		The {@link DecoderState} that can be used during decode of the value.
     *
     * @return the decoded primitive value.
     *
     * @throws DecodeException if an error occurs while reading the encoded value.
     */
    public short readPrimitiveValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        return ProtonStreamUtils.readShort(stream);
    }

    @Override
    public void skipValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        buffer.skipBytes(Short.BYTES);
    }

    @Override
    public void skipValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        ProtonStreamUtils.skipBytes(stream, Short.BYTES);
    }
}
