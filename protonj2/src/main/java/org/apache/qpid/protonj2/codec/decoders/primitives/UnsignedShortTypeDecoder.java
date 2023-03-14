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
import org.apache.qpid.protonj2.types.UnsignedShort;

/**
 * Decode AMQP Unsigned Short values from a byte stream
 */
public final class UnsignedShortTypeDecoder extends AbstractPrimitiveTypeDecoder<UnsignedShort> {

    @Override
    public Class<UnsignedShort> getTypeClass() {
        return UnsignedShort.class;
    }

    @Override
    public int getTypeCode() {
        return EncodingCodes.USHORT & 0xff;
    }

    @Override
    public UnsignedShort readValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        return UnsignedShort.valueOf(buffer.readShort());
    }

    @Override
    public UnsignedShort readValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        return UnsignedShort.valueOf(ProtonStreamUtils.readShort(stream));
    }

    @Override
    public void skipValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        buffer.advanceReadOffset(Short.BYTES);
    }

    @Override
    public void skipValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        ProtonStreamUtils.skipBytes(stream, Short.BYTES);
    }

    @Override
    public int readSize(ProtonBuffer buffer, DecoderState state) {
        return Short.BYTES;
    }

    @Override
    public int readSize(InputStream stream, StreamDecoderState state) {
        return Short.BYTES;
    }
}
