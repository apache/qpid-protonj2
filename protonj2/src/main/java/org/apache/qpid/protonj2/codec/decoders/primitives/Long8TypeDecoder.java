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
import org.apache.qpid.protonj2.codec.decoders.ProtonStreamUtils;

/**
 * Decode AMQP small Long values from a byte stream
 */
public final class Long8TypeDecoder extends LongTypeDecoder {

    @Override
    public Long readValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        return (long) buffer.readByte() & 0xff;
    }

    @Override
    public Long readValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        return Long.valueOf(ProtonStreamUtils.readByte(stream));
    }

    @Override
    public long readPrimitiveValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        return buffer.readByte();
    }

    @Override
    public long readPrimitiveValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        return ProtonStreamUtils.readByte(stream);
    }

    @Override
    public int getTypeCode() {
        return EncodingCodes.SMALLLONG & 0xff;
    }

    @Override
    public void skipValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        buffer.advanceReadOffset(Byte.BYTES);
    }

    @Override
    public void skipValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        ProtonStreamUtils.skipBytes(stream, Byte.BYTES);
    }

    @Override
    public int readSize(ProtonBuffer buffer, DecoderState state) {
        return Byte.BYTES;
    }

    @Override
    public int readSize(InputStream stream, StreamDecoderState state) {
        return Byte.BYTES;
    }
}
