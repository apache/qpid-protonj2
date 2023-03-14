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
import org.apache.qpid.protonj2.types.UnsignedLong;

/**
 * Decode AMQP Zero value Unsigned Long values from a byte stream
 */
public final class UnsignedLong0TypeDecoder extends AbstractPrimitiveTypeDecoder<UnsignedLong> {

    @Override
    public int getTypeCode() {
        return EncodingCodes.ULONG0 & 0xff;
    }

    @Override
    public Class<UnsignedLong> getTypeClass() {
        return UnsignedLong.class;
    }

    @Override
    public UnsignedLong readValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        return UnsignedLong.ZERO;
    }

    @Override
    public UnsignedLong readValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        return UnsignedLong.ZERO;
    }

    @Override
    public void skipValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
    }

    @Override
    public void skipValue(InputStream stream, StreamDecoderState state) throws DecodeException {
    }

    @Override
    public int readSize(ProtonBuffer buffer, DecoderState state) {
        return 0;
    }

    @Override
    public int readSize(InputStream stream, StreamDecoderState state) {
        return 0;
    }
}
