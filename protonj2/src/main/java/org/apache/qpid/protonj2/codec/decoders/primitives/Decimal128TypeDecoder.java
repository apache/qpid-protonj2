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
import org.apache.qpid.protonj2.types.Decimal128;

/**
 * Decoder of AMQP Decimal128 values from a byte stream
 */
public final class Decimal128TypeDecoder extends AbstractPrimitiveTypeDecoder<Decimal128> {

    @Override
    public Class<Decimal128> getTypeClass() {
        return Decimal128.class;
    }

    @Override
    public int getTypeCode() {
        return EncodingCodes.DECIMAL128 & 0xff;
    }

    @Override
    public Decimal128 readValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        long msb = buffer.readLong();
        long lsb = buffer.readLong();

        return new Decimal128(msb, lsb);
    }

    @Override
    public Decimal128 readValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        long msb = ProtonStreamUtils.readLong(stream);
        long lsb = ProtonStreamUtils.readLong(stream);

        return new Decimal128(msb, lsb);
    }

    @Override
    public void skipValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        buffer.advanceReadOffset(Decimal128.BYTES);
    }

    @Override
    public void skipValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        ProtonStreamUtils.skipBytes(stream, Decimal128.BYTES);
    }
}
