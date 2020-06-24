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

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.DecoderState;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.decoders.AbstractPrimitiveTypeDecoder;
import org.apache.qpid.protonj2.types.Decimal64;

/**
 * Decoder of AMQP Decimal64 values from a byte stream
 */
public final class Decimal64TypeDecoder extends AbstractPrimitiveTypeDecoder<Decimal64> {

    @Override
    public Class<Decimal64> getTypeClass() {
        return Decimal64.class;
    }

    @Override
    public Decimal64 readValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        return new Decimal64(buffer.readLong());
    }

    @Override
    public int getTypeCode() {
        return EncodingCodes.DECIMAL64 & 0xff;
    }

    @Override
    public void skipValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        buffer.skipBytes(Decimal64.BYTES);
    }
}
