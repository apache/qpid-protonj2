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
package org.apache.qpid.proton4j.codec.decoders.primitives;

import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.codec.DecodeException;
import org.apache.qpid.proton4j.codec.DecoderState;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.apache.qpid.proton4j.codec.decoders.AbstractPrimitiveTypeDecoder;
import org.apache.qpid.proton4j.types.Decimal128;

/**
 * Decoder of AMQP Decimal128 values from a byte stream
 */
public final class Decimal128TypeDecoder extends AbstractPrimitiveTypeDecoder<Decimal128> {

    @Override
    public Class<Decimal128> getTypeClass() {
        return Decimal128.class;
    }

    @Override
    public Decimal128 readValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        long msb = buffer.readLong();
        long lsb = buffer.readLong();

        return new Decimal128(msb, lsb);
    }

    @Override
    public int getTypeCode() {
        return EncodingCodes.DECIMAL128 & 0xff;
    }

    @Override
    public void skipValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        buffer.skipBytes(Decimal128.BYTES);
    }
}
