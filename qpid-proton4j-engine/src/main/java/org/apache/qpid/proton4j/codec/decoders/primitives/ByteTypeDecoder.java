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

/**
 * Decoder of AMQP Bytes from a byte stream.
 */
public final class ByteTypeDecoder extends AbstractPrimitiveTypeDecoder<Byte> {

    @Override
    public boolean isJavaPrimitive() {
        return true;
    }

    @Override
    public Class<Byte> getTypeClass() {
        return Byte.class;
    }

    @Override
    public Byte readValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        return buffer.readByte();
    }

    @Override
    public int getTypeCode() {
        return EncodingCodes.BYTE & 0xff;
    }

    public byte readPrimitiveValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        return buffer.readByte();
    }

    @Override
    public void skipValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        buffer.skipBytes(Byte.BYTES);
    }
}
