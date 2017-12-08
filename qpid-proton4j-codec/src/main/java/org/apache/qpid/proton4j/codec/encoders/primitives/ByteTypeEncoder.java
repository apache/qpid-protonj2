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
package org.apache.qpid.proton4j.codec.encoders.primitives;

import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.codec.EncoderState;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.apache.qpid.proton4j.codec.PrimitiveTypeEncoder;

/**
 * Encoder of AMQP byte type value to a byte stream.
 */
public class ByteTypeEncoder implements PrimitiveTypeEncoder<Byte> {

    @Override
    public Class<Byte> getTypeClass() {
        return Byte.class;
    }

    @Override
    public void writeType(ProtonBuffer buffer, EncoderState state, Byte value) {
        buffer.writeByte(EncodingCodes.BYTE);
        buffer.writeByte(value.byteValue());
    }

    public void writeType(ProtonBuffer buffer, EncoderState state, byte value) {
        buffer.writeByte(EncodingCodes.BYTE);
        buffer.writeByte(value);
    }

    @Override
    public void writeArrayElements(ProtonBuffer buffer, EncoderState state, Byte[] values) {
        buffer.writeByte(EncodingCodes.BYTE);
        for (Byte byteVal : values) {
            buffer.writeByte(byteVal.byteValue());
        }
    }

    public void writeArrayElements(ProtonBuffer buffer, EncoderState state, byte[] values) {
        buffer.writeByte(EncodingCodes.BYTE);
        for (byte byteVal : values) {
            buffer.writeByte(byteVal);
        }
    }
}
