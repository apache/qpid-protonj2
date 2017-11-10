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

import org.apache.qpid.proton4j.amqp.UnsignedInteger;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.codec.EncoderState;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.apache.qpid.proton4j.codec.PrimitiveTypeEncoder;

/**
 * Encoder of AMQP UnsignedShort type values to a byte stream.
 */
public class UnsignedIntegerTypeEncoder implements PrimitiveTypeEncoder<UnsignedInteger> {

    @Override
    public Class<UnsignedInteger> getTypeClass() {
        return UnsignedInteger.class;
    }

    @Override
    public void writeType(ProtonBuffer buffer, EncoderState state, UnsignedInteger value) {
        int intValue = value.intValue();

        if (intValue == 0) {
            buffer.writeByte(EncodingCodes.UINT0);
        } else if (intValue > 0 && intValue <= 255) {
            buffer.writeByte(EncodingCodes.SMALLUINT);
            buffer.writeByte(intValue);
        } else {
            buffer.writeByte(EncodingCodes.UINT);
            buffer.writeInt(intValue);
        }
    }

    @Override
    public void writeValue(ProtonBuffer buffer, EncoderState state, UnsignedInteger value) {
        buffer.writeInt(value.intValue());
    }

    @Override
    public void writeArray(ProtonBuffer buffer, EncoderState state, UnsignedInteger[] values) {
        buffer.writeByte(EncodingCodes.ARRAY32);

        // Array Size -> Total Bytes + Number of elements + Type Code
        long size = (Integer.BYTES * values.length) + Integer.BYTES + Byte.BYTES;

        if (size > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Cannot encode given UnsignedInteger array, encoded size to large: " + size);
        }

        buffer.writeInt((int) size);
        buffer.writeInt(values.length);
        buffer.writeByte(EncodingCodes.UINT);
        for (UnsignedInteger value : values) {
            buffer.writeInt(value.intValue());
        }
    }
}
