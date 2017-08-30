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

import org.apache.qpid.proton4j.amqp.UnsignedLong;
import org.apache.qpid.proton4j.codec.EncoderState;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.apache.qpid.proton4j.codec.PrimitiveTypeEncoder;

import io.netty.buffer.ByteBuf;

/**
 * Encoder of AMQP UnsignedShort type values to a byte stream.
 */
public class UnsignedLongTypeEncoder implements PrimitiveTypeEncoder<UnsignedLong> {

    @Override
    public Class<UnsignedLong> getTypeClass() {
        return UnsignedLong.class;
    }

    @Override
    public void writeType(ByteBuf buffer, EncoderState state, UnsignedLong value) {
        write(buffer, state, value, true, true);
    }

    @Override
    public void writeValue(ByteBuf buffer, EncoderState state, UnsignedLong value) {
        write(buffer, state, value, false, true);
    }

    public void write(ByteBuf buffer, EncoderState state, UnsignedLong value, boolean writeType, boolean compact) {
        if (!compact) {
            if (writeType) {
                buffer.writeByte(EncodingCodes.ULONG);
            }
            buffer.writeLong(value.longValue());
        } else {
            if (value.equals(UnsignedLong.ZERO)) {
                if (writeType) {
                    buffer.writeByte(EncodingCodes.ULONG0);
                }
            } else if (value.longValue() <= 255l) {
                if (writeType) {
                    buffer.writeByte(EncodingCodes.SMALLULONG);
                }
                buffer.writeByte(value.byteValue());
            } else {
                if (writeType) {
                    buffer.writeByte(EncodingCodes.ULONG);
                }
                buffer.writeLong(value.longValue());
            }
        }
    }

    @Override
    public void writeArray(ByteBuf buffer, EncoderState state, UnsignedLong[] values) {
        buffer.writeByte(EncodingCodes.ARRAY32);

        // Array Size -> Total Bytes + Number of elements + Type Code
        long size = (Long.BYTES * values.length) + Integer.BYTES + Byte.BYTES;

        if (size > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Cannot encode given UnsignedLong array, encoded size to large: " + size);
        }

        buffer.writeInt((int) size);
        buffer.writeInt(values.length);
        buffer.writeByte(EncodingCodes.ULONG);
        for (UnsignedLong value : values) {
            buffer.writeLong(value.longValue());
        }
    }
}
