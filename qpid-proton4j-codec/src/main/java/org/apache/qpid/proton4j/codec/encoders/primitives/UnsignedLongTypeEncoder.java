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
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.codec.EncoderState;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.apache.qpid.proton4j.codec.encoders.AbstractPrimitiveTypeEncoder;

/**
 * Encoder of AMQP UnsignedShort type values to a byte stream.
 */
public final class UnsignedLongTypeEncoder extends AbstractPrimitiveTypeEncoder<UnsignedLong> {

    @Override
    public Class<UnsignedLong> getTypeClass() {
        return UnsignedLong.class;
    }

    @Override
    public void writeType(ProtonBuffer buffer, EncoderState state, UnsignedLong value) {
        writeType(buffer, state, value.longValue());
    }

    public void writeType(ProtonBuffer buffer, EncoderState state, long value) {
        if (value == 0) {
            buffer.writeByte(EncodingCodes.ULONG0);
        } else if (value > 0 && value <= 255) {
            buffer.writeByte(EncodingCodes.SMALLULONG);
            buffer.writeByte((int) value);
        } else {
            buffer.writeByte(EncodingCodes.ULONG);
            buffer.writeLong(value);
        }
    }

    public void writeType(ProtonBuffer buffer, EncoderState state, byte value) {
        if (value == 0) {
            buffer.writeByte(EncodingCodes.ULONG0);
        } else {
            buffer.writeByte(EncodingCodes.SMALLULONG);
            buffer.writeByte(value);
        }
    }

    @Override
    public void writeRawArray(ProtonBuffer buffer, EncoderState state, Object[] values) {
        buffer.writeByte(EncodingCodes.ULONG);
        for (Object value : values) {
            buffer.writeLong(((UnsignedLong)value).longValue());
        }
    }
}
