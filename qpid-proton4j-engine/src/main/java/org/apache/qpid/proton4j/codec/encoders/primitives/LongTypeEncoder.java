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
import org.apache.qpid.proton4j.codec.encoders.AbstractPrimitiveTypeEncoder;

/**
 * Encoder of AMQP Integer type values to a byte stream.
 */
public final class LongTypeEncoder extends AbstractPrimitiveTypeEncoder<Long> {

    @Override
    public Class<Long> getTypeClass() {
        return Long.class;
    }

    @Override
    public void writeType(ProtonBuffer buffer, EncoderState state, Long value) {
        writeType(buffer, state, value.longValue());
    }

    public void writeType(ProtonBuffer buffer, EncoderState state, long value) {
        if (value >= -128 && value <= 127) {
            buffer.writeByte(EncodingCodes.SMALLLONG);
            buffer.writeByte((byte) value);
        } else {
            buffer.writeByte(EncodingCodes.LONG);
            buffer.writeLong(value);
        }
    }

    @Override
    public void writeRawArray(ProtonBuffer buffer, EncoderState state, Object[] values) {
        buffer.writeByte(EncodingCodes.LONG);
        for (Object value : values) {
            buffer.writeLong(((Long) value).longValue());
        }
    }

    public void writeRawArray(ProtonBuffer buffer, EncoderState state, long[] values) {
        buffer.writeByte(EncodingCodes.LONG);
        for (long value : values) {
            buffer.writeLong(value);
        }
    }

    public void writeArray(ProtonBuffer buffer, EncoderState state, long[] values) {
        if (values.length < 31) {
            writeAsArray8(buffer, state, values);
        } else {
            writeAsArray32(buffer, state, values);
        }
    }

    private void writeAsArray8(ProtonBuffer buffer, EncoderState state, long[] values) {
        buffer.writeByte(EncodingCodes.ARRAY8);

        int startIndex = buffer.getWriteIndex();

        buffer.writeByte(0);
        buffer.writeByte(values.length);

        // Write the array elements after writing the array length
        writeRawArray(buffer, state, values);

        // Move back and write the size
        int endIndex = buffer.getWriteIndex();
        long writeSize = endIndex - startIndex - Byte.BYTES;

        buffer.setByte(startIndex, (byte) writeSize);
    }

    private void writeAsArray32(ProtonBuffer buffer, EncoderState state, long[] values) {
        buffer.writeByte(EncodingCodes.ARRAY32);

        int startIndex = buffer.getWriteIndex();

        buffer.writeInt(0);
        buffer.writeInt(values.length);

        // Write the array elements after writing the array length
        writeRawArray(buffer, state, values);

        // Move back and write the size
        int endIndex = buffer.getWriteIndex();
        long writeSize = endIndex - startIndex - Integer.BYTES;

        if (writeSize > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Cannot encode given array, encoded size to large: " + writeSize);
        }

        buffer.setInt(startIndex, (int) writeSize);
    }
}
