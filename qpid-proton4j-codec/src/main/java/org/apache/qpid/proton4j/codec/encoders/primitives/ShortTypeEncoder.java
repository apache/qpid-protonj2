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
import org.apache.qpid.proton4j.codec.encoders.PrimitiveTypeEncoder;

/**
 * Encoder of AMQP Short type values to a byte stream.
 */
public class ShortTypeEncoder implements PrimitiveTypeEncoder<Short> {

    @Override
    public Class<Short> getTypeClass() {
        return Short.class;
    }

    @Override
    public void writeType(ProtonBuffer buffer, EncoderState state, Short value) {
        buffer.writeByte(EncodingCodes.SHORT);
        buffer.writeShort(value.shortValue());
    }

    public void writeType(ProtonBuffer buffer, EncoderState state, short value) {
        buffer.writeByte(EncodingCodes.SHORT);
        buffer.writeShort(value);
    }

    @Override
    public void writeRawArray(ProtonBuffer buffer, EncoderState state, Object[] values) {
        buffer.writeByte(EncodingCodes.SHORT);
        for (Object value : values) {
            buffer.writeShort(((Short) value).shortValue());
        }
    }

    public void writeArray(ProtonBuffer buffer, EncoderState state, short[] values) {
        // Write the Array Type encoding code, we don't optimize here.
        buffer.writeByte(EncodingCodes.ARRAY32);

        int startIndex = buffer.getWriteIndex();

        // Reserve space for the size and write the count of list elements.
        buffer.writeInt(0);
        buffer.writeInt(values.length);

        // Write the array elements after writing the array length
        buffer.writeByte(EncodingCodes.SHORT);
        for (short value : values) {
            buffer.writeShort(value);
        }

        // Move back and write the size
        int endIndex = buffer.getWriteIndex();
        long writeSize = endIndex - startIndex - Integer.BYTES;

        if (writeSize > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Cannot encode given array, encoded size to large: " + writeSize);
        }

        buffer.setInt(startIndex, (int) writeSize);
    }
}
