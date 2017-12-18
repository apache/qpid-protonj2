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
 * Encoder of AMQP Boolean True types to a byte stream.
 */
public class BooleanTypeEncoder implements PrimitiveTypeEncoder<Boolean> {

    @Override
    public Class<Boolean> getTypeClass() {
        return Boolean.class;
    }

    @Override
    public void writeType(ProtonBuffer buffer, EncoderState state, Boolean value) {
        buffer.writeByte(value == Boolean.TRUE ? EncodingCodes.BOOLEAN_TRUE : EncodingCodes.BOOLEAN_FALSE);
    }

    public void writeType(ProtonBuffer buffer, EncoderState state, boolean value) {
        buffer.writeByte(value == true ? EncodingCodes.BOOLEAN_TRUE : EncodingCodes.BOOLEAN_FALSE);
    }

    @Override
    public void writeRawArray(ProtonBuffer buffer, EncoderState state, Object[] values) {
        // Write the array elements after writing the array length
        buffer.writeByte(EncodingCodes.BOOLEAN);
        for (Object bool : values) {
            buffer.writeByte((Boolean) bool ? 1 : 0);
        }
    }

    public void writeArray(ProtonBuffer buffer, EncoderState state, boolean[] value) {
        // Write the Array Type encoding code, we don't optimize here.
        buffer.writeByte(EncodingCodes.ARRAY32);

        int startIndex = buffer.getWriteIndex();

        // Reserve space for the size and write the count of list elements.
        buffer.writeInt(0);
        buffer.writeInt(value.length);

        // Write the array elements after writing the array length
        buffer.writeByte(EncodingCodes.BOOLEAN);
        for (boolean bool : value) {
            buffer.writeByte(bool ? 1 : 0);
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
