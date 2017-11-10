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
    public void writeValue(ProtonBuffer buffer, EncoderState state, Boolean value) {
        buffer.writeByte(value.equals(Boolean.TRUE) ? 1 : 0);
    }

    public void writeValue(ProtonBuffer buffer, EncoderState state, boolean value) {
        buffer.writeByte(value ? 1 : 0);
    }

    @Override
    public void writeArray(ProtonBuffer buffer, EncoderState state, Boolean[] value) {
        buffer.writeByte(EncodingCodes.ARRAY32);

        // Array Size -> Total Bytes + Number of elements + Type Code
        int size = (Byte.BYTES * value.length) + Integer.BYTES + Byte.BYTES;

        buffer.writeInt(size);
        buffer.writeInt(value.length);
        buffer.writeByte(EncodingCodes.BOOLEAN);
        for (Boolean bool : value) {
            buffer.writeByte(bool ? 1 : 0);
        }
    }

    public void writeArray(ProtonBuffer buffer, EncoderState state, boolean[] value) {
        buffer.writeByte(EncodingCodes.ARRAY32);

        // Array Size -> Total Bytes + Number of elements + Type Code
        int size = (Byte.BYTES * value.length) + Integer.BYTES + Byte.BYTES;

        buffer.writeInt(size);
        buffer.writeInt(value.length);
        buffer.writeByte(EncodingCodes.BOOLEAN);
        for (boolean bool : value) {
            buffer.writeByte(bool ? 1 : 0);
        }
    }
}
