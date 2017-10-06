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

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.codec.EncoderState;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.apache.qpid.proton4j.codec.PrimitiveTypeEncoder;

/**
 * Encoder of AMQP Binary type values to a byte stream.
 */
public class BinaryTypeEncoder implements PrimitiveTypeEncoder<Binary> {

    @Override
    public Class<Binary> getTypeClass() {
        return Binary.class;
    }

    @Override
    public void writeType(ProtonBuffer buffer, EncoderState state, Binary value) {
        if (value.getLength() > 255) {
            buffer.writeByte(EncodingCodes.VBIN32);
            buffer.writeInt(value.getLength());
            buffer.writeBytes(value.getArray(), value.getArrayOffset(), value.getLength());
        } else {
            buffer.writeByte(EncodingCodes.VBIN8);
            buffer.writeByte((byte) value.getLength());
            buffer.writeBytes(value.getArray(), value.getArrayOffset(), value.getLength());
        }
    }

    @Override
    public void writeValue(ProtonBuffer buffer, EncoderState state, Binary value) {
        if (value.getLength() > 255) {
            buffer.writeInt(value.getLength());
            buffer.writeBytes(value.getArray(), value.getArrayOffset(), value.getLength());
        } else {
            buffer.writeByte((byte) value.getLength());
            buffer.writeBytes(value.getArray(), value.getArrayOffset(), value.getLength());
        }
    }

    @Override
    public void writeArray(ProtonBuffer buffer, EncoderState state, Binary[] values) {
        buffer.writeByte(EncodingCodes.ARRAY32);

        // Array Size -> Total Bytes + Number of elements + Type Code
        //
        // Binary types are variable sized values so we write the payload
        // and then we write the size using the result.
        int startIndex = buffer.getWriteIndex();

        // Reserve space for the size
        buffer.writeInt(0);

        buffer.writeInt(values.length);
        buffer.writeByte(EncodingCodes.VBIN32);
        for (Binary value : values) {
            buffer.writeInt(value.getLength());
            buffer.writeBytes(value.getArray(), value.getArrayOffset(), value.getLength());
        }

        // Move back and write the size
        int endIndex = buffer.getWriteIndex();

        long size = endIndex - startIndex;
        if (size > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Cannot encode given Symbol array, encoded size to large: " + size);
        }

        buffer.setInt(startIndex, (int) size);
    }
}
