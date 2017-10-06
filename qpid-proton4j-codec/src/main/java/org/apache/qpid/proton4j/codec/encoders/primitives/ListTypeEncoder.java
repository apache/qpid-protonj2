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

import java.util.List;

import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.codec.EncoderState;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.apache.qpid.proton4j.codec.PrimitiveTypeEncoder;
import org.apache.qpid.proton4j.codec.TypeEncoder;

/**
 * Encoder of AMQP List type values to a byte stream.
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class ListTypeEncoder implements PrimitiveTypeEncoder<List> {

    @Override
    public Class<List> getTypeClass() {
        return List.class;
    }

    @Override
    public void writeType(ProtonBuffer buffer, EncoderState state, List value) {
        buffer.writeByte(EncodingCodes.LIST32);
        writeValue(buffer, state, value);
    }

    @Override
    public void writeValue(ProtonBuffer buffer, EncoderState state, List value) {
        int startIndex = buffer.getWriteIndex();

        // Reserve space for the size
        buffer.writeInt(0);

        // Write the count of list elements.
        buffer.writeInt(value.size());

        TypeEncoder encoder = null;

        // Write the list elements and then compute total size written, try not to lookup
        // encoders when the types in the list all match.
        for (Object entry : value) {
            if (encoder == null || !encoder.getTypeClass().equals(entry.getClass())) {
                encoder = state.getEncoder().getTypeEncoder(entry);
            }

            if (encoder == null) {
                throw new IllegalArgumentException("Cannot find encoder for type " + entry);
            }

            encoder.writeType(buffer, state, entry);
        };

        // Move back and write the size
        int endIndex = buffer.getWriteIndex();
        buffer.setInt(startIndex, endIndex - startIndex - 4);
    }

    @Override
    public void writeArray(ProtonBuffer buffer, EncoderState state, List[] values) {
        buffer.writeByte(EncodingCodes.ARRAY32);

        // Array Size -> Total Bytes + Number of elements + Type Code
        //
        // List types are variable sized values so we write the payload
        // and then we write the size using the result.
        int startIndex = buffer.getWriteIndex();

        // Reserve space for the size
        buffer.writeInt(0);

        buffer.writeInt(values.length);
        buffer.writeByte(EncodingCodes.LIST32);
        for (List value : values) {
            writeValue(buffer, state, value);
        }

        // Move back and write the size
        int endIndex = buffer.getWriteIndex();

        long size = endIndex - startIndex;
        if (size > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Cannot encode given List array, encoded size to large: " + size);
        }

        buffer.setInt(startIndex, (int) size);
    }
}
