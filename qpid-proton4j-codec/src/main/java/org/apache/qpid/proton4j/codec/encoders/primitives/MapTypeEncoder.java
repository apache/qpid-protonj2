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

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.codec.EncoderState;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.apache.qpid.proton4j.codec.PrimitiveTypeEncoder;
import org.apache.qpid.proton4j.codec.TypeEncoder;

/**
 * Encoder of AMQP Map type values to a byte stream.
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class MapTypeEncoder implements PrimitiveTypeEncoder<Map> {

    @Override
    public Class<Map> getTypeClass() {
        return Map.class;
    }

    @Override
    public void writeType(ProtonBuffer buffer, EncoderState state, Map value) {
        buffer.writeByte(EncodingCodes.MAP32);
        writeValue(buffer, state, value);
    }

    private void writeValue(ProtonBuffer buffer, EncoderState state, Map value) {
        int startIndex = buffer.getWriteIndex();

        // Reserve space for the size
        buffer.writeInt(0);

        // Record the count of elements which include both key and value in the count.
        buffer.writeInt(value.size() * 2);

        // Write the list elements and then compute total size written.
        Set<Map.Entry> entries = value.entrySet();
        for (Entry entry : entries) {
            Object entryKey = entry.getKey();
            Object entryValue = entry.getValue();

            TypeEncoder keyEncoder = state.getEncoder().getTypeEncoder(entryKey);
            keyEncoder.writeType(buffer, state, entryKey);

            TypeEncoder valueEncoder = state.getEncoder().getTypeEncoder(entryValue);
            valueEncoder.writeType(buffer, state, entryValue);
        }

        // Move back and write the size
        int endIndex = buffer.getWriteIndex();
        buffer.setInt(startIndex, endIndex - startIndex - 4);
    }

    @Override
    public void writeArray(ProtonBuffer buffer, EncoderState state, Map[] values) {
        buffer.writeByte(EncodingCodes.ARRAY32);

        // Array Size -> Total Bytes + Number of elements + Type Code
        //
        // List types are variable sized values so we write the payload
        // and then we write the size using the result.
        int startIndex = buffer.getWriteIndex();

        // Reserve space for the size
        buffer.writeInt(0);

        buffer.writeInt(values.length);
        buffer.writeByte(EncodingCodes.MAP32);
        for (Map value : values) {
            writeValue(buffer, state, value);
        }

        // Move back and write the size
        int endIndex = buffer.getWriteIndex();

        long size = endIndex - startIndex;
        if (size > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Cannot encode given Map array, encoded size to large: " + size);
        }

        buffer.setInt(startIndex, (int) size);
    }
}
