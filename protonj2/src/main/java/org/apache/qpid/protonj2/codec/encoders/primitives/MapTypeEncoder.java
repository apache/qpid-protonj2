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
package org.apache.qpid.protonj2.codec.encoders.primitives;

import java.util.Map;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.codec.EncodeException;
import org.apache.qpid.protonj2.codec.EncoderState;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.TypeEncoder;
import org.apache.qpid.protonj2.codec.encoders.AbstractPrimitiveTypeEncoder;

/**
 * Encoder of AMQP Map type values to a byte stream.
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public final class MapTypeEncoder extends AbstractPrimitiveTypeEncoder<Map> {

    @Override
    public Class<Map> getTypeClass() {
        return Map.class;
    }

    @Override
    public void writeType(ProtonBuffer buffer, EncoderState state, Map value) {
        buffer.writeByte(EncodingCodes.MAP32);
        writeValue(buffer, state, value);
    }

    @Override
    public void writeRawArray(ProtonBuffer buffer, EncoderState state, Object[] values) {
        buffer.writeByte(EncodingCodes.MAP32);
        for (Object value : values) {
            writeValue(buffer, state, (Map) value);
        }
    }

    private void writeValue(ProtonBuffer buffer, EncoderState state, Map value) {
        final int startIndex = buffer.getWriteOffset();

        // Reserve space for the size
        buffer.writeInt(0);

        // Record the count of elements which include both key and value in the count.
        buffer.writeInt(value.size() * 2);

        // Write the list elements and then compute total size written.
        value.forEach((key, entry) -> {
            TypeEncoder keyEncoder = state.getEncoder().getTypeEncoder(key);
            if (keyEncoder == null) {
                throw new EncodeException("Cannot find encoder for type " + key);
            }

            keyEncoder.writeType(buffer, state, key);

            TypeEncoder valueEncoder = state.getEncoder().getTypeEncoder(entry);
            if (valueEncoder == null) {
                throw new EncodeException("Cannot find encoder for type " + entry);
            }

            valueEncoder.writeType(buffer, state, entry);
        });

        // Move back and write the size
        buffer.setInt(startIndex, buffer.getWriteOffset() - startIndex - Integer.BYTES);
    }
}
