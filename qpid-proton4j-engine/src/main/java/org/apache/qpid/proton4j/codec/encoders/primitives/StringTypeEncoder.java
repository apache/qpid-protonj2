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
 * Encoder of AMQP String type values to a byte stream.
 */
public final class StringTypeEncoder extends AbstractPrimitiveTypeEncoder<String> {

    @Override
    public Class<String> getTypeClass() {
        return String.class;
    }

    @Override
    public void writeType(ProtonBuffer buffer, EncoderState state, String value) {
        // We are pessimistic and assume larger strings will encode
        // at the max 4 bytes per character instead of calculating
        if (value.length() > 64) {
            writeString(buffer, state, value);
        } else {
            writeSmallString(buffer, state, value);
        }
    }

    private static void writeSmallString(ProtonBuffer buffer, EncoderState state, String value) {
        buffer.writeByte(EncodingCodes.STR8);
        buffer.writeByte(0);

        int startIndex = buffer.getWriteIndex();

        // Write the full string value
        state.encodeUTF8(buffer, value);

        // Move back and write the size into the size slot
        buffer.setByte(startIndex - Byte.BYTES, buffer.getWriteIndex() - startIndex);
    }

    private static void writeString(ProtonBuffer buffer, EncoderState state, String value) {
        buffer.writeByte(EncodingCodes.STR32);
        buffer.writeInt(0);

        int startIndex = buffer.getWriteIndex();

        // Write the full string value
        state.encodeUTF8(buffer, value);

        // Move back and write the size into the size slot
        buffer.setInt(startIndex - Integer.BYTES, buffer.getWriteIndex() - startIndex);
    }

    @Override
    public void writeRawArray(ProtonBuffer buffer, EncoderState state, Object[] values) {
        buffer.writeByte(EncodingCodes.STR32);
        for (Object value : values) {
            // Reserve space for the size
            buffer.writeInt(0);

            int stringStart = buffer.getWriteIndex();

            // Write the full string value
            state.encodeUTF8(buffer, (CharSequence) value);

            // Move back and write the string size
            buffer.setInt(stringStart - Integer.BYTES, buffer.getWriteIndex() - stringStart);
        }
    }
}
