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
public class StringTypeEncoder extends AbstractPrimitiveTypeEncoder<String> {

    @Override
    public Class<String> getTypeClass() {
        return String.class;
    }

    @Override
    public void writeType(ProtonBuffer buffer, EncoderState state, String value) {
        int startIndex = buffer.getWriteIndex() + 1;

        int fieldWidth = 1;

        // We are pessimistic and assume larger strings will encode
        // at the max 4 bytes per character instead of calculating
        if (value.length() > 64) {
            fieldWidth = 4;
        }

        // Reserve space for the size
        if (fieldWidth == 1) {
            buffer.writeByte(EncodingCodes.STR8);
            buffer.writeByte(0);
        } else {
            buffer.writeByte(EncodingCodes.STR32);
            buffer.writeInt(0);
        }

        // Write the full string value
        writeString(buffer, state, value);

        // Move back and write the size
        int endIndex = buffer.getWriteIndex();
        if (fieldWidth == 1) {
            buffer.setByte(startIndex, endIndex - startIndex - fieldWidth);
        } else {
            buffer.setInt(startIndex, endIndex - startIndex - fieldWidth);
        }
    }

    @Override
    public void writeRawArray(ProtonBuffer buffer, EncoderState state, Object[] values) {
        buffer.writeByte(EncodingCodes.STR32);
        for (Object value : values) {
            // Reserve space for the size
            buffer.writeInt(0);

            int stringStart = buffer.getWriteIndex();

            // Write the full string value
            writeString(buffer, state, (String) value);

            // Move back and write the string size
            buffer.setInt(stringStart - Integer.BYTES, buffer.getWriteIndex() - stringStart);
        }
    }

    // TODO - Can be used later if we have an optimized for space profile.
    @SuppressWarnings("unused")
    private int calculateUTF8Length(final String s) {
        int encodedSize = s.length();
        final int length = encodedSize;

        for (int i = 0; i < length; i++) {
            int c = s.charAt(i);

            // U+0080..
            if ((c & 0xFF80) != 0) {
                encodedSize++;

                // U+0800..
                if(((c & 0xF800) != 0)) {
                    encodedSize++;

                    // surrogate pairs should always combine to create a code point
                    // with a 4 octet representation
                    if ((c & 0xD800) == 0xD800 && c < 0xDC00) {
                        i++;
                    }
                }
            }
        }

        return encodedSize;
    }

    // TODO - We should probably have a string encoder in the state object and let the
    //        implementations override this or have a String write method in the buffer class
    private void writeString(ProtonBuffer buffer, EncoderState state, String value) {
        final int length = value.length();
        int c;

        int position = buffer.getWriteIndex();

        // We need to ensure that we have enough space for the largest encoding possible for this
        // string value.  We could compute the UTF8 length.
        buffer.ensureWritable(value.length() * 4);

        for (int i = 0; i < length; i++) {
            c = value.charAt(i);
            if ((c & 0xFF80) == 0) {
                /* U+0000..U+007F */
                buffer.setByte(position++, (byte) c);
            } else if ((c & 0xF800) == 0) {
                /* U+0080..U+07FF */
                buffer.setByte(position++, (byte)(0xC0 | ((c >> 6) & 0x1F)));
                buffer.setByte(position++, (byte)(0x80 | (c & 0x3F)));
            } else if ((c & 0xD800) != 0xD800 || (c > 0xDBFF)) {
                /* U+0800..U+FFFF - excluding surrogate pairs */
                buffer.setByte(position++, (byte)(0xE0 | ((c >> 12) & 0x0F)));
                buffer.setByte(position++, (byte)(0x80 | ((c >> 6) & 0x3F)));
                buffer.setByte(position++, (byte)(0x80 | (c & 0x3F)));
            } else {
                int low;

                if ((++i == length) || ((low = value.charAt(i)) & 0xDC00) != 0xDC00) {
                    throw new IllegalArgumentException("String contains invalid Unicode code points");
                }

                c = 0x010000 + ((c & 0x03FF) << 10) + (low & 0x03FF);

                buffer.setByte(position++, (byte)(0xF0 | ((c >> 18) & 0x07)));
                buffer.setByte(position++, (byte)(0x80 | ((c >> 12) & 0x3F)));
                buffer.setByte(position++, (byte)(0x80 | ((c >> 6) & 0x3F)));
                buffer.setByte(position++, (byte)(0x80 | (c & 0x3F)));
            }
        }

        buffer.setWriteIndex(position);
    }
}
