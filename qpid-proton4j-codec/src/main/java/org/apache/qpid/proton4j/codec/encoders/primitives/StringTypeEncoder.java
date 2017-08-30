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

import org.apache.qpid.proton4j.codec.EncoderState;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.apache.qpid.proton4j.codec.PrimitiveTypeEncoder;

import io.netty.buffer.ByteBuf;

/**
 * Encoder of AMQP String type values to a byte stream.
 */
public class StringTypeEncoder implements PrimitiveTypeEncoder<String> {

    @Override
    public Class<String> getTypeClass() {
        return String.class;
    }

    @Override
    public void writeType(ByteBuf buffer, EncoderState state, String value) {
        write(buffer, state, value, true);
    }

    @Override
    public void writeValue(ByteBuf buffer, EncoderState state, String value) {
        write(buffer, state, value, false);
    }

    private void write(ByteBuf buffer, EncoderState state, String value, boolean writeEncoding) {
        int startIndex = buffer.writerIndex() + 1;

        int fieldWidth = 1;

        // We are pessimistic and assume larger strings will encode
        // at the max 4 bytes per character instead of calculating
        if (value.length() > 64) {
            fieldWidth = 4;
        }

        // TODO - We can have a profile where we actually do this but the
        //        overall savings of bytes written doesn't seem worth it.
        //
        //          int encodedSize = calculateUTF8Length(value);
        //          if (encodedSize > 255) {
        //              fieldWidth = 4;
        //          }

        // Reserve space for the size
        if (fieldWidth == 1) {
            if (writeEncoding) {
                buffer.writeByte(EncodingCodes.STR8);
            }
            buffer.writeByte(0);
        } else {
            if (writeEncoding) {
                buffer.writeByte(EncodingCodes.STR32);
            }
            buffer.writeInt(0);
        }

        // Write the full string value
        writeString(buffer, state, value);

        // Move back and write the size
        int endIndex = buffer.writerIndex();
        if (fieldWidth == 1) {
            buffer.setByte(startIndex, endIndex - startIndex - fieldWidth);
        } else {
            buffer.setInt(startIndex, endIndex - startIndex - fieldWidth);
        }
    }

    @Override
    public void writeArray(ByteBuf buffer, EncoderState state, String[] values) {
        buffer.writeByte(EncodingCodes.ARRAY32);

        // Array Size -> Total Bytes + Number of elements + Type Code
        //
        // String types are variable sized values so we write the payload
        // and then we write the size using the result.
        int startIndex = buffer.writerIndex();

        // Reserve space for the size
        buffer.writeInt(0);

        buffer.writeInt(values.length);
        buffer.writeByte(EncodingCodes.STR32);
        for (String value : values) {
            // Reserve space for the size
            buffer.writeInt(0);

            int stringStart = buffer.writerIndex();

            // Write the full string value
            writeString(buffer, state, value);

            // Move back and write the string size
            int stringEnd = buffer.writerIndex();
            buffer.setInt(startIndex, stringEnd - stringStart);
        }

        // Move back and write the size
        int endIndex = buffer.writerIndex();

        long size = endIndex - startIndex;
        if (size > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Cannot encode given Symbol array, encoded size to large: " + size);
        }

        buffer.setInt(startIndex, (int) size);
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

    private void writeString(ByteBuf buffer, EncoderState state, String value) {
        final int length = value.length();
        int c;

        for (int i = 0; i < length; i++) {
            c = value.charAt(i);
            if ((c & 0xFF80) == 0) {
                /* U+0000..U+007F */
                buffer.writeByte((byte) c);
            } else if ((c & 0xF800) == 0) {
                /* U+0080..U+07FF */
                buffer.writeByte((byte)(0xC0 | ((c >> 6) & 0x1F)));
                buffer.writeByte((byte)(0x80 | (c & 0x3F)));
            } else if ((c & 0xD800) != 0xD800 || (c > 0xDBFF)) {
                /* U+0800..U+FFFF - excluding surrogate pairs */
                buffer.writeByte((byte)(0xE0 | ((c >> 12) & 0x0F)));
                buffer.writeByte((byte)(0x80 | ((c >> 6) & 0x3F)));
                buffer.writeByte((byte)(0x80 | (c & 0x3F)));
            } else {
                int low;

                if ((++i == length) || ((low = value.charAt(i)) & 0xDC00) != 0xDC00) {
                    throw new IllegalArgumentException("String contains invalid Unicode code points");
                }

                c = 0x010000 + ((c & 0x03FF) << 10) + (low & 0x03FF);

                buffer.writeByte((byte)(0xF0 | ((c >> 18) & 0x07)));
                buffer.writeByte((byte)(0x80 | ((c >> 12) & 0x3F)));
                buffer.writeByte((byte)(0x80 | ((c >> 6) & 0x3F)));
                buffer.writeByte((byte)(0x80 | (c & 0x3F)));
            }
        }
    }
}
