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
package org.apache.qpid.proton4j.codec.encoders;

import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.codec.EncoderState;

/**
 * State object used by the Built in Encoder implementation.
 */
public class ProtonEncoderState implements EncoderState {

    private final ProtonEncoder encoder;

    private UTF8Encoder utf8Encoder;

    public ProtonEncoderState(ProtonEncoder encoder) {
        this.encoder = encoder;
    }

    @Override
    public ProtonEncoder getEncoder() {
        return this.encoder;
    }

    public UTF8Encoder getUTF8Encoder() {
        return utf8Encoder;
    }

    public void setUTF8Encoder(UTF8Encoder utf8Encoder) {
        this.utf8Encoder = utf8Encoder;
    }

    @Override
    public void reset() {
        // No intermediate state to reset
    }

    @Override
    public ProtonBuffer encodeUTF8(ProtonBuffer buffer, CharSequence sequence) {
        if (utf8Encoder == null) {
            encodeUTF8Sequence(buffer, sequence);
        } else {
            utf8Encoder.encodeUTF8(buffer, sequence);
        }

        return buffer;
    }

    private static void encodeUTF8Sequence(ProtonBuffer buffer, CharSequence sequence) {
        final int length = sequence.length();

        int position = buffer.getWriteIndex();
        int index = 0;
        int ch = 0;

        // We need to ensure that we have enough space for the largest encoding possible for this
        // string value.  We could compute the UTF8 length if we wanted a precise sizing.
        buffer.ensureWritable(length * 4);

        // ASCII Optimized path U+0000..U+007F
        for (; index < length && (ch = sequence.charAt(index)) < 0x80; ++index) {
            buffer.setByte(position++, (byte) ch);
        }

        if (index < length) {
            // Non-ASCII path
            position = extendedEncodeUTF8Sequence(buffer, sequence, index, length, position);
        }

        buffer.setWriteIndex(position);
    }

    private static int extendedEncodeUTF8Sequence(ProtonBuffer buffer, CharSequence value, int index, int remaining, int position) {
        for (int i = index; i < remaining; i++) {
            int c = value.charAt(i);
            if ((c & 0xFF80) == 0) {
                // U+0000..U+007F
                buffer.setByte(position++, (byte) c);
            } else if ((c & 0xF800) == 0) {
                // U+0080..U+07FF
                buffer.setByte(position++, (byte)(0xC0 | ((c >> 6) & 0x1F)));
                buffer.setByte(position++, (byte)(0x80 | (c & 0x3F)));
            } else if ((c & 0xD800) != 0xD800 || (c > 0xDBFF)) {
                // U+0800..U+FFFF - excluding surrogate pairs
                buffer.setByte(position++, (byte)(0xE0 | ((c >> 12) & 0x0F)));
                buffer.setByte(position++, (byte)(0x80 | ((c >> 6) & 0x3F)));
                buffer.setByte(position++, (byte)(0x80 | (c & 0x3F)));
            } else {
                int low;

                if ((++i == remaining) || ((low = value.charAt(i)) & 0xDC00) != 0xDC00) {
                    throw new IllegalArgumentException("String contains invalid Unicode code points");
                }

                c = 0x010000 + ((c & 0x03FF) << 10) + (low & 0x03FF);

                buffer.setByte(position++, (byte)(0xF0 | ((c >> 18) & 0x07)));
                buffer.setByte(position++, (byte)(0x80 | ((c >> 12) & 0x3F)));
                buffer.setByte(position++, (byte)(0x80 | ((c >> 6) & 0x3F)));
                buffer.setByte(position++, (byte)(0x80 | (c & 0x3F)));
            }
        }

        return position;
    }
}
