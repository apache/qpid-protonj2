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
package org.apache.qpid.proton4j.codec.decoders;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.nio.charset.StandardCharsets;

import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.codec.DecoderState;

/**
 * State object used by the Built in Decoder implementation.
 */
public class ProtonDecoderState implements DecoderState {

    private static final int UTF8_MAX_CHARS_PER_BYTES = Math.round(StandardCharsets.UTF_8.newDecoder().maxCharsPerByte());
    private static final int SCRATCH_BUFFER_SIZE = 256;

    private final CharsetDecoder STRING_DECODER = StandardCharsets.UTF_8.newDecoder();
    private final ProtonDecoder decoder;
    private final char[] scratchBuffer = new char[SCRATCH_BUFFER_SIZE];

    private UTF8Decoder stringDecoder;

    public ProtonDecoderState(ProtonDecoder decoder) {
        this.decoder = decoder;
    }

    @Override
    public ProtonDecoder getDecoder() {
        return decoder;
    }

    @Override
    public void reset() {
        // No intermediate state to reset
    }

    public UTF8Decoder getStringDecoder() {
        return stringDecoder;
    }

    public void setStringDecoder(UTF8Decoder stringDecoder) {
        this.stringDecoder = stringDecoder;
    }

    @Override
    public String decodeUTF8(ProtonBuffer buffer, int length) {
        if (stringDecoder == null) {
            return internalDecode(buffer, length, STRING_DECODER, scratchBuffer);
        } else {
            // TODO - We could pass the buffer and length here as well and specify that the
            //        decoder must move the buffer position to consume them if we really trust
            //        that the user will actually do the right thing.
            ProtonBuffer slice = buffer.slice(buffer.getReadIndex(), length);
            buffer.skipBytes(length);
            return stringDecoder.decodeUTF8(slice);
        }
    }

    private static String internalDecode(ProtonBuffer buffer, final int length, CharsetDecoder decoder, char[] scratchBuffer) {
        final int maxExpectedSize = UTF8_MAX_CHARS_PER_BYTES * length;
        final char[] chars = maxExpectedSize <= scratchBuffer.length ? scratchBuffer : new char[maxExpectedSize];
        final int bufferInitialPosition = buffer.getReadIndex();

        int offset;
        for (offset = 0; offset < length; offset++) {
            final byte b = buffer.getByte(bufferInitialPosition + offset);
            if (b < 0) {
                break;
            }
            chars[offset] = (char) b;
        }

        buffer.setReadIndex(bufferInitialPosition + offset);

        if (offset == length) {
            return new String(chars, 0, length);
        } else {
            return internalDecodeUTF8(buffer, length, chars, offset, decoder);
        }
    }

    private static String internalDecodeUTF8(final ProtonBuffer buffer, final int length, final char[] chars, final int offset, final CharsetDecoder decoder) {
        final CharBuffer out = CharBuffer.wrap(chars);
        out.position(offset);

        ProtonBuffer slice = buffer.slice(buffer.getReadIndex(), length - offset);
        ByteBuffer byteBuffer = slice.toByteBuffer();

        try {
            for (;;) {
                CoderResult cr = byteBuffer.hasRemaining() ? decoder.decode(byteBuffer, out, true) : CoderResult.UNDERFLOW;
                if (cr.isUnderflow()) {
                    cr = decoder.flush(out);
                }
                if (cr.isUnderflow()) {
                    break;
                }

                // The char buffer should have been sufficient here but wasn't so we know
                // that there was some encoding issue on the other end.
                cr.throwException();
            }

            out.flip();

            return out.toString();
        } catch (CharacterCodingException e) {
            throw new IllegalArgumentException("Cannot parse encoded UTF8 String");
        } finally {
            decoder.reset();
        }
    }
}
