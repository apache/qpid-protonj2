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
import org.apache.qpid.proton4j.codec.DecodeException;
import org.apache.qpid.proton4j.codec.DecoderState;

/**
 * State object used by the Built in Decoder implementation.
 */
public final class ProtonDecoderState implements DecoderState {

    private static final int MAX_CHAR_BUFFER_CAHCE_SIZE = 100;

    private final CharsetDecoder STRING_DECODER = StandardCharsets.UTF_8.newDecoder();
    private final ProtonDecoder decoder;
    private final char[] decodeCache = new char[MAX_CHAR_BUFFER_CAHCE_SIZE];

    private UTF8Decoder stringDecoder;

    public ProtonDecoderState(ProtonDecoder decoder) {
        this.decoder = decoder;
    }

    @Override
    public ProtonDecoder getDecoder() {
        return decoder;
    }

    @Override
    public ProtonDecoderState reset() {
        // No intermediate state to reset
        return this;
    }

    public UTF8Decoder getStringDecoder() {
        return stringDecoder;
    }

    public void setStringDecoder(UTF8Decoder stringDecoder) {
        this.stringDecoder = stringDecoder;
    }

    @Override
    public String decodeUTF8(ProtonBuffer buffer, int length) throws DecodeException {
        if (stringDecoder == null) {
            return internalDecode(buffer, length, STRING_DECODER, length > MAX_CHAR_BUFFER_CAHCE_SIZE ? new char[length] : decodeCache);
        } else {
            // TODO - We could pass the buffer and length here as well and specify that the
            //        decoder must move the buffer position to consume them if we really trust
            //        that the user will actually do the right thing.
            ProtonBuffer slice = buffer.slice(buffer.getReadIndex(), length);
            buffer.skipBytes(length);
            try {
                return stringDecoder.decodeUTF8(slice);
            } catch (Exception ex) {
                throw new DecodeException("Cannot parse encoded UTF8 String", ex);
            }
        }
    }

    private static String internalDecode(ProtonBuffer buffer, final int length, CharsetDecoder decoder, char[] scratch) {
        final int bufferInitialPosition = buffer.getReadIndex();

        int offset;
        for (offset = 0; offset < length; offset++) {
            final byte b = buffer.getByte(bufferInitialPosition + offset);
            if (b < 0) {
                break;
            }
            scratch[offset] = (char) b;
        }

        buffer.setReadIndex(bufferInitialPosition + offset);

        if (offset == length) {
            return new String(scratch, 0, length);
        } else {
            return internalDecodeUTF8(buffer, length, scratch, offset, decoder);
        }
    }

    private static String internalDecodeUTF8(final ProtonBuffer buffer, final int length, final char[] chars, final int offset, final CharsetDecoder decoder) {
        final CharBuffer out = CharBuffer.wrap(chars);
        out.position(offset);

        // Create a buffer from the remaining portion of the buffer and then use the decoder to complete the work
        // remember to move the main buffer position to consume the data processed.
        ProtonBuffer slice = buffer.slice(buffer.getReadIndex(), length - offset);
        buffer.setReadIndex(buffer.getReadIndex() + slice.getReadableBytes());
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

            return out.flip().toString();
        } catch (CharacterCodingException e) {
            throw new DecodeException("Cannot parse encoded UTF8 String", e);
        } finally {
            decoder.reset();
        }
    }
}
