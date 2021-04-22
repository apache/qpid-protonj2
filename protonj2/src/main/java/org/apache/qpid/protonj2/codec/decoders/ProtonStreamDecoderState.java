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
package org.apache.qpid.protonj2.codec.decoders;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.nio.charset.StandardCharsets;

import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.Decoder;
import org.apache.qpid.protonj2.codec.StreamDecoderState;

/**
 * State object used by the Built in Decoder implementation.
 */
public final class ProtonStreamDecoderState implements StreamDecoderState {

    private static final int MAX_CHAR_BUFFER_CAHCE_SIZE = 100;

    private final CharsetDecoder STRING_DECODER = StandardCharsets.UTF_8.newDecoder();
    private final ProtonStreamDecoder decoder;
    private final char[] decodeCache = new char[MAX_CHAR_BUFFER_CAHCE_SIZE];

    private UTF8StreamDecoder stringDecoder;

    /**
     * Create a new {@link StreamDecoderState} instance that is joined forever to the given {@link Decoder}.
     * @param decoder
     */
    public ProtonStreamDecoderState(ProtonStreamDecoder decoder) {
        this.decoder = decoder;
    }

    @Override
    public ProtonStreamDecoder getDecoder() {
        return decoder;
    }

    @Override
    public ProtonStreamDecoderState reset() {
        // No intermediate state to reset
        return this;
    }

    /**
     * @return the currently set custom UTF-8 {@link String} decoder or null if non set.
     */
    public UTF8StreamDecoder getStringDecoder() {
        return stringDecoder;
    }

    /**
     * Sets a custom UTF-8 {@link String} decoder that will be used for all {@link String} decoding done
     * from the encoder associated with this {@link StreamDecoderState} instance.  If no decoder is registered
     * then the implementation uses its own decoding algorithm.
     *
     * @param stringDecoder
     * 		a custom {@link UTF8Decoder} that will be used for all {@link String} decoding.
     */
    public void setStringDecoder(UTF8StreamDecoder stringDecoder) {
        this.stringDecoder = stringDecoder;
    }

    @Override
    public String decodeUTF8(InputStream stream, int length) throws DecodeException {
        try {
            if (stringDecoder == null) {
                return internalDecode(stream, length, STRING_DECODER, length > MAX_CHAR_BUFFER_CAHCE_SIZE ? new char[length] : decodeCache);
            } else {
                return stringDecoder.decodeUTF8(stream);
            }
        } catch (Exception ex) {
            throw new DecodeException("Cannot parse encoded UTF8 String", ex);
        }
    }

    private static String internalDecode(InputStream stream, final int length, CharsetDecoder decoder, char[] scratch) throws IOException {
        int offset;
        int lastRead = 0;

        for (offset = 0; offset < length; offset++) {
            lastRead = stream.read();
            if (lastRead < 0) {
                throw new EOFException("Reached end of stream before decoding the full String content");
            } else if (lastRead > 127) {
                break;
            }
            scratch[offset] = (char) lastRead;
        }

        if (offset == length) {
            return new String(scratch, 0, length);
        } else {
            return internalDecodeUTF8(stream, length, scratch, (byte) lastRead, offset, decoder);
        }
    }

    private static String internalDecodeUTF8(final InputStream stream, final int length, final char[] chars, final byte stoppageByte, final int offset, final CharsetDecoder decoder) throws IOException {
        final CharBuffer out = CharBuffer.wrap(chars);
        out.position(offset);

        // Create a buffer from the remaining portion of the buffer and then use the decoder to complete the work
        // remember to move the main buffer position to consume the data processed.
        final byte[] trailingBytes = new byte[length - offset];
        trailingBytes[0] = stoppageByte;
        stream.read(trailingBytes, 1, trailingBytes.length - 1);
        ByteBuffer byteBuffer = ByteBuffer.wrap(trailingBytes);

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
