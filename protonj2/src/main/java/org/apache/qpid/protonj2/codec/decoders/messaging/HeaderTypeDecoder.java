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
package org.apache.qpid.protonj2.codec.decoders.messaging;

import java.io.InputStream;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.DecoderState;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.StreamDecoderState;
import org.apache.qpid.protonj2.codec.StreamTypeDecoder;
import org.apache.qpid.protonj2.codec.TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.AbstractDescribedListTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.ProtonStreamUtils;
import org.apache.qpid.protonj2.codec.decoders.primitives.ListTypeDecoder;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.messaging.Header;

/**
 * Decoder of AMQP Header types from a byte stream
 */
public final class HeaderTypeDecoder extends AbstractDescribedListTypeDecoder<Header> {

    private static final int MIN_HEADER_LIST_ENTRIES = 0;
    private static final int MAX_HEADER_LIST_ENTRIES = 5;

    @Override
    public Class<Header> getTypeClass() {
        return Header.class;
    }

    @Override
    public UnsignedLong getDescriptorCode() {
        return Header.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return Header.DESCRIPTOR_SYMBOL;
    }

    @Override
    public Header readValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        return readHeader(buffer, state, checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder));
    }

    @Override
    public Header[] readArrayElements(ProtonBuffer buffer, DecoderState state, int count) throws DecodeException {
        final TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);
        final ListTypeDecoder listDecoder = checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder);

        final Header[] result = new Header[count];
        for (int i = 0; i < count; ++i) {
            result[i] = readHeader(buffer, state, listDecoder);
        }

        return result;
    }

    private Header readHeader(ProtonBuffer buffer, DecoderState state, ListTypeDecoder listDecoder) throws DecodeException {
        final Header header = new Header();

        @SuppressWarnings("unused")
        final int size = listDecoder.readSize(buffer, state);
        final int count = listDecoder.readCount(buffer, state);

        // Don't decode anything if things already look wrong.
        if (count < MIN_HEADER_LIST_ENTRIES) {
            throw new DecodeException("Not enough entries in Header list encoding: " + count);
        }

        if (count > MAX_HEADER_LIST_ENTRIES) {
            throw new DecodeException("To many entries in Header list encoding: " + count);
        }

        for (int index = 0; index < count; ++index) {
            // Peek ahead and see if there is a null in the next slot, if so we don't call
            // the setter for that entry to ensure the returned type reflects the encoded
            // state in the modification entry.
            if (buffer.peekByte() == EncodingCodes.NULL) {
                buffer.advanceReadOffset(1);
                continue;
            }

            switch (index) {
                case 0:
                    header.setDurable(state.getDecoder().readBoolean(buffer, state, false));
                    break;
                case 1:
                    header.setPriority(state.getDecoder().readUnsignedByte(buffer, state, Header.DEFAULT_PRIORITY));
                    break;
                case 2:
                    header.setTimeToLive(state.getDecoder().readUnsignedInteger(buffer, state, 0l));
                    break;
                case 3:
                    header.setFirstAcquirer(state.getDecoder().readBoolean(buffer, state, false));
                    break;
                case 4:
                    header.setDeliveryCount(state.getDecoder().readUnsignedInteger(buffer, state, 0l));
                    break;
            }
        }

        return header;
    }

    @Override
    public Header readValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        final StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        return readHeader(stream, state, checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder));
    }

    @Override
    public Header[] readArrayElements(InputStream stream, StreamDecoderState state, int count) throws DecodeException {
        final StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);
        final ListTypeDecoder listDecoder = checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder);

        final Header[] result = new Header[count];
        for (int i = 0; i < count; ++i) {
            result[i] = readHeader(stream, state, listDecoder);
        }

        return result;
    }

    private Header readHeader(InputStream stream, StreamDecoderState state, ListTypeDecoder listDecoder) throws DecodeException {
        final Header header = new Header();

        @SuppressWarnings("unused")
        final int size = listDecoder.readSize(stream, state);
        final int count = listDecoder.readCount(stream, state);

        // Don't decode anything if things already look wrong.
        if (count < MIN_HEADER_LIST_ENTRIES) {
            throw new DecodeException("Not enough entries in Header list encoding: " + count);
        }

        if (count > MAX_HEADER_LIST_ENTRIES) {
            throw new DecodeException("To many entries in Header list encoding: " + count);
        }

        for (int index = 0; index < count; ++index) {
            // If the stream allows we peek ahead and see if there is a null in the next slot,
            // if so we don't call the setter for that entry to ensure the returned type reflects
            // the encoded state in the modification entry.
            if (stream.markSupported()) {
                stream.mark(1);
                if (ProtonStreamUtils.readByte(stream) == EncodingCodes.NULL) {
                    continue;
                } else {
                    ProtonStreamUtils.reset(stream);
                }
            }

            switch (index) {
                case 0:
                    header.setDurable(state.getDecoder().readBoolean(stream, state, false));
                    break;
                case 1:
                    header.setPriority(state.getDecoder().readUnsignedByte(stream, state, Header.DEFAULT_PRIORITY));
                    break;
                case 2:
                    header.setTimeToLive(state.getDecoder().readUnsignedInteger(stream, state, 0l));
                    break;
                case 3:
                    header.setFirstAcquirer(state.getDecoder().readBoolean(stream, state, false));
                    break;
                case 4:
                    header.setDeliveryCount(state.getDecoder().readUnsignedInteger(stream, state, 0l));
                    break;
            }
        }

        return header;
    }
}
