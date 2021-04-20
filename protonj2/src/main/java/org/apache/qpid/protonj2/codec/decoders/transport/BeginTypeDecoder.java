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
package org.apache.qpid.protonj2.codec.decoders.transport;

import java.io.InputStream;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.DecoderState;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.StreamDecoderState;
import org.apache.qpid.protonj2.codec.StreamTypeDecoder;
import org.apache.qpid.protonj2.codec.TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.AbstractDescribedTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.ProtonStreamUtils;
import org.apache.qpid.protonj2.codec.decoders.primitives.ListTypeDecoder;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.transport.Begin;

/**
 * Decoder of AMQP Begin type values from a byte stream
 */
public final class BeginTypeDecoder extends AbstractDescribedTypeDecoder<Begin> {

    private static final int MIN_BEGIN_LIST_ENTRIES = 4;
    private static final int MAX_BEGIN_LIST_ENTRIES = 8;

    @Override
    public Class<Begin> getTypeClass() {
        return Begin.class;
    }

    @Override
    public UnsignedLong getDescriptorCode() {
        return Begin.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return Begin.DESCRIPTOR_SYMBOL;
    }

    @Override
    public Begin readValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        return readBegin(buffer, state, checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder));
    }

    @Override
    public Begin[] readArrayElements(ProtonBuffer buffer, DecoderState state, int count) throws DecodeException {
        final TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        final Begin[] result = new Begin[count];
        for (int i = 0; i < count; ++i) {
            result[i] = readBegin(buffer, state, checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder));
        }

        return result;
    }

    @Override
    public void skipValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        decoder.skipValue(buffer, state);
    }

    private Begin readBegin(ProtonBuffer buffer, DecoderState state, ListTypeDecoder listDecoder) throws DecodeException {
        final Begin begin = new Begin();

        @SuppressWarnings("unused")
        final int size = listDecoder.readSize(buffer);
        final int count = listDecoder.readCount(buffer);

        if (count < MIN_BEGIN_LIST_ENTRIES) {
            throw new DecodeException(errorForMissingRequiredFields(count));
        }

        if (count > MAX_BEGIN_LIST_ENTRIES) {
            throw new DecodeException("To many entries in Begin list encoding: " + count);
        }

        for (int index = 0; index < count; ++index) {
            // Peek ahead and see if there is a null in the next slot, if so we don't call
            // the setter for that entry to ensure the returned type reflects the encoded
            // state in the modification entry.
            final boolean nullValue = buffer.getByte(buffer.getReadIndex()) == EncodingCodes.NULL;
            if (nullValue) {
                // Ensure mandatory fields are set
                if (index > 0 && index < MIN_BEGIN_LIST_ENTRIES) {
                    throw new DecodeException(errorForMissingRequiredFields(index));
                }

                buffer.readByte();
                continue;
            }

            switch (index) {
                case 0:
                    begin.setRemoteChannel(state.getDecoder().readUnsignedShort(buffer, state, 0));
                    break;
                case 1:
                    begin.setNextOutgoingId(state.getDecoder().readUnsignedInteger(buffer, state, 0l));
                    break;
                case 2:
                    begin.setIncomingWindow(state.getDecoder().readUnsignedInteger(buffer, state, 0l));
                    break;
                case 3:
                    begin.setOutgoingWindow(state.getDecoder().readUnsignedInteger(buffer, state, 0l));
                    break;
                case 4:
                    begin.setHandleMax(state.getDecoder().readUnsignedInteger(buffer, state, 0l));
                    break;
                case 5:
                    begin.setOfferedCapabilities(state.getDecoder().readMultiple(buffer, state, Symbol.class));
                    break;
                case 6:
                    begin.setDesiredCapabilities(state.getDecoder().readMultiple(buffer, state, Symbol.class));
                    break;
                case 7:
                    begin.setProperties(state.getDecoder().readMap(buffer, state));
                    break;
            }
        }

        return begin;
    }

    @Override
    public Begin readValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        final StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        return readBegin(stream, state, checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder));
    }

    @Override
    public Begin[] readArrayElements(InputStream stream, StreamDecoderState state, int count) throws DecodeException {
        final StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        final Begin[] result = new Begin[count];
        for (int i = 0; i < count; ++i) {
            result[i] = readBegin(stream, state, checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder));
        }

        return result;
    }

    @Override
    public void skipValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        final StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        decoder.skipValue(stream, state);
    }

    private Begin readBegin(InputStream stream, StreamDecoderState state, ListTypeDecoder listDecoder) throws DecodeException {
        final Begin begin = new Begin();

        @SuppressWarnings("unused")
        final int size = listDecoder.readSize(stream);
        final int count = listDecoder.readCount(stream);

        if (count < MIN_BEGIN_LIST_ENTRIES) {
            throw new DecodeException(errorForMissingRequiredFields(count));
        }

        if (count > MAX_BEGIN_LIST_ENTRIES) {
            throw new DecodeException("To many entries in Begin list encoding: " + count);
        }

        for (int index = 0; index < count; ++index) {
            // If the stream allows we peek ahead and see if there is a null in the next slot,
            // if so we don't call the setter for that entry to ensure the returned type reflects
            // the encoded state in the modification entry.
            if (stream.markSupported()) {
                stream.mark(1);
                final boolean nullValue = ProtonStreamUtils.readByte(stream) == EncodingCodes.NULL;
                if (nullValue) {
                    // Ensure mandatory fields are set
                    if (index > 0 && index < MIN_BEGIN_LIST_ENTRIES) {
                        throw new DecodeException(errorForMissingRequiredFields(index));
                    }

                    continue;
                } else {
                    ProtonStreamUtils.reset(stream);
                }
            }

            switch (index) {
                case 0:
                    begin.setRemoteChannel(state.getDecoder().readUnsignedShort(stream, state, 0));
                    break;
                case 1:
                    begin.setNextOutgoingId(state.getDecoder().readUnsignedInteger(stream, state, 0l));
                    break;
                case 2:
                    begin.setIncomingWindow(state.getDecoder().readUnsignedInteger(stream, state, 0l));
                    break;
                case 3:
                    begin.setOutgoingWindow(state.getDecoder().readUnsignedInteger(stream, state, 0l));
                    break;
                case 4:
                    begin.setHandleMax(state.getDecoder().readUnsignedInteger(stream, state, 0l));
                    break;
                case 5:
                    begin.setOfferedCapabilities(state.getDecoder().readMultiple(stream, state, Symbol.class));
                    break;
                case 6:
                    begin.setDesiredCapabilities(state.getDecoder().readMultiple(stream, state, Symbol.class));
                    break;
                case 7:
                    begin.setProperties(state.getDecoder().readMap(stream, state));
                    break;
            }
        }

        return begin;
    }

    private String errorForMissingRequiredFields(int present) {
        switch (present) {
            case 3:
                return "The outgoing-window field cannot be omitted";
            case 2:
                return "The incoming-window field cannot be omitted";
            default:
                return "The next-outgoing-id field cannot be omitted";
        }
    }
}
