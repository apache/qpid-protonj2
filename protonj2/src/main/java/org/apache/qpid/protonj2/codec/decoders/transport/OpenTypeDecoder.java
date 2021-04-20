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
import org.apache.qpid.protonj2.types.transport.Open;

/**
 * Decoder of AMQP Open type values from a byte stream.
 */
public final class OpenTypeDecoder extends AbstractDescribedTypeDecoder<Open> {

    private static final int MIN_OPEN_LIST_ENTRIES = 1;
    private static final int MAX_OPEN_LIST_ENTRIES = 10;

    @Override
    public Class<Open> getTypeClass() {
        return Open.class;
    }

    @Override
    public UnsignedLong getDescriptorCode() {
        return Open.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return Open.DESCRIPTOR_SYMBOL;
    }

    @Override
    public Open readValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        return readOpen(buffer, state, checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder));
    }

    @Override
    public Open[] readArrayElements(ProtonBuffer buffer, DecoderState state, int count) throws DecodeException {
        final TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        final Open[] result = new Open[count];
        for (int i = 0; i < count; ++i) {
            result[i] = readOpen(buffer, state, checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder));
        }

        return result;
    }

    @Override
    public void skipValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        decoder.skipValue(buffer, state);
    }

    private Open readOpen(ProtonBuffer buffer, DecoderState state, ListTypeDecoder listDecoder) throws DecodeException {
        final Open open = new Open();

        @SuppressWarnings("unused")
        final int size = listDecoder.readSize(buffer);
        final int count = listDecoder.readCount(buffer);

        if (count < MIN_OPEN_LIST_ENTRIES) {
            throw new DecodeException("The container-id field cannot be omitted");
        }

        if (count > MAX_OPEN_LIST_ENTRIES) {
            throw new DecodeException("To many entries in Open list encoding: " + count);
        }

        for (int index = 0; index < count; ++index) {
            // Peek ahead and see if there is a null in the next slot, if so we don't call
            // the setter for that entry to ensure the returned type reflects the encoded
            // state in the modification entry.
            final boolean nullValue = buffer.getByte(buffer.getReadIndex()) == EncodingCodes.NULL;
            if (nullValue) {
                if (index == 0) {
                    throw new DecodeException("The container-id field cannot be omitted");
                }
                buffer.readByte();
                continue;
            }

            switch (index) {
                case 0:
                    open.setContainerId(state.getDecoder().readString(buffer, state));
                    break;
                case 1:
                    open.setHostname(state.getDecoder().readString(buffer, state));
                    break;
                case 2:
                    open.setMaxFrameSize(state.getDecoder().readUnsignedInteger(buffer, state, 0l));
                    break;
                case 3:
                    open.setChannelMax(state.getDecoder().readUnsignedShort(buffer, state, 0));
                    break;
                case 4:
                    open.setIdleTimeout(state.getDecoder().readUnsignedInteger(buffer, state, 0l));
                    break;
                case 5:
                    open.setOutgoingLocales(state.getDecoder().readMultiple(buffer, state, Symbol.class));
                    break;
                case 6:
                    open.setIncomingLocales(state.getDecoder().readMultiple(buffer, state, Symbol.class));
                    break;
                case 7:
                    open.setOfferedCapabilities(state.getDecoder().readMultiple(buffer, state, Symbol.class));
                    break;
                case 8:
                    open.setDesiredCapabilities(state.getDecoder().readMultiple(buffer, state, Symbol.class));
                    break;
                case 9:
                    open.setProperties(state.getDecoder().readMap(buffer, state));
                    break;
            }
        }

        return open;
    }

    @Override
    public Open readValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        final StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        return readOpen(stream, state, checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder));
    }

    @Override
    public Open[] readArrayElements(InputStream stream, StreamDecoderState state, int count) throws DecodeException {
        final StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        final Open[] result = new Open[count];
        for (int i = 0; i < count; ++i) {
            result[i] = readOpen(stream, state, checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder));
        }

        return result;
    }

    @Override
    public void skipValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        final StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        decoder.skipValue(stream, state);
    }

    private Open readOpen(InputStream stream, StreamDecoderState state, ListTypeDecoder listDecoder) throws DecodeException {
        final Open open = new Open();

        @SuppressWarnings("unused")
        final int size = listDecoder.readSize(stream);
        final int count = listDecoder.readCount(stream);

        if (count < MIN_OPEN_LIST_ENTRIES) {
            throw new DecodeException("The container-id field cannot be omitted");
        }

        if (count > MAX_OPEN_LIST_ENTRIES) {
            throw new DecodeException("To many entries in Open list encoding: " + count);
        }

        for (int index = 0; index < count; ++index) {
            // If the stream allows we peek ahead and see if there is a null in the next slot,
            // if so we don't call the setter for that entry to ensure the returned type reflects
            // the encoded state in the modification entry.
            if (stream.markSupported()) {
                stream.mark(1);
                final boolean nullValue = ProtonStreamUtils.readByte(stream) == EncodingCodes.NULL;
                if (nullValue) {
                    if (index == 0) {
                        throw new DecodeException("The container-id field cannot be omitted");
                    }

                    continue;
                } else {
                    ProtonStreamUtils.reset(stream);
                }
            }

            switch (index) {
                case 0:
                    open.setContainerId(state.getDecoder().readString(stream, state));
                    break;
                case 1:
                    open.setHostname(state.getDecoder().readString(stream, state));
                    break;
                case 2:
                    open.setMaxFrameSize(state.getDecoder().readUnsignedInteger(stream, state, 0l));
                    break;
                case 3:
                    open.setChannelMax(state.getDecoder().readUnsignedShort(stream, state, 0));
                    break;
                case 4:
                    open.setIdleTimeout(state.getDecoder().readUnsignedInteger(stream, state, 0l));
                    break;
                case 5:
                    open.setOutgoingLocales(state.getDecoder().readMultiple(stream, state, Symbol.class));
                    break;
                case 6:
                    open.setIncomingLocales(state.getDecoder().readMultiple(stream, state, Symbol.class));
                    break;
                case 7:
                    open.setOfferedCapabilities(state.getDecoder().readMultiple(stream, state, Symbol.class));
                    break;
                case 8:
                    open.setDesiredCapabilities(state.getDecoder().readMultiple(stream, state, Symbol.class));
                    break;
                case 9:
                    open.setProperties(state.getDecoder().readMap(stream, state));
                    break;
            }
        }

        return open;
    }
}
