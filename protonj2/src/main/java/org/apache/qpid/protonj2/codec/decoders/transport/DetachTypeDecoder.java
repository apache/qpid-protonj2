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
import org.apache.qpid.protonj2.codec.Decoder;
import org.apache.qpid.protonj2.codec.DecoderState;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.StreamDecoder;
import org.apache.qpid.protonj2.codec.StreamDecoderState;
import org.apache.qpid.protonj2.codec.StreamTypeDecoder;
import org.apache.qpid.protonj2.codec.TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.AbstractDescribedListTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.ProtonStreamUtils;
import org.apache.qpid.protonj2.codec.decoders.primitives.ListTypeDecoder;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.transport.Detach;
import org.apache.qpid.protonj2.types.transport.ErrorCondition;

/**
 * Decoder of AMQP Detach type values from a byte stream
 */
public final class DetachTypeDecoder extends AbstractDescribedListTypeDecoder<Detach> {

    private static final int MIN_DETACH_LIST_ENTRIES = 1;
    private static final int MAX_DETACH_LIST_ENTRIES = 3;

    @Override
    public Class<Detach> getTypeClass() {
        return Detach.class;
    }

    @Override
    public UnsignedLong getDescriptorCode() {
        return Detach.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return Detach.DESCRIPTOR_SYMBOL;
    }

    @Override
    public Detach readValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        return readDetach(buffer, state.getDecoder(), state, checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder));
    }

    @Override
    public Detach[] readArrayElements(ProtonBuffer buffer, DecoderState state, int count) throws DecodeException {
        final TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        final Detach[] result = new Detach[count];
        for (int i = 0; i < count; ++i) {
            result[i] = readDetach(buffer, state.getDecoder(), state, checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder));
        }

        return result;
    }

    private Detach readDetach(ProtonBuffer buffer, Decoder decoder, DecoderState state, ListTypeDecoder listDecoder) throws DecodeException {
        final Detach detach = new Detach();

        @SuppressWarnings("unused")
        final int size = listDecoder.readSize(buffer, state);
        final int count = listDecoder.readCount(buffer, state);

        if (count < MIN_DETACH_LIST_ENTRIES) {
            throw new DecodeException("The handle field is mandatory");
        }

        if (count > MAX_DETACH_LIST_ENTRIES) {
            throw new DecodeException("To many entries in Detach list encoding: " + count);
        }

        for (int index = 0; index < count; ++index) {
            // Peek ahead and see if there is a null in the next slot, if so we don't call
            // the setter for that entry to ensure the returned type reflects the encoded
            // state in the modification entry.
            final boolean nullValue = buffer.getByte(buffer.getReadOffset()) == EncodingCodes.NULL;
            if (nullValue) {
                if (index == 0) {
                    throw new DecodeException("The handle field is mandatory in a Detach");
                }
                buffer.advanceReadOffset(1);
                continue;
            }

            switch (index) {
                case 0:
                    detach.setHandle(decoder.readUnsignedInteger(buffer, state, 0l));
                    break;
                case 1:
                    detach.setClosed(decoder.readBoolean(buffer, state, false));
                    break;
                case 2:
                    detach.setError(decoder.readObject(buffer, state, ErrorCondition.class));
                    break;
            }
        }

        return detach;
    }

    @Override
    public Detach readValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        final StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        return readDetach(stream, state.getDecoder(), state, checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder));
    }

    @Override
    public Detach[] readArrayElements(InputStream stream, StreamDecoderState state, int count) throws DecodeException {
        final StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        final Detach[] result = new Detach[count];
        for (int i = 0; i < count; ++i) {
            result[i] = readDetach(stream, state.getDecoder(), state, checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder));
        }

        return result;
    }

    private Detach readDetach(InputStream stream, StreamDecoder decoder, StreamDecoderState state, ListTypeDecoder listDecoder) throws DecodeException {
        final Detach detach = new Detach();

        @SuppressWarnings("unused")
        final int size = listDecoder.readSize(stream, state);
        final int count = listDecoder.readCount(stream, state);

        if (count < MIN_DETACH_LIST_ENTRIES) {
            throw new DecodeException("The handle field is mandatory in a Detach");
        }

        if (count > MAX_DETACH_LIST_ENTRIES) {
            throw new DecodeException("To many entries in Detach list encoding: " + count);
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
                        throw new DecodeException("The handle field is mandatory");
                    }

                    continue;
                } else {
                    ProtonStreamUtils.reset(stream);
                }
            }

            switch (index) {
                case 0:
                    detach.setHandle(decoder.readUnsignedInteger(stream, state, 0l));
                    break;
                case 1:
                    detach.setClosed(decoder.readBoolean(stream, state, false));
                    break;
                case 2:
                    detach.setError(decoder.readObject(stream, state, ErrorCondition.class));
                    break;
            }
        }

        return detach;
    }
}
