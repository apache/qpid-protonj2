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
package org.apache.qpid.protonj2.codec.decoders.security;

import java.io.InputStream;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.DecoderState;
import org.apache.qpid.protonj2.codec.StreamDecoderState;
import org.apache.qpid.protonj2.codec.StreamTypeDecoder;
import org.apache.qpid.protonj2.codec.TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.AbstractDescribedTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.ListTypeDecoder;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.security.SaslInit;

/**
 * Decoder of AMQP SaslInit type values from a byte stream.
 */
public final class SaslInitTypeDecoder extends AbstractDescribedTypeDecoder<SaslInit> {

    private static final int MIN_SASL_INIT_LIST_ENTRIES = 0;
    private static final int MAX_SASL_INIT_LIST_ENTRIES = 3;

    @Override
    public UnsignedLong getDescriptorCode() {
        return SaslInit.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return SaslInit.DESCRIPTOR_SYMBOL;
    }

    @Override
    public Class<SaslInit> getTypeClass() {
        return SaslInit.class;
    }

    @Override
    public SaslInit readValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        return readProperties(buffer, state, checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder));
    }

    @Override
    public SaslInit[] readArrayElements(ProtonBuffer buffer, DecoderState state, int count) throws DecodeException {
        final TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        final SaslInit[] result = new SaslInit[count];
        for (int i = 0; i < count; ++i) {
            result[i] = readProperties(buffer, state, checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder));
        }

        return result;
    }

    @Override
    public void skipValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        decoder.skipValue(buffer, state);
    }

    private SaslInit readProperties(ProtonBuffer buffer, DecoderState state, ListTypeDecoder listDecoder) throws DecodeException {
        final SaslInit init = new SaslInit();

        @SuppressWarnings("unused")
        final int size = listDecoder.readSize(buffer);
        final int count = listDecoder.readCount(buffer);

        // Don't decode anything if things already look wrong.
        if (count < MIN_SASL_INIT_LIST_ENTRIES) {
            throw new DecodeException("Not enough entries in SaslInit list encoding: " + count);
        }

        if (count > MAX_SASL_INIT_LIST_ENTRIES) {
            throw new DecodeException("To many entries in SaslInit list encoding: " + count);
        }

        for (int index = 0; index < count; ++index) {
            switch (index) {
                case 0:
                    init.setMechanism(state.getDecoder().readSymbol(buffer, state));
                    break;
                case 1:
                    init.setInitialResponse(state.getDecoder().readBinaryAsBuffer(buffer, state));
                    break;
                case 2:
                    init.setHostname(state.getDecoder().readString(buffer, state));
                    break;
            }
        }

        return init;
    }

    @Override
    public SaslInit readValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        final StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        return readProperties(stream, state, checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder));
    }

    @Override
    public SaslInit[] readArrayElements(InputStream stream, StreamDecoderState state, int count) throws DecodeException {
        final StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        final SaslInit[] result = new SaslInit[count];
        for (int i = 0; i < count; ++i) {
            result[i] = readProperties(stream, state, checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder));
        }

        return result;
    }

    @Override
    public void skipValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        final StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        decoder.skipValue(stream, state);
    }

    private SaslInit readProperties(InputStream stream, StreamDecoderState state, ListTypeDecoder listDecoder) throws DecodeException {
        final SaslInit init = new SaslInit();

        @SuppressWarnings("unused")
        final int size = listDecoder.readSize(stream);
        final int count = listDecoder.readCount(stream);

        // Don't decode anything if things already look wrong.
        if (count < MIN_SASL_INIT_LIST_ENTRIES) {
            throw new DecodeException("Not enough entries in SaslInit list encoding: " + count);
        }

        if (count > MAX_SASL_INIT_LIST_ENTRIES) {
            throw new DecodeException("To many entries in SaslInit list encoding: " + count);
        }

        for (int index = 0; index < count; ++index) {
            switch (index) {
                case 0:
                    init.setMechanism(state.getDecoder().readSymbol(stream, state));
                    break;
                case 1:
                    init.setInitialResponse(state.getDecoder().readBinaryAsBuffer(stream, state));
                    break;
                case 2:
                    init.setHostname(state.getDecoder().readString(stream, state));
                    break;
            }
        }

        return init;
    }
}
