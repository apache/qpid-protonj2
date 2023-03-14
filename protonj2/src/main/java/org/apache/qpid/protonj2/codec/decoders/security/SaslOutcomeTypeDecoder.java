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
import org.apache.qpid.protonj2.codec.decoders.AbstractDescribedListTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.ListTypeDecoder;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.security.SaslCode;
import org.apache.qpid.protonj2.types.security.SaslOutcome;

/**
 * Decoder of AMQP SaslOutcome type values from a byte stream.
 */
public final class SaslOutcomeTypeDecoder extends AbstractDescribedListTypeDecoder<SaslOutcome> {

    private static final int MIN_SASL_OUTCOME_LIST_ENTRIES = 1;
    private static final int MAX_SASL_OUTCOME_LIST_ENTRIES = 2;

    @Override
    public UnsignedLong getDescriptorCode() {
        return SaslOutcome.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return SaslOutcome.DESCRIPTOR_SYMBOL;
    }

    @Override
    public Class<SaslOutcome> getTypeClass() {
        return SaslOutcome.class;
    }

    @Override
    public SaslOutcome readValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        return readProperties(buffer, state, checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder));
    }

    @Override
    public SaslOutcome[] readArrayElements(ProtonBuffer buffer, DecoderState state, int count) throws DecodeException {
        final TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        final SaslOutcome[] result = new SaslOutcome[count];
        for (int i = 0; i < count; ++i) {
            result[i] = readProperties(buffer, state, checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder));
        }

        return result;
    }

    private SaslOutcome readProperties(ProtonBuffer buffer, DecoderState state, ListTypeDecoder listDecoder) throws DecodeException {
        final SaslOutcome outcome = new SaslOutcome();

        @SuppressWarnings("unused")
        final int size = listDecoder.readSize(buffer, state);
        final int count = listDecoder.readCount(buffer, state);

        // Don't decode anything if things already look wrong.
        if (count < MIN_SASL_OUTCOME_LIST_ENTRIES) {
            throw new DecodeException("Not enough entries in SASL Outcome list encoding: " + count);
        }

        if (count > MAX_SASL_OUTCOME_LIST_ENTRIES) {
            throw new DecodeException("To many entries in SASL Outcome list encoding: " + count);
        }

        for (int index = 0; index < count; ++index) {
            switch (index) {
                case 0:
                    outcome.setCode(SaslCode.valueOf(state.getDecoder().readUnsignedByte(buffer, state)));
                    break;
                case 1:
                    outcome.setAdditionalData(state.getDecoder().readBinaryAsBuffer(buffer, state));
                    break;
            }
        }

        return outcome;
    }

    @Override
    public SaslOutcome readValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        final StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        return readProperties(stream, state, checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder));
    }

    @Override
    public SaslOutcome[] readArrayElements(InputStream stream, StreamDecoderState state, int count) throws DecodeException {
        final StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        final SaslOutcome[] result = new SaslOutcome[count];
        for (int i = 0; i < count; ++i) {
            result[i] = readProperties(stream, state, checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder));
        }

        return result;
    }

    private SaslOutcome readProperties(InputStream stream, StreamDecoderState state, ListTypeDecoder listDecoder) throws DecodeException {
        final SaslOutcome outcome = new SaslOutcome();

        @SuppressWarnings("unused")
        final int size = listDecoder.readSize(stream, state);
        final int count = listDecoder.readCount(stream, state);

        // Don't decode anything if things already look wrong.
        if (count < MIN_SASL_OUTCOME_LIST_ENTRIES) {
            throw new DecodeException("Not enough entries in SASL Outcome list encoding: " + count);
        }

        if (count > MAX_SASL_OUTCOME_LIST_ENTRIES) {
            throw new DecodeException("To many entries in SASL Outcome list encoding: " + count);
        }

        for (int index = 0; index < count; ++index) {
            switch (index) {
                case 0:
                    outcome.setCode(SaslCode.valueOf(state.getDecoder().readUnsignedByte(stream, state)));
                    break;
                case 1:
                    outcome.setAdditionalData(state.getDecoder().readBinaryAsBuffer(stream, state));
                    break;
            }
        }

        return outcome;
    }
}
