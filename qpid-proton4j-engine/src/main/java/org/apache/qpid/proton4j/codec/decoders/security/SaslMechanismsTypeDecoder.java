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
package org.apache.qpid.proton4j.codec.decoders.security;

import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.codec.DecodeException;
import org.apache.qpid.proton4j.codec.DecoderState;
import org.apache.qpid.proton4j.codec.TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.AbstractDescribedTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.ListTypeDecoder;
import org.apache.qpid.proton4j.types.Symbol;
import org.apache.qpid.proton4j.types.UnsignedLong;
import org.apache.qpid.proton4j.types.security.SaslMechanisms;

/**
 * Decoder of AMQP SaslChallenge type values from a byte stream.
 */
public final class SaslMechanismsTypeDecoder extends AbstractDescribedTypeDecoder<SaslMechanisms> {

    private static final int MIN_SASL_MECHANISMS_LIST_ENTRIES = 1;

    @Override
    public UnsignedLong getDescriptorCode() {
        return SaslMechanisms.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return SaslMechanisms.DESCRIPTOR_SYMBOL;
    }

    @Override
    public Class<SaslMechanisms> getTypeClass() {
        return SaslMechanisms.class;
    }

    @Override
    public SaslMechanisms readValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        return readProperties(buffer, state, (ListTypeDecoder) decoder);
    }

    @Override
    public void skipValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        decoder.skipValue(buffer, state);
    }

    @Override
    public SaslMechanisms[] readArrayElements(ProtonBuffer buffer, DecoderState state, int count) throws DecodeException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        SaslMechanisms[] result = new SaslMechanisms[count];
        for (int i = 0; i < count; ++i) {
            result[i] = readProperties(buffer, state, (ListTypeDecoder) decoder);
        }

        return result;
    }

    private SaslMechanisms readProperties(ProtonBuffer buffer, DecoderState state, ListTypeDecoder listDecoder) throws DecodeException {
        SaslMechanisms mechanisms = new SaslMechanisms();

        @SuppressWarnings("unused")
        int size = listDecoder.readSize(buffer);
        int count = listDecoder.readCount(buffer);

        if (count < MIN_SASL_MECHANISMS_LIST_ENTRIES) {
            throw new DecodeException("SASL Mechanisms must contain at least one mechanisms entry: " + count);
        }

        for (int index = 0; index < count; ++index) {
            switch (index) {
                case 0:
                    mechanisms.setSaslServerMechanisms(state.getDecoder().readMultiple(buffer, state, Symbol.class));
                    break;
                default:
                    throw new DecodeException("To many entries in Properties encoding");
            }
        }

        return mechanisms;
    }
}
