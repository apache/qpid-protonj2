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
package org.apache.qpid.proton4j.codec.decoders.messaging;

import java.io.IOException;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedInteger;
import org.apache.qpid.proton4j.amqp.UnsignedLong;
import org.apache.qpid.proton4j.amqp.messaging.Outcome;
import org.apache.qpid.proton4j.amqp.messaging.Source;
import org.apache.qpid.proton4j.amqp.messaging.TerminusDurability;
import org.apache.qpid.proton4j.amqp.messaging.TerminusExpiryPolicy;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.codec.DecoderState;
import org.apache.qpid.proton4j.codec.TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.AbstractDescribedTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.ListTypeDecoder;

/**
 * Decoder of AMQP Source type values from a byte stream.
 */
public final class SourceTypeDecoder extends AbstractDescribedTypeDecoder<Source> {

    private static final int MIN_SOURCE_LIST_ENTRIES = 0;
    private static final int MAX_SOURCE_LIST_ENTRIES = 11;

    @Override
    public Class<Source> getTypeClass() {
        return Source.class;
    }

    @Override
    public UnsignedLong getDescriptorCode() {
        return Source.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return Source.DESCRIPTOR_SYMBOL;
    }

    @Override
    public Source readValue(ProtonBuffer buffer, DecoderState state) throws IOException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        return readSource(buffer, state, (ListTypeDecoder) decoder);
    }

    @Override
    public Source[] readArrayElements(ProtonBuffer buffer, DecoderState state, int count) throws IOException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        Source[] result = new Source[count];
        for (int i = 0; i < count; ++i) {
            result[i] = readSource(buffer, state, (ListTypeDecoder) decoder);
        }

        return result;
    }

    @Override
    public void skipValue(ProtonBuffer buffer, DecoderState state) throws IOException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        decoder.skipValue(buffer, state);
    }

    private Source readSource(ProtonBuffer buffer, DecoderState state, ListTypeDecoder listDecoder) throws IOException {
        Source source = new Source();

        @SuppressWarnings("unused")
        int size = listDecoder.readSize(buffer);
        int count = listDecoder.readCount(buffer);

        if (count < MIN_SOURCE_LIST_ENTRIES) {
            throw new IllegalStateException("Not enough entries in Source list encoding: " + count);
        }

        if (count > MAX_SOURCE_LIST_ENTRIES) {
            throw new IllegalStateException("To many entries in Source list encoding: " + count);
        }

        for (int index = 0; index < count; ++index) {
            switch (index) {
                case 0:
                    source.setAddress(state.getDecoder().readString(buffer, state));
                    break;
                case 1:
                    long durability = state.getDecoder().readUnsignedInteger(buffer, state, 0);
                    source.setDurable(TerminusDurability.valueOf(durability));
                    break;
                case 2:
                    Symbol expiryPolicy = state.getDecoder().readSymbol(buffer, state);
                    source.setExpiryPolicy(expiryPolicy == null ? TerminusExpiryPolicy.SESSION_END : TerminusExpiryPolicy.valueOf(expiryPolicy));
                    break;
                case 3:
                    UnsignedInteger timeout = state.getDecoder().readUnsignedInteger(buffer, state);
                    source.setTimeout(timeout == null ? UnsignedInteger.ZERO : timeout);
                    break;
                case 4:
                    source.setDynamic(state.getDecoder().readBoolean(buffer, state, false));
                    break;
                case 5:
                    source.setDynamicNodeProperties(state.getDecoder().readMap(buffer, state));
                    break;
                case 6:
                    source.setDistributionMode(state.getDecoder().readSymbol(buffer, state));
                    break;
                case 7:
                    source.setFilter(state.getDecoder().readMap(buffer, state));
                    break;
                case 8:
                    source.setDefaultOutcome(state.getDecoder().readObject(buffer, state, Outcome.class));
                    break;
                case 9:
                    source.setOutcomes(state.getDecoder().readMultiple(buffer, state, Symbol.class));
                    break;
                case 10:
                    source.setCapabilities(state.getDecoder().readMultiple(buffer, state, Symbol.class));
                    break;
                default:
                    throw new IllegalStateException("To many entries in Source encoding");
            }
        }

        return source;
    }
}
