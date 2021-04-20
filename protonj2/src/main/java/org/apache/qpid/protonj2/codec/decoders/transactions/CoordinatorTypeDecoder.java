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
package org.apache.qpid.protonj2.codec.decoders.transactions;

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
import org.apache.qpid.protonj2.types.transactions.Coordinator;

/**
 * Decoder of AMQP Coordinator type values from a byte stream.
 */
public final class CoordinatorTypeDecoder extends AbstractDescribedTypeDecoder<Coordinator> {

    private static final int MIN_COORDINATOR_LIST_ENTRIES = 0;
    private static final int MAX_COORDINATOR_LIST_ENTRIES = 1;

    @Override
    public Class<Coordinator> getTypeClass() {
        return Coordinator.class;
    }

    @Override
    public UnsignedLong getDescriptorCode() {
        return Coordinator.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return Coordinator.DESCRIPTOR_SYMBOL;
    }

    @Override
    public Coordinator readValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        return readCoordinator(buffer, state, checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder));
    }

    @Override
    public Coordinator[] readArrayElements(ProtonBuffer buffer, DecoderState state, int count) throws DecodeException {
        final TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        final Coordinator[] result = new Coordinator[count];
        for (int i = 0; i < count; ++i) {
            result[i] = readCoordinator(buffer, state, checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder));
        }

        return result;
    }

    @Override
    public void skipValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        decoder.skipValue(buffer, state);
    }

    private Coordinator readCoordinator(ProtonBuffer buffer, DecoderState state, ListTypeDecoder listDecoder) throws DecodeException {
        final Coordinator coordinator = new Coordinator();

        @SuppressWarnings("unused")
        final int size = listDecoder.readSize(buffer);
        final int count = listDecoder.readCount(buffer);

        // Don't decode anything if things already look wrong.
        if (count < MIN_COORDINATOR_LIST_ENTRIES) {
            throw new DecodeException("Not enougn entries in Coordinator list encoding: " + count);
        } else if (count > MAX_COORDINATOR_LIST_ENTRIES) {
            throw new DecodeException("To many entries in Coordinator list encoding: " + count);
        } else if (count == 1) {
            coordinator.setCapabilities(state.getDecoder().readMultiple(buffer, state, Symbol.class));
        }

        return coordinator;
    }

    @Override
    public Coordinator readValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        final StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        return readCoordinator(stream, state, checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder));
    }

    @Override
    public Coordinator[] readArrayElements(InputStream stream, StreamDecoderState state, int count) throws DecodeException {
        final StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        final Coordinator[] result = new Coordinator[count];
        for (int i = 0; i < count; ++i) {
            result[i] = readCoordinator(stream, state, checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder));
        }

        return result;
    }

    @Override
    public void skipValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        final StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        decoder.skipValue(stream, state);
    }

    private Coordinator readCoordinator(InputStream stream, StreamDecoderState state, ListTypeDecoder listDecoder) throws DecodeException {
        final Coordinator coordinator = new Coordinator();

        @SuppressWarnings("unused")
        final int size = listDecoder.readSize(stream);
        final int count = listDecoder.readCount(stream);

        // Don't decode anything if things already look wrong.
        if (count < MIN_COORDINATOR_LIST_ENTRIES) {
            throw new DecodeException("Not enougn entries in Coordinator list encoding: " + count);
        } else if (count > MAX_COORDINATOR_LIST_ENTRIES) {
            throw new DecodeException("To many entries in Coordinator list encoding: " + count);
        } else if (count == 1) {
            coordinator.setCapabilities(state.getDecoder().readMultiple(stream, state, Symbol.class));
        }

        return coordinator;
    }
}
