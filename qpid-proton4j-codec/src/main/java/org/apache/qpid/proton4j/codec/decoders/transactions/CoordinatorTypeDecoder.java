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
package org.apache.qpid.proton4j.codec.decoders.transactions;

import java.io.IOException;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedLong;
import org.apache.qpid.proton4j.amqp.transactions.Coordinator;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.codec.DecoderState;
import org.apache.qpid.proton4j.codec.TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.AbstractDescribedTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.ListTypeDecoder;

/**
 * Decoder of AMQP Coordinator type values from a byte stream.
 */
public class CoordinatorTypeDecoder extends AbstractDescribedTypeDecoder<Coordinator> {

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
    public Coordinator readValue(ProtonBuffer buffer, DecoderState state) throws IOException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        if (!(decoder instanceof ListTypeDecoder)) {
            throw new IOException("Expected List type indicator but got decoder for type: " + decoder.getTypeClass().getName());
        }

        return readCoordinator(buffer, state, (ListTypeDecoder) decoder);
    }

    @Override
    public Coordinator[] readArrayElements(ProtonBuffer buffer, DecoderState state, int count) throws IOException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        if (!(decoder instanceof ListTypeDecoder)) {
            throw new IOException("Expected List type indicator but got decoder for type: " + decoder.getTypeClass().getName());
        }

        Coordinator[] result = new Coordinator[count];
        for (int i = 0; i < count; ++i) {
            result[i] = readCoordinator(buffer, state, (ListTypeDecoder) decoder);
        }

        return result;
    }

    @Override
    public void skipValue(ProtonBuffer buffer, DecoderState state) throws IOException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        if (!(decoder instanceof ListTypeDecoder)) {
            throw new IOException("Expected List type indicator but got decoder for type: " + decoder.getTypeClass().getName());
        }

        decoder.skipValue(buffer, state);
    }

    private Coordinator readCoordinator(ProtonBuffer buffer, DecoderState state, ListTypeDecoder listDecoder) throws IOException {
        Coordinator coordinator = new Coordinator();

        @SuppressWarnings("unused")
        int size = listDecoder.readSize(buffer);
        int count = listDecoder.readCount(buffer);

        // TODO - Decoding correctness checks

        // Don't decode anything if things already look wrong.
        if (count < MIN_COORDINATOR_LIST_ENTRIES) {
            throw new IllegalStateException("Not enougn entries in Coordinator list encoding: " + count);
        }

        if (count > MAX_COORDINATOR_LIST_ENTRIES) {
            throw new IllegalStateException("To many entries in Coordinator list encoding: " + count);
        }

        for (int index = 0; index < count; ++index) {
            switch (index) {
                case 0:
                    coordinator.setCapabilities(state.getDecoder().readMultiple(buffer, state, Symbol.class));
                    break;
                default:
                    throw new IllegalStateException("To many entries in Header encoding");
            }
        }

        return coordinator;
    }
}
