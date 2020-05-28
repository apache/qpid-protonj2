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
import org.apache.qpid.proton4j.amqp.messaging.Outcome;
import org.apache.qpid.proton4j.amqp.transactions.TransactionalState;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.codec.DecoderState;
import org.apache.qpid.proton4j.codec.TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.AbstractDescribedTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.ListTypeDecoder;

/**
 * Decoder of AMQP TransactionState types from a byte stream.
 */
public final class TransactionStateTypeDecoder extends AbstractDescribedTypeDecoder<TransactionalState> {

    private static final int MIN_TRANSACTION_STATE_LIST_ENTRIES = 1;
    private static final int MAX_TRANSACTION_STATE_LIST_ENTRIES = 2;

    @Override
    public Class<TransactionalState> getTypeClass() {
        return TransactionalState.class;
    }

    @Override
    public UnsignedLong getDescriptorCode() {
        return TransactionalState.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return TransactionalState.DESCRIPTOR_SYMBOL;
    }

    @Override
    public TransactionalState readValue(ProtonBuffer buffer, DecoderState state) throws IOException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        return readTransactionalState(buffer, state, (ListTypeDecoder) decoder);
    }

    @Override
    public TransactionalState[] readArrayElements(ProtonBuffer buffer, DecoderState state, int count) throws IOException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        TransactionalState[] result = new TransactionalState[count];
        for (int i = 0; i < count; ++i) {
            result[i] = readTransactionalState(buffer, state, (ListTypeDecoder) decoder);
        }

        return result;
    }

    @Override
    public void skipValue(ProtonBuffer buffer, DecoderState state) throws IOException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        decoder.skipValue(buffer, state);
    }

    private TransactionalState readTransactionalState(ProtonBuffer buffer, DecoderState state, ListTypeDecoder listDecoder) throws IOException {
        TransactionalState transactionalState = new TransactionalState();

        @SuppressWarnings("unused")
        int size = listDecoder.readSize(buffer);
        int count = listDecoder.readCount(buffer);

        // Don't decode anything if things already look wrong.
        if (count < MIN_TRANSACTION_STATE_LIST_ENTRIES) {
            throw new IllegalStateException("Not enough entries in TransactionalState list encoding: " + count);
        }

        if (count > MAX_TRANSACTION_STATE_LIST_ENTRIES) {
            throw new IllegalStateException("To many entries in TransactionalState list encoding: " + count);
        }

        for (int index = 0; index < count; ++index) {
            switch (index) {
                case 0:
                    transactionalState.setTxnId(state.getDecoder().readBinary(buffer, state));
                    break;
                case 1:
                    transactionalState.setOutcome((Outcome) state.getDecoder().readObject(buffer, state));
                    break;
                default:
                    throw new IllegalStateException("To many entries in TransactionalState encoding");
            }
        }

        return transactionalState;
    }
}
