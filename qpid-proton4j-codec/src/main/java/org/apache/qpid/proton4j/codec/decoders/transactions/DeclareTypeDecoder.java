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

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedLong;
import org.apache.qpid.proton4j.amqp.transactions.Declare;
import org.apache.qpid.proton4j.amqp.transactions.GlobalTxId;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.codec.DecodeException;
import org.apache.qpid.proton4j.codec.DecoderState;
import org.apache.qpid.proton4j.codec.TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.AbstractDescribedTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.ListTypeDecoder;

/**
 * Decoder of AMQP Declare type values from a byte stream
 */
public final class DeclareTypeDecoder extends AbstractDescribedTypeDecoder<Declare> {

    private static final int MIN_DECLARE_LIST_ENTRIES = 0;
    private static final int MAX_DECLARE_LIST_ENTRIES = 1;

    @Override
    public Class<Declare> getTypeClass() {
        return Declare.class;
    }

    @Override
    public UnsignedLong getDescriptorCode() {
        return Declare.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return Declare.DESCRIPTOR_SYMBOL;
    }

    @Override
    public Declare readValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        return readDeclare(buffer, state, (ListTypeDecoder) decoder);
    }

    @Override
    public Declare[] readArrayElements(ProtonBuffer buffer, DecoderState state, int count) throws DecodeException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        Declare[] result = new Declare[count];
        for (int i = 0; i < count; ++i) {
            result[i] = readDeclare(buffer, state, (ListTypeDecoder) decoder);
        }

        return result;
    }

    @Override
    public void skipValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        decoder.skipValue(buffer, state);
    }

    private Declare readDeclare(ProtonBuffer buffer, DecoderState state, ListTypeDecoder listDecoder) throws DecodeException {
        Declare declare = new Declare();

        @SuppressWarnings("unused")
        int size = listDecoder.readSize(buffer);
        int count = listDecoder.readCount(buffer);

        // Don't decode anything if things already look wrong.
        if (count < MIN_DECLARE_LIST_ENTRIES) {
            throw new DecodeException("Not enough entries in Declare list encoding: " + count);
        }

        if (count > MAX_DECLARE_LIST_ENTRIES) {
            throw new DecodeException("To many entries in Declare list encoding: " + count);
        }

        for (int index = 0; index < count; ++index) {
            switch (index) {
                case 0:
                    declare.setGlobalId(state.getDecoder().readObject(buffer, state, GlobalTxId.class));
                    break;
                default:
                    throw new DecodeException("To many entries in Declare encoding");
            }
        }

        return declare;
    }
}
