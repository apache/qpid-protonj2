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
import org.apache.qpid.protonj2.types.transactions.Declared;

/**
 * Decoder of AMQP Declared types from a byte stream.
 */
public final class DeclaredTypeDecoder extends AbstractDescribedTypeDecoder<Declared> {

    private static final int MIN_DECLARED_LIST_ENTRIES = 1;
    private static final int MAX_DECLARED_LIST_ENTRIES = 1;

    @Override
    public Class<Declared> getTypeClass() {
        return Declared.class;
    }

    @Override
    public UnsignedLong getDescriptorCode() {
        return Declared.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return Declared.DESCRIPTOR_SYMBOL;
    }

    @Override
    public Declared readValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        return readDeclared(buffer, state, (ListTypeDecoder) decoder);
    }

    @Override
    public Declared[] readArrayElements(ProtonBuffer buffer, DecoderState state, int count) throws DecodeException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        Declared[] result = new Declared[count];
        for (int i = 0; i < count; ++i) {
            result[i] = readDeclared(buffer, state, (ListTypeDecoder) decoder);
        }

        return result;
    }

    @Override
    public void skipValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        decoder.skipValue(buffer, state);
    }

    private Declared readDeclared(ProtonBuffer buffer, DecoderState state, ListTypeDecoder listDecoder) throws DecodeException {
        Declared declared = new Declared();

        @SuppressWarnings("unused")
        int size = listDecoder.readSize(buffer);
        int count = listDecoder.readCount(buffer);

        // Don't decode anything if things already look wrong.
        if (count < MIN_DECLARED_LIST_ENTRIES) {
            throw new DecodeException("The txn-id field cannot be omitted");
        }

        for (int index = 0; index < count; ++index) {
            switch (index) {
                case 0:
                    declared.setTxnId(state.getDecoder().readBinary(buffer, state));
                    break;
                default:
                    throw new DecodeException(
                        "To many entries in Declared list encoding: " + count + " max allowed entries = " + MAX_DECLARED_LIST_ENTRIES);
            }
        }

        return declared;
    }

    @Override
    public Declared readValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        return readDeclared(stream, state, (ListTypeDecoder) decoder);
    }

    @Override
    public Declared[] readArrayElements(InputStream stream, StreamDecoderState state, int count) throws DecodeException {
        StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        Declared[] result = new Declared[count];
        for (int i = 0; i < count; ++i) {
            result[i] = readDeclared(stream, state, (ListTypeDecoder) decoder);
        }

        return result;
    }

    @Override
    public void skipValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        decoder.skipValue(stream, state);
    }

    private Declared readDeclared(InputStream stream, StreamDecoderState state, ListTypeDecoder listDecoder) throws DecodeException {
        Declared declared = new Declared();

        @SuppressWarnings("unused")
        int size = listDecoder.readSize(stream);
        int count = listDecoder.readCount(stream);

        // Don't decode anything if things already look wrong.
        if (count < MIN_DECLARED_LIST_ENTRIES) {
            throw new DecodeException("The txn-id field cannot be omitted");
        }

        for (int index = 0; index < count; ++index) {
            switch (index) {
                case 0:
                    declared.setTxnId(state.getDecoder().readBinary(stream, state));
                    break;
                default:
                    throw new DecodeException(
                        "To many entries in Declared list encoding: " + count + " max allowed entries = " + MAX_DECLARED_LIST_ENTRIES);
            }
        }

        return declared;
    }
}
