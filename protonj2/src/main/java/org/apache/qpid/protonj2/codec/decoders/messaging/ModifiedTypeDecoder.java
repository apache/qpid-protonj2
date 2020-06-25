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
package org.apache.qpid.protonj2.codec.decoders.messaging;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.DecoderState;
import org.apache.qpid.protonj2.codec.TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.AbstractDescribedTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.ListTypeDecoder;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.messaging.Modified;

/**
 * Decoder of AMQP Modified type values from a byte stream.
 */
public final class ModifiedTypeDecoder extends AbstractDescribedTypeDecoder<Modified> {

    private static final int MIN_MODIFIED_LIST_ENTRIES = 0;
    private static final int MAX_MODIFIED_LIST_ENTRIES = 3;

    @Override
    public Class<Modified> getTypeClass() {
        return Modified.class;
    }

    @Override
    public UnsignedLong getDescriptorCode() {
        return Modified.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return Modified.DESCRIPTOR_SYMBOL;
    }

    @Override
    public Modified readValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        return readModified(buffer, state, (ListTypeDecoder) decoder);
    }

    @Override
    public Modified[] readArrayElements(ProtonBuffer buffer, DecoderState state, int count) throws DecodeException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        Modified[] result = new Modified[count];
        for (int i = 0; i < count; ++i) {
            result[i] = readModified(buffer, state, (ListTypeDecoder) decoder);
        }

        return result;
    }

    @Override
    public void skipValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        decoder.skipValue(buffer, state);
    }

    private Modified readModified(ProtonBuffer buffer, DecoderState state, ListTypeDecoder listDecoder) throws DecodeException {
        Modified modified = new Modified();

        @SuppressWarnings("unused")
        int size = listDecoder.readSize(buffer);
        int count = listDecoder.readCount(buffer);

        // Don't decode anything if things already look wrong.
        if (count < MIN_MODIFIED_LIST_ENTRIES) {
            throw new DecodeException("Not enough entries in Modified list encoding: " + count);
        }

        if (count > MAX_MODIFIED_LIST_ENTRIES) {
            throw new DecodeException("To many entries in Modified list encoding: " + count);
        }

        for (int index = 0; index < count; ++index) {
            switch (index) {
                case 0:
                    modified.setDeliveryFailed(state.getDecoder().readBoolean(buffer, state, false));
                    break;
                case 1:
                    modified.setUndeliverableHere(state.getDecoder().readBoolean(buffer, state, false));
                    break;
                case 2:
                    modified.setMessageAnnotations(state.getDecoder().readMap(buffer, state));
                    break;
                default:
                    throw new DecodeException("To many entries in Modified encoding");
            }
        }

        return modified;
    }
}
