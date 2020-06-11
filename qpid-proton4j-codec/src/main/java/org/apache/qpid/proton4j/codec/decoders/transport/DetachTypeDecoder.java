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
package org.apache.qpid.proton4j.codec.decoders.transport;

import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.codec.DecodeException;
import org.apache.qpid.proton4j.codec.DecoderState;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.apache.qpid.proton4j.codec.TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.AbstractDescribedTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.ListTypeDecoder;
import org.apache.qpid.proton4j.types.Symbol;
import org.apache.qpid.proton4j.types.UnsignedLong;
import org.apache.qpid.proton4j.types.transport.Detach;
import org.apache.qpid.proton4j.types.transport.ErrorCondition;

/**
 * Decoder of AMQP Detach type values from a byte stream
 */
public final class DetachTypeDecoder extends AbstractDescribedTypeDecoder<Detach> {

    private static final int MIN_DETACH_LIST_ENTRIES = 1;
    private static final int MAX_DETACH_LIST_ENTRIES = 3;

    @Override
    public Class<Detach> getTypeClass() {
        return Detach.class;
    }

    @Override
    public UnsignedLong getDescriptorCode() {
        return Detach.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return Detach.DESCRIPTOR_SYMBOL;
    }

    @Override
    public Detach readValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        return readDetach(buffer, state, (ListTypeDecoder) decoder);
    }

    @Override
    public Detach[] readArrayElements(ProtonBuffer buffer, DecoderState state, int count) throws DecodeException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        Detach[] result = new Detach[count];
        for (int i = 0; i < count; ++i) {
            result[i] = readDetach(buffer, state, (ListTypeDecoder) decoder);
        }

        return result;
    }

    @Override
    public void skipValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        decoder.skipValue(buffer, state);
    }

    private Detach readDetach(ProtonBuffer buffer, DecoderState state, ListTypeDecoder listDecoder) throws DecodeException {
        Detach detach = new Detach();

        @SuppressWarnings("unused")
        int size = listDecoder.readSize(buffer);
        int count = listDecoder.readCount(buffer);

        if (count < MIN_DETACH_LIST_ENTRIES) {
            throw new DecodeException("The handle field is mandatory");
        }

        for (int index = 0; index < count; ++index) {
            // Peek ahead and see if there is a null in the next slot, if so we don't call
            // the setter for that entry to ensure the returned type reflects the encoded
            // state in the modification entry.
            boolean nullValue = buffer.getByte(buffer.getReadIndex()) == EncodingCodes.NULL;
            if (nullValue) {
                if (index == 0) {
                    throw new DecodeException("The handle field is mandatory");
                }
                buffer.readByte();
                continue;
            }

            switch (index) {
                case 0:
                    detach.setHandle(state.getDecoder().readUnsignedInteger(buffer, state, 0l));
                    break;
                case 1:
                    detach.setClosed(state.getDecoder().readBoolean(buffer, state, false));
                    break;
                case 2:
                    detach.setError(state.getDecoder().readObject(buffer, state, ErrorCondition.class));
                    break;
                default:
                    throw new DecodeException(
                        "To many entries in Detach list encoding: " + count + " max allowed entries = " + MAX_DETACH_LIST_ENTRIES);
            }
        }

        return detach;
    }
}
