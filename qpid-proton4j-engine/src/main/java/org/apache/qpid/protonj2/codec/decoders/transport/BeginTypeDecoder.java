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
package org.apache.qpid.protonj2.codec.decoders.transport;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.DecoderState;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.AbstractDescribedTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.ListTypeDecoder;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.transport.Begin;

/**
 * Decoder of AMQP Begin type values from a byte stream
 */
public final class BeginTypeDecoder extends AbstractDescribedTypeDecoder<Begin> {

    private static final int MIN_BEGIN_LIST_ENTRIES = 4;
    private static final int MAX_BEGIN_LIST_ENTRIES = 8;

    @Override
    public Class<Begin> getTypeClass() {
        return Begin.class;
    }

    @Override
    public UnsignedLong getDescriptorCode() {
        return Begin.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return Begin.DESCRIPTOR_SYMBOL;
    }

    @Override
    public Begin readValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        return readBegin(buffer, state, (ListTypeDecoder) decoder);
    }

    @Override
    public Begin[] readArrayElements(ProtonBuffer buffer, DecoderState state, int count) throws DecodeException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        Begin[] result = new Begin[count];
        for (int i = 0; i < count; ++i) {
            result[i] = readBegin(buffer, state, (ListTypeDecoder) decoder);
        }

        return result;
    }

    @Override
    public void skipValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        decoder.skipValue(buffer, state);
    }

    private Begin readBegin(ProtonBuffer buffer, DecoderState state, ListTypeDecoder listDecoder) throws DecodeException {
        Begin begin = new Begin();

        @SuppressWarnings("unused")
        int size = listDecoder.readSize(buffer);
        int count = listDecoder.readCount(buffer);

        if (count < MIN_BEGIN_LIST_ENTRIES) {
            throw new DecodeException(errorForMissingRequiredFields(count));
        }

        for (int index = 0; index < count; ++index) {
            // Peek ahead and see if there is a null in the next slot, if so we don't call
            // the setter for that entry to ensure the returned type reflects the encoded
            // state in the modification entry.
            boolean nullValue = buffer.getByte(buffer.getReadIndex()) == EncodingCodes.NULL;
            if (nullValue) {
                // Ensure mandatory fields are set
                if (index > 0 && index < MIN_BEGIN_LIST_ENTRIES) {
                    throw new DecodeException(errorForMissingRequiredFields(index));
                }

                buffer.readByte();
                continue;
            }

            switch (index) {
                case 0:
                    begin.setRemoteChannel(state.getDecoder().readUnsignedShort(buffer, state, 0));
                    break;
                case 1:
                    begin.setNextOutgoingId(state.getDecoder().readUnsignedInteger(buffer, state, 0l));
                    break;
                case 2:
                    begin.setIncomingWindow(state.getDecoder().readUnsignedInteger(buffer, state, 0l));
                    break;
                case 3:
                    begin.setOutgoingWindow(state.getDecoder().readUnsignedInteger(buffer, state, 0l));
                    break;
                case 4:
                    begin.setHandleMax(state.getDecoder().readUnsignedInteger(buffer, state, 0l));
                    break;
                case 5:
                    begin.setOfferedCapabilities(state.getDecoder().readMultiple(buffer, state, Symbol.class));
                    break;
                case 6:
                    begin.setDesiredCapabilities(state.getDecoder().readMultiple(buffer, state, Symbol.class));
                    break;
                case 7:
                    begin.setProperties(state.getDecoder().readMap(buffer, state));
                    break;
                default:
                    throw new DecodeException(
                        "To many entries in Begin list encoding: " + count + " max allowed entries = " + MAX_BEGIN_LIST_ENTRIES);
            }
        }

        return begin;
    }

    private String errorForMissingRequiredFields(int present) {
        switch (present) {
            case 3:
                return "The outgoing-window field cannot be omitted";
            case 2:
                return "The incoming-window field cannot be omitted";
            default:
                return "The next-outgoing-id field cannot be omitted";
        }
    }
}
