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

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedLong;
import org.apache.qpid.proton4j.amqp.messaging.Header;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.codec.DecodeException;
import org.apache.qpid.proton4j.codec.DecoderState;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.apache.qpid.proton4j.codec.TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.AbstractDescribedTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.ListTypeDecoder;

/**
 * Decoder of AMQP Header types from a byte stream
 */
public final class HeaderTypeDecoder extends AbstractDescribedTypeDecoder<Header> {

    private static final int MIN_HEADER_LIST_ENTRIES = 0;
    private static final int MAX_HEADER_LIST_ENTRIES = 5;

    @Override
    public Class<Header> getTypeClass() {
        return Header.class;
    }

    @Override
    public UnsignedLong getDescriptorCode() {
        return Header.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return Header.DESCRIPTOR_SYMBOL;
    }

    @Override
    public Header readValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        return readHeader(buffer, state, (ListTypeDecoder) decoder);
    }

    @Override
    public Header[] readArrayElements(ProtonBuffer buffer, DecoderState state, int count) throws DecodeException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        Header[] result = new Header[count];
        for (int i = 0; i < count; ++i) {
            result[i] = readHeader(buffer, state, (ListTypeDecoder) decoder);
        }

        return result;
    }

    @Override
    public void skipValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        decoder.skipValue(buffer, state);
    }

    private Header readHeader(ProtonBuffer buffer, DecoderState state, ListTypeDecoder listDecoder) throws DecodeException {
        Header header = new Header();

        @SuppressWarnings("unused")
        int size = listDecoder.readSize(buffer);
        int count = listDecoder.readCount(buffer);

        // Don't decode anything if things already look wrong.
        if (count < MIN_HEADER_LIST_ENTRIES) {
            throw new DecodeException("Not enough entries in Header list encoding: " + count);
        }

        if (count > MAX_HEADER_LIST_ENTRIES) {
            throw new DecodeException("To many entries in Header list encoding: " + count);
        }

        for (int index = 0; index < count; ++index) {
            // Peek ahead and see if there is a null in the next slot, if so we don't call
            // the setter for that entry to ensure the returned type reflects the encoded
            // state in the modification entry.
            boolean nullValue = buffer.getByte(buffer.getReadIndex()) == EncodingCodes.NULL;
            if (nullValue) {
                buffer.readByte();
                continue;
            }

            switch (index) {
                case 0:
                    header.setDurable(state.getDecoder().readBoolean(buffer, state, false));
                    break;
                case 1:
                    header.setPriority(state.getDecoder().readUnsignedByte(buffer, state, Header.DEFAULT_PRIORITY));
                    break;
                case 2:
                    header.setTimeToLive(state.getDecoder().readUnsignedInteger(buffer, state, 0));
                    break;
                case 3:
                    header.setFirstAcquirer(state.getDecoder().readBoolean(buffer, state, false));
                    break;
                case 4:
                    header.setDeliveryCount(state.getDecoder().readUnsignedInteger(buffer, state, 0));
                    break;
                default:
                    throw new DecodeException("To many entries in Header encoding");
            }
        }

        return header;
    }
}
