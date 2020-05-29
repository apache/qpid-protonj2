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

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedLong;
import org.apache.qpid.proton4j.amqp.transport.Open;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.codec.DecodeException;
import org.apache.qpid.proton4j.codec.DecoderState;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.apache.qpid.proton4j.codec.TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.AbstractDescribedTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.ListTypeDecoder;

/**
 * Decoder of AMQP Open type values from a byte stream.
 */
public final class OpenTypeDecoder extends AbstractDescribedTypeDecoder<Open> {

    private static final int MIN_OPEN_LIST_ENTRIES = 1;
    private static final int MAX_OPEN_LIST_ENTRIES = 10;

    @Override
    public Class<Open> getTypeClass() {
        return Open.class;
    }

    @Override
    public UnsignedLong getDescriptorCode() {
        return Open.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return Open.DESCRIPTOR_SYMBOL;
    }

    @Override
    public Open readValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        return readOpen(buffer, state, (ListTypeDecoder) decoder);
    }

    @Override
    public Open[] readArrayElements(ProtonBuffer buffer, DecoderState state, int count) throws DecodeException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        Open[] result = new Open[count];
        for (int i = 0; i < count; ++i) {
            result[i] = readOpen(buffer, state, (ListTypeDecoder) decoder);
        }

        return result;
    }

    @Override
    public void skipValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        decoder.skipValue(buffer, state);
    }

    private Open readOpen(ProtonBuffer buffer, DecoderState state, ListTypeDecoder listDecoder) throws DecodeException {
        Open open = new Open();

        @SuppressWarnings("unused")
        int size = listDecoder.readSize(buffer);
        int count = listDecoder.readCount(buffer);

        if (count < MIN_OPEN_LIST_ENTRIES) {
            throw new DecodeException("The container-id field cannot be omitted");
        }

        for (int index = 0; index < count; ++index) {
            // Peek ahead and see if there is a null in the next slot, if so we don't call
            // the setter for that entry to ensure the returned type reflects the encoded
            // state in the modification entry.
            boolean nullValue = buffer.getByte(buffer.getReadIndex()) == EncodingCodes.NULL;
            if (nullValue) {
                if (index == 0) {
                    throw new DecodeException("The container-id field cannot be omitted");
                }
                buffer.readByte();
                continue;
            }

            switch (index) {
                case 0:
                    open.setContainerId(state.getDecoder().readString(buffer, state));
                    break;
                case 1:
                    open.setHostname(state.getDecoder().readString(buffer, state));
                    break;
                case 2:
                    open.setMaxFrameSize(state.getDecoder().readUnsignedInteger(buffer, state, 0l));
                    break;
                case 3:
                    open.setChannelMax(state.getDecoder().readUnsignedShort(buffer, state, 0));
                    break;
                case 4:
                    open.setIdleTimeout(state.getDecoder().readUnsignedInteger(buffer, state, 0l));
                    break;
                case 5:
                    open.setOutgoingLocales(state.getDecoder().readMultiple(buffer, state, Symbol.class));
                    break;
                case 6:
                    open.setIncomingLocales(state.getDecoder().readMultiple(buffer, state, Symbol.class));
                    break;
                case 7:
                    open.setOfferedCapabilities(state.getDecoder().readMultiple(buffer, state, Symbol.class));
                    break;
                case 8:
                    open.setDesiredCapabilities(state.getDecoder().readMultiple(buffer, state, Symbol.class));
                    break;
                case 9:
                    open.setProperties(state.getDecoder().readMap(buffer, state));
                    break;
                default:
                    throw new DecodeException(
                        "To many entries in Open list encoding: " + count + " max allowed entries = " + MAX_OPEN_LIST_ENTRIES);
            }
        }

        return open;
    }
}
