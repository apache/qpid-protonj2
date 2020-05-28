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

import java.io.IOException;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedLong;
import org.apache.qpid.proton4j.amqp.transport.Flow;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.codec.DecoderState;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.apache.qpid.proton4j.codec.TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.AbstractDescribedTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.ListTypeDecoder;

/**
 * Decoder of AMQP Flow type values from a byte stream.
 */
public final class FlowTypeDecoder extends AbstractDescribedTypeDecoder<Flow> {

    private static final int MIN_FLOW_LIST_ENTRIES = 4;
    private static final int MAX_FLOW_LIST_ENTRIES = 11;

    @Override
    public Class<Flow> getTypeClass() {
        return Flow.class;
    }

    @Override
    public UnsignedLong getDescriptorCode() {
        return Flow.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return Flow.DESCRIPTOR_SYMBOL;
    }

    @Override
    public Flow readValue(ProtonBuffer buffer, DecoderState state) throws IOException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        return readFlow(buffer, state, (ListTypeDecoder) decoder);
    }

    @Override
    public Flow[] readArrayElements(ProtonBuffer buffer, DecoderState state, int count) throws IOException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        Flow[] result = new Flow[count];
        for (int i = 0; i < count; ++i) {
            result[i] = readFlow(buffer, state, (ListTypeDecoder) decoder);
        }

        return result;
    }

    @Override
    public void skipValue(ProtonBuffer buffer, DecoderState state) throws IOException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        decoder.skipValue(buffer, state);
    }

    private Flow readFlow(ProtonBuffer buffer, DecoderState state, ListTypeDecoder listDecoder) throws IOException {
        Flow flow = new Flow();

        @SuppressWarnings("unused")
        int size = listDecoder.readSize(buffer);
        int count = listDecoder.readCount(buffer);

        // Don't decode anything if things already look wrong.
        if (count < MIN_FLOW_LIST_ENTRIES) {
            throw new IllegalStateException("Not enough entries in Flow list encoding: " + count);
        }
        if (count > MAX_FLOW_LIST_ENTRIES) {
            throw new IllegalStateException("To many entries in Flow list encoding: " + count);
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
                    flow.setNextIncomingId(state.getDecoder().readUnsignedInteger(buffer, state, 0l));
                    break;
                case 1:
                    flow.setIncomingWindow(state.getDecoder().readUnsignedInteger(buffer, state, 0l));
                    break;
                case 2:
                    flow.setNextOutgoingId(state.getDecoder().readUnsignedInteger(buffer, state, 0l));
                    break;
                case 3:
                    flow.setOutgoingWindow(state.getDecoder().readUnsignedInteger(buffer, state, 0l));
                    break;
                case 4:
                    flow.setHandle(state.getDecoder().readUnsignedInteger(buffer, state, 0l));
                    break;
                case 5:
                    flow.setDeliveryCount(state.getDecoder().readUnsignedInteger(buffer, state, 0l));
                    break;
                case 6:
                    flow.setLinkCredit(state.getDecoder().readUnsignedInteger(buffer, state, 0l));
                    break;
                case 7:
                    flow.setAvailable(state.getDecoder().readUnsignedInteger(buffer, state, 0l));
                    break;
                case 8:
                    flow.setDrain(state.getDecoder().readBoolean(buffer, state, false));
                    break;
                case 9:
                    flow.setEcho(state.getDecoder().readBoolean(buffer, state, false));
                    break;
                case 10:
                    flow.setProperties(state.getDecoder().readMap(buffer, state));
                    break;
                default:
                    throw new IllegalStateException("To many entries in Flow encoding");
            }
        }

        return flow;
    }
}
