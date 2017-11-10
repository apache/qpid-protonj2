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
import org.apache.qpid.proton4j.codec.DescribedTypeDecoder;
import org.apache.qpid.proton4j.codec.TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.ListTypeDecoder;

/**
 * Decoder of AMQP Flow type values from a byte stream.
 */
public class FlowTypeDecoder implements DescribedTypeDecoder<Flow> {

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

        if (!(decoder instanceof ListTypeDecoder)) {
            throw new IOException("Expected List type indicator but got decoder for type: " + decoder.getTypeClass().getName());
        }

        ListTypeDecoder listDecoder = (ListTypeDecoder) decoder;
        Flow flow = new Flow();

        @SuppressWarnings("unused")
        int size = listDecoder.readSize(buffer);
        int count = listDecoder.readCount(buffer);

        for (int index = 0; index < count; ++index) {
            switch (index) {
                case 0:
                    flow.setNextIncomingId(state.getDecoder().readUnsignedInteger(buffer, state));
                    break;
                case 1:
                    flow.setIncomingWindow(state.getDecoder().readUnsignedInteger(buffer, state));
                    break;
                case 2:
                    flow.setNextOutgoingId(state.getDecoder().readUnsignedInteger(buffer, state));
                    break;
                case 3:
                    flow.setOutgoingWindow(state.getDecoder().readUnsignedInteger(buffer, state));
                    break;
                case 4:
                    flow.setHandle(state.getDecoder().readUnsignedInteger(buffer, state));
                    break;
                case 5:
                    flow.setDeliveryCount(state.getDecoder().readUnsignedInteger(buffer, state));
                    break;
                case 6:
                    flow.setLinkCredit(state.getDecoder().readUnsignedInteger(buffer, state));
                    break;
                case 7:
                    flow.setAvailable(state.getDecoder().readUnsignedInteger(buffer, state));
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

    @Override
    public void skipValue(ProtonBuffer buffer, DecoderState state) throws IOException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        if (!(decoder instanceof ListTypeDecoder)) {
            throw new IOException("Expected List type indicator but got decoder for type: " + decoder.getTypeClass().getName());
        }

        decoder.skipValue(buffer, state);
    }
}
