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
import org.apache.qpid.proton4j.amqp.UnsignedInteger;
import org.apache.qpid.proton4j.amqp.UnsignedLong;
import org.apache.qpid.proton4j.amqp.transport.Begin;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.codec.DecoderState;
import org.apache.qpid.proton4j.codec.DescribedTypeDecoder;
import org.apache.qpid.proton4j.codec.TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.ListTypeDecoder;

/**
 * Decoder of AMQP Begin type values from a byte stream
 */
public class BeginTypeDecoder implements DescribedTypeDecoder<Begin> {

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
    public Begin readValue(ProtonBuffer buffer, DecoderState state) throws IOException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        if (!(decoder instanceof ListTypeDecoder)) {
            throw new IOException("Expected List type indicator but got decoder for type: " + decoder.getTypeClass().getName());
        }

        ListTypeDecoder listDecoder = (ListTypeDecoder) decoder;
        Begin begin = new Begin();

        @SuppressWarnings("unused")
        int size = listDecoder.readSize(buffer);
        int count = listDecoder.readCount(buffer);

        for (int index = 0; index < count; ++index) {
            switch (index) {
                case 0:
                    begin.setRemoteChannel(state.getDecoder().readUnsignedShort(buffer, state));
                    break;
                case 1:
                    begin.setNextOutgoingId(state.getDecoder().readUnsignedInteger(buffer, state));
                    break;
                case 2:
                    begin.setIncomingWindow(state.getDecoder().readUnsignedInteger(buffer, state));
                    break;
                case 3:
                    begin.setOutgoingWindow(state.getDecoder().readUnsignedInteger(buffer, state));
                    break;
                case 4:
                    UnsignedInteger handleMax = state.getDecoder().readUnsignedInteger(buffer, state);
                    begin.setHandleMax(handleMax == null ? UnsignedInteger.MAX_VALUE : handleMax);
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
                    throw new IllegalStateException("To many entries in Begin encoding");
            }
        }

        return begin;
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
