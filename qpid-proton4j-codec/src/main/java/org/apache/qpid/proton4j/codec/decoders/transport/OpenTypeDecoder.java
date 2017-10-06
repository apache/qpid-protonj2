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
import org.apache.qpid.proton4j.amqp.UnsignedShort;
import org.apache.qpid.proton4j.amqp.transport.Open;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.codec.DecoderState;
import org.apache.qpid.proton4j.codec.DescribedTypeDecoder;
import org.apache.qpid.proton4j.codec.TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.ListTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.ListTypeDecoder.ListEntryHandler;

/**
 * Decoder of AMQP Open type values from a byte stream.
 */
public class OpenTypeDecoder implements DescribedTypeDecoder<Open>, ListEntryHandler<Open> {

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
    public Open readValue(ProtonBuffer buffer, DecoderState state) throws IOException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        if (!(decoder instanceof ListTypeDecoder)) {
            throw new IOException("Expected List type indicator but got decoder for type: " + decoder.getTypeClass().getName());
        }

        ListTypeDecoder listDecoder = (ListTypeDecoder) decoder;
        Open open = new Open();

        listDecoder.readValue(buffer, state, this, open);

        return open;
    }

    @Override
    public void onListEntry(int index, Open open, ProtonBuffer buffer, DecoderState state) throws IOException {
        switch (index) {
            case 0:
                open.setContainerId(state.getDecoder().readString(buffer, state));
                break;
            case 1:
                open.setHostname(state.getDecoder().readString(buffer, state));
                break;
            case 2:
                UnsignedInteger maxFrameSize = state.getDecoder().readUnsignedInteger(buffer, state);
                open.setMaxFrameSize(maxFrameSize == null ? UnsignedInteger.MAX_VALUE : maxFrameSize);
                break;
            case 3:
                UnsignedShort channelMax = state.getDecoder().readUnsignedShort(buffer, state);
                open.setChannelMax(channelMax == null ? UnsignedShort.MAX_VALUE : channelMax);
                break;
            case 4:
                open.setIdleTimeOut(state.getDecoder().readUnsignedInteger(buffer, state));
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
                throw new IllegalStateException("To many entries in Open encoding");
        }
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
