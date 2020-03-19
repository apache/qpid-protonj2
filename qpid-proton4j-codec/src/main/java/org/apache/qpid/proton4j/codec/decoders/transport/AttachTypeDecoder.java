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
import org.apache.qpid.proton4j.amqp.messaging.Source;
import org.apache.qpid.proton4j.amqp.messaging.Terminus;
import org.apache.qpid.proton4j.amqp.transport.Attach;
import org.apache.qpid.proton4j.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton4j.amqp.transport.Role;
import org.apache.qpid.proton4j.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.codec.DecoderState;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.apache.qpid.proton4j.codec.TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.AbstractDescribedTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.ListTypeDecoder;

/**
 * Decoder of AMQP Attach type values from a byte stream.
 */
public final class AttachTypeDecoder extends AbstractDescribedTypeDecoder<Attach> {

    private static final int MIN_ATTACH_LIST_ENTRIES = 3;
    private static final int MAX_ATTACH_LIST_ENTRIES = 14;

    @Override
    public Class<Attach> getTypeClass() {
        return Attach.class;
    }

    @Override
    public UnsignedLong getDescriptorCode() {
        return Attach.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return Attach.DESCRIPTOR_SYMBOL;
    }

    @Override
    public Attach readValue(ProtonBuffer buffer, DecoderState state) throws IOException {

        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        if (!(decoder instanceof ListTypeDecoder)) {
            throw new IOException("Expected List type indicator but got decoder for type: " + decoder.getTypeClass().getName());
        }

        return readAttach(buffer, state, (ListTypeDecoder) decoder);
    }

    @Override
    public Attach[] readArrayElements(ProtonBuffer buffer, DecoderState state, int count) throws IOException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        if (!(decoder instanceof ListTypeDecoder)) {
            throw new IOException("Expected List type indicator but got decoder for type: " + decoder.getTypeClass().getName());
        }

        Attach[] result = new Attach[count];
        for (int i = 0; i < count; ++i) {
            result[i] = readAttach(buffer, state, (ListTypeDecoder) decoder);
        }

        return result;
    }

    @Override
    public void skipValue(ProtonBuffer buffer, DecoderState state) throws IOException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        if (!(decoder instanceof ListTypeDecoder)) {
            throw new IOException("Expected List type indicator but got decoder for type: " + decoder.getTypeClass().getName());
        }

        decoder.skipValue(buffer, state);
    }

    private Attach readAttach(ProtonBuffer buffer, DecoderState state, ListTypeDecoder listDecoder) throws IOException {
        Attach attach = new Attach();

        @SuppressWarnings("unused")
        int size = listDecoder.readSize(buffer);
        int count = listDecoder.readCount(buffer);

        if (count < MIN_ATTACH_LIST_ENTRIES) {
            throw new IllegalStateException("Not enough entries in Attach list encoding: " + count);
        }
        if (count > MAX_ATTACH_LIST_ENTRIES) {
            throw new IllegalStateException("To many entries in Attach list encoding: " + count);
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
                    attach.setName(state.getDecoder().readString(buffer, state));
                    break;
                case 1:
                    attach.setHandle(state.getDecoder().readUnsignedInteger(buffer, state, 0l));
                    break;
                case 2:
                    Boolean role = state.getDecoder().readBoolean(buffer, state);
                    attach.setRole(Boolean.TRUE.equals(role) ? Role.RECEIVER : Role.SENDER);
                    break;
                case 3:
                    byte sndSettleMode = state.getDecoder().readUnsignedByte(buffer, state, (byte) 2);
                    attach.setSenderSettleMode(SenderSettleMode.valueOf(sndSettleMode));
                    break;
                case 4:
                    byte rcvSettleMode = state.getDecoder().readUnsignedByte(buffer, state, (byte) 0);
                    attach.setReceiverSettleMode(ReceiverSettleMode.valueOf(rcvSettleMode));
                    break;
                case 5:
                    attach.setSource(state.getDecoder().readObject(buffer, state, Source.class));
                    break;
                case 6:
                    attach.setTarget(state.getDecoder().readObject(buffer, state, Terminus.class));
                    break;
                case 7:
                    attach.setUnsettled(state.getDecoder().readMap(buffer, state));
                    break;
                case 8:
                    attach.setIncompleteUnsettled(state.getDecoder().readBoolean(buffer, state, true));
                    break;
                case 9:
                    attach.setInitialDeliveryCount(state.getDecoder().readUnsignedInteger(buffer, state, 0l));
                    break;
                case 10:
                    attach.setMaxMessageSize(state.getDecoder().readUnsignedLong(buffer, state));
                    break;
                case 11:
                    attach.setOfferedCapabilities(state.getDecoder().readMultiple(buffer, state, Symbol.class));
                    break;
                case 12:
                    attach.setDesiredCapabilities(state.getDecoder().readMultiple(buffer, state, Symbol.class));
                    break;
                case 13:
                    attach.setProperties(state.getDecoder().readMap(buffer, state));
                    break;
                default:
                    throw new IllegalStateException("To many entries in Attach encoding");
            }
        }

        return attach;
    }
}
