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
import org.apache.qpid.proton4j.amqp.transport.DeliveryState;
import org.apache.qpid.proton4j.amqp.transport.Disposition;
import org.apache.qpid.proton4j.amqp.transport.Role;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.codec.DecoderState;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.apache.qpid.proton4j.codec.TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.AbstractDescribedTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.ListTypeDecoder;

/**
 * Decoder of AMQP Disposition type values from a byte stream.
 */
public class DispositionTypeDecoder extends AbstractDescribedTypeDecoder<Disposition> {

    private static final int MIN_DISPOSITION_LIST_ENTRIES = 2;
    private static final int MAX_DISPOSITION_LIST_ENTRIES = 6;

    @Override
    public Class<Disposition> getTypeClass() {
        return Disposition.class;
    }

    @Override
    public UnsignedLong getDescriptorCode() {
        return Disposition.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return Disposition.DESCRIPTOR_SYMBOL;
    }

    @Override
    public Disposition readValue(ProtonBuffer buffer, DecoderState state) throws IOException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        if (!(decoder instanceof ListTypeDecoder)) {
            throw new IOException("Expected List type indicator but got decoder for type: " + decoder.getTypeClass().getName());
        }

        return readDisposition(buffer, state, (ListTypeDecoder) decoder);
    }

    @Override
    public Disposition[] readArrayElements(ProtonBuffer buffer, DecoderState state, int count) throws IOException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        if (!(decoder instanceof ListTypeDecoder)) {
            throw new IOException("Expected List type indicator but got decoder for type: " + decoder.getTypeClass().getName());
        }

        Disposition[] result = new Disposition[count];
        for (int i = 0; i < count; ++i) {
            result[i] = readDisposition(buffer, state, (ListTypeDecoder) decoder);
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

    private Disposition readDisposition(ProtonBuffer buffer, DecoderState state, ListTypeDecoder listDecoder) throws IOException {
        Disposition disposition = new Disposition();

        @SuppressWarnings("unused")
        int size = listDecoder.readSize(buffer);
        int count = listDecoder.readCount(buffer);

        // TODO - Validate that mandatory fields are present, what error ? Here or further up the chain
        if (count < MIN_DISPOSITION_LIST_ENTRIES) {
            throw new IllegalStateException("Not enough entries in Disposition list encoding: " + count);
        }

        if (count > MAX_DISPOSITION_LIST_ENTRIES) {
            throw new IllegalStateException("To many entries in Disposition list encoding: " + count);
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
                    disposition.setRole(Boolean.TRUE.equals(state.getDecoder().readBoolean(buffer, state)) ? Role.RECEIVER : Role.SENDER);
                    break;
                case 1:
                    disposition.setFirst(state.getDecoder().readUnsignedInteger(buffer, state, 0));
                    break;
                case 2:
                    disposition.setLast(state.getDecoder().readUnsignedInteger(buffer, state, 0));
                    break;
                case 3:
                    disposition.setSettled(state.getDecoder().readBoolean(buffer, state, false));
                    break;
                case 4:
                    disposition.setState(state.getDecoder().readObject(buffer, state, DeliveryState.class));
                    break;
                case 5:
                    disposition.setBatchable(state.getDecoder().readBoolean(buffer, state, false));
                    break;
                default:
                    throw new IllegalStateException("To many entries in Disposition encoding");
            }
        }

        return disposition;
    }
}
