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
import org.apache.qpid.proton4j.amqp.UnsignedByte;
import org.apache.qpid.proton4j.amqp.UnsignedLong;
import org.apache.qpid.proton4j.amqp.transport.DeliveryState;
import org.apache.qpid.proton4j.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton4j.amqp.transport.Transfer;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.codec.DecoderState;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.apache.qpid.proton4j.codec.TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.AbstractDescribedTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.ListTypeDecoder;

/**
 * Decoder of AMQP Transfer type values from a byte stream
 */
public class TransferTypeDecoder extends AbstractDescribedTypeDecoder<Transfer> {

    private static final int MIN_TRANSFER_LIST_ENTRIES = 1;
    private static final int MAX_TRANSFER_LIST_ENTRIES = 11;

    @Override
    public Class<Transfer> getTypeClass() {
        return Transfer.class;
    }

    @Override
    public UnsignedLong getDescriptorCode() {
        return Transfer.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return Transfer.DESCRIPTOR_SYMBOL;
    }

    @Override
    public Transfer readValue(ProtonBuffer buffer, DecoderState state) throws IOException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        if (!(decoder instanceof ListTypeDecoder)) {
            throw new IOException("Expected List type indicator but got decoder for type: " + decoder.getTypeClass().getName());
        }

        return readTransfer(buffer, state, (ListTypeDecoder) decoder);
    }

    @Override
    public Transfer[] readArrayElements(ProtonBuffer buffer, DecoderState state, int count) throws IOException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        if (!(decoder instanceof ListTypeDecoder)) {
            throw new IOException("Expected List type indicator but got decoder for type: " + decoder.getTypeClass().getName());
        }

        Transfer[] result = new Transfer[count];
        for (int i = 0; i < count; ++i) {
            result[i] = readTransfer(buffer, state, (ListTypeDecoder) decoder);
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

    private Transfer readTransfer(ProtonBuffer buffer, DecoderState state, ListTypeDecoder listDecoder) throws IOException {
        Transfer transfer = new Transfer();

        @SuppressWarnings("unused")
        int size = listDecoder.readSize(buffer);
        int count = listDecoder.readCount(buffer);

        // TODO - Decoding correctness checks

        // How much checking do we do, and do we provide a specific exception type for these
        // decoding errors so the transport can tell if the error is fatal or not.

        // Don't decode anything if things already look wrong.
        if (count < MIN_TRANSFER_LIST_ENTRIES) {
            throw new IllegalStateException("Not enough entries in Transfer list encoding: " + count);
        }

        if (count > MAX_TRANSFER_LIST_ENTRIES) {
            throw new IllegalStateException("To many entries in Transfer list encoding: " + count);
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
                    transfer.setHandle(state.getDecoder().readUnsignedInteger(buffer, state, 0l));
                    break;
                case 1:
                    transfer.setDeliveryId(state.getDecoder().readUnsignedInteger(buffer, state, 0l));
                    break;
                case 2:
                    transfer.setDeliveryTag(state.getDecoder().readDeliveryTag(buffer, state));
                    break;
                case 3:
                    transfer.setMessageFormat(state.getDecoder().readUnsignedInteger(buffer, state, 0l));
                    break;
                case 4:
                    transfer.setSettled(state.getDecoder().readBoolean(buffer, state, false));
                    break;
                case 5:
                    transfer.setMore(state.getDecoder().readBoolean(buffer, state, false));
                    break;
                case 6:
                    UnsignedByte rcvSettleMode = state.getDecoder().readUnsignedByte(buffer, state);
                    transfer.setRcvSettleMode(rcvSettleMode == null ? null : ReceiverSettleMode.values()[rcvSettleMode.intValue()]);
                    break;
                case 7:
                    transfer.setState(state.getDecoder().readObject(buffer, state, DeliveryState.class));
                    break;
                case 8:
                    transfer.setResume(state.getDecoder().readBoolean(buffer, state, false));
                    break;
                case 9:
                    transfer.setAborted(state.getDecoder().readBoolean(buffer, state, false));
                    break;
                case 10:
                    transfer.setBatchable(state.getDecoder().readBoolean(buffer, state, false));
                    break;
                default:
                    throw new IllegalStateException("To many entries in Transfer encoding");
            }
        }

        return transfer;
    }
}
