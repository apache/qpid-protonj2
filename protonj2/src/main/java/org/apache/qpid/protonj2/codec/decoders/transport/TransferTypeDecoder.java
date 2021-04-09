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

import java.io.InputStream;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.DecoderState;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.StreamDecoderState;
import org.apache.qpid.protonj2.codec.StreamTypeDecoder;
import org.apache.qpid.protonj2.codec.TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.AbstractDescribedTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.ProtonStreamUtils;
import org.apache.qpid.protonj2.codec.decoders.primitives.ListTypeDecoder;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedByte;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.transport.DeliveryState;
import org.apache.qpid.protonj2.types.transport.ReceiverSettleMode;
import org.apache.qpid.protonj2.types.transport.Transfer;

/**
 * Decoder of AMQP Transfer type values from a byte stream
 */
public final class TransferTypeDecoder extends AbstractDescribedTypeDecoder<Transfer> {

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
    public Transfer readValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        return readTransfer(buffer, state, checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder));
    }

    @Override
    public Transfer[] readArrayElements(ProtonBuffer buffer, DecoderState state, int count) throws DecodeException {
        final TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        final Transfer[] result = new Transfer[count];
        for (int i = 0; i < count; ++i) {
            result[i] = readTransfer(buffer, state, checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder));
        }

        return result;
    }

    @Override
    public void skipValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        decoder.skipValue(buffer, state);
    }

    private Transfer readTransfer(ProtonBuffer buffer, DecoderState state, ListTypeDecoder listDecoder) throws DecodeException {
        final Transfer transfer = new Transfer();

        @SuppressWarnings("unused")
        final int size = listDecoder.readSize(buffer);
        final int count = listDecoder.readCount(buffer);

        // Don't decode anything if things already look wrong.
        if (count < MIN_TRANSFER_LIST_ENTRIES) {
            throw new DecodeException("The handle field cannot be omitted");
        }

        if (count > MAX_TRANSFER_LIST_ENTRIES) {
            throw new DecodeException("To many entries in Transfer list encoding: " + count);
        }

        for (int index = 0; index < count; ++index) {
            // Peek ahead and see if there is a null in the next slot, if so we don't call
            // the setter for that entry to ensure the returned type reflects the encoded
            // state in the modification entry.
            final boolean nullValue = buffer.getByte(buffer.getReadIndex()) == EncodingCodes.NULL;
            if (nullValue) {
                if (index == 0) {
                    throw new DecodeException("The handle field cannot be omitted");
                }

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
                    final UnsignedByte rcvSettleMode = state.getDecoder().readUnsignedByte(buffer, state);
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
            }
        }

        return transfer;
    }

    @Override
    public Transfer readValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        final StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        return readTransfer(stream, state, checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder));
    }

    @Override
    public Transfer[] readArrayElements(InputStream stream, StreamDecoderState state, int count) throws DecodeException {
        final StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        final Transfer[] result = new Transfer[count];
        for (int i = 0; i < count; ++i) {
            result[i] = readTransfer(stream, state, checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder));
        }

        return result;
    }

    @Override
    public void skipValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        final StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        decoder.skipValue(stream, state);
    }

    private Transfer readTransfer(InputStream stream, StreamDecoderState state, ListTypeDecoder listDecoder) throws DecodeException {
        final Transfer transfer = new Transfer();

        @SuppressWarnings("unused")
        final int size = listDecoder.readSize(stream);
        final int count = listDecoder.readCount(stream);

        // Don't decode anything if things already look wrong.
        if (count < MIN_TRANSFER_LIST_ENTRIES) {
            throw new DecodeException("The handle field cannot be omitted");
        }

        if (count > MAX_TRANSFER_LIST_ENTRIES) {
            throw new DecodeException("To many entries in Transfer list encoding: " + count);
        }

        for (int index = 0; index < count; ++index) {
            // If the stream allows we peek ahead and see if there is a null in the next slot,
            // if so we don't call the setter for that entry to ensure the returned type reflects
            // the encoded state in the modification entry.
            if (stream.markSupported()) {
                stream.mark(1);
                final boolean nullValue = ProtonStreamUtils.readByte(stream) == EncodingCodes.NULL;
                if (nullValue) {
                    if (index == 0) {
                        throw new DecodeException("The handle field cannot be omitted");
                    }

                    continue;
                } else {
                    ProtonStreamUtils.reset(stream);
                }
            }

            switch (index) {
                case 0:
                    transfer.setHandle(state.getDecoder().readUnsignedInteger(stream, state, 0l));
                    break;
                case 1:
                    transfer.setDeliveryId(state.getDecoder().readUnsignedInteger(stream, state, 0l));
                    break;
                case 2:
                    transfer.setDeliveryTag(state.getDecoder().readDeliveryTag(stream, state));
                    break;
                case 3:
                    transfer.setMessageFormat(state.getDecoder().readUnsignedInteger(stream, state, 0l));
                    break;
                case 4:
                    transfer.setSettled(state.getDecoder().readBoolean(stream, state, false));
                    break;
                case 5:
                    transfer.setMore(state.getDecoder().readBoolean(stream, state, false));
                    break;
                case 6:
                    final UnsignedByte rcvSettleMode = state.getDecoder().readUnsignedByte(stream, state);
                    transfer.setRcvSettleMode(rcvSettleMode == null ? null : ReceiverSettleMode.values()[rcvSettleMode.intValue()]);
                    break;
                case 7:
                    transfer.setState(state.getDecoder().readObject(stream, state, DeliveryState.class));
                    break;
                case 8:
                    transfer.setResume(state.getDecoder().readBoolean(stream, state, false));
                    break;
                case 9:
                    transfer.setAborted(state.getDecoder().readBoolean(stream, state, false));
                    break;
                case 10:
                    transfer.setBatchable(state.getDecoder().readBoolean(stream, state, false));
                    break;
            }
        }

        return transfer;
    }
}
