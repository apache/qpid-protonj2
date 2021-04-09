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
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.transport.DeliveryState;
import org.apache.qpid.protonj2.types.transport.Disposition;
import org.apache.qpid.protonj2.types.transport.Role;

/**
 * Decoder of AMQP Disposition type values from a byte stream.
 */
public final class DispositionTypeDecoder extends AbstractDescribedTypeDecoder<Disposition> {

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
    public Disposition readValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        return readDisposition(buffer, state, checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder));
    }

    @Override
    public Disposition[] readArrayElements(ProtonBuffer buffer, DecoderState state, int count) throws DecodeException {
        final TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        Disposition[] result = new Disposition[count];
        for (int i = 0; i < count; ++i) {
            result[i] = readDisposition(buffer, state, checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder));
        }

        return result;
    }

    @Override
    public void skipValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        decoder.skipValue(buffer, state);
    }

    private Disposition readDisposition(ProtonBuffer buffer, DecoderState state, ListTypeDecoder listDecoder) throws DecodeException {
        final Disposition disposition = new Disposition();

        @SuppressWarnings("unused")
        final int size = listDecoder.readSize(buffer);
        final int count = listDecoder.readCount(buffer);

        if (count < MIN_DISPOSITION_LIST_ENTRIES) {
            throw new DecodeException(errorForMissingRequiredFields(count));
        }

        if (count > MAX_DISPOSITION_LIST_ENTRIES) {
            throw new DecodeException("To many entries in Disposition list encoding: " + count);
        }

        for (int index = 0; index < count; ++index) {
            // Peek ahead and see if there is a null in the next slot, if so we don't call
            // the setter for that entry to ensure the returned type reflects the encoded
            // state in the modification entry.
            final boolean nullValue = buffer.getByte(buffer.getReadIndex()) == EncodingCodes.NULL;
            if (nullValue) {
                // Ensure mandatory fields are set
                if (index < MIN_DISPOSITION_LIST_ENTRIES) {
                    throw new DecodeException(errorForMissingRequiredFields(index));
                }

                buffer.readByte();
                continue;
            }

            switch (index) {
                case 0:
                    disposition.setRole(Boolean.TRUE.equals(state.getDecoder().readBoolean(buffer, state)) ? Role.RECEIVER : Role.SENDER);
                    break;
                case 1:
                    disposition.setFirst(state.getDecoder().readUnsignedInteger(buffer, state, 0l));
                    break;
                case 2:
                    disposition.setLast(state.getDecoder().readUnsignedInteger(buffer, state, 0l));
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
            }
        }

        return disposition;
    }

    private String errorForMissingRequiredFields(int present) {
        switch (present) {
            case 1:
                return "The first field cannot be omitted";
            default:
                return "The role field cannot be omitted";
        }
    }

    @Override
    public Disposition readValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        final StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        return readDisposition(stream, state, checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder));
    }

    @Override
    public Disposition[] readArrayElements(InputStream stream, StreamDecoderState state, int count) throws DecodeException {
        final StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        final Disposition[] result = new Disposition[count];
        for (int i = 0; i < count; ++i) {
            result[i] = readDisposition(stream, state, checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder));
        }

        return result;
    }

    @Override
    public void skipValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        final StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        decoder.skipValue(stream, state);
    }

    private Disposition readDisposition(InputStream stream, StreamDecoderState state, ListTypeDecoder listDecoder) throws DecodeException {
        final Disposition disposition = new Disposition();

        @SuppressWarnings("unused")
        final int size = listDecoder.readSize(stream);
        final int count = listDecoder.readCount(stream);

        if (count < MIN_DISPOSITION_LIST_ENTRIES) {
            throw new DecodeException(errorForMissingRequiredFields(count));
        }

        if (count > MAX_DISPOSITION_LIST_ENTRIES) {
            throw new DecodeException("To many entries in Disposition list encoding: " + count);
        }

        for (int index = 0; index < count; ++index) {
            // If the stream allows we peek ahead and see if there is a null in the next slot,
            // if so we don't call the setter for that entry to ensure the returned type reflects
            // the encoded state in the modification entry.
            if (stream.markSupported()) {
                stream.mark(1);
                final boolean nullValue = ProtonStreamUtils.readByte(stream) == EncodingCodes.NULL;
                if (nullValue) {
                    // Ensure mandatory fields are set
                    if (index < MIN_DISPOSITION_LIST_ENTRIES) {
                        throw new DecodeException(errorForMissingRequiredFields(index));
                    }

                    continue;
                } else {
                    ProtonStreamUtils.reset(stream);
                }
            }

            switch (index) {
                case 0:
                    disposition.setRole(Boolean.TRUE.equals(state.getDecoder().readBoolean(stream, state)) ? Role.RECEIVER : Role.SENDER);
                    break;
                case 1:
                    disposition.setFirst(state.getDecoder().readUnsignedInteger(stream, state, 0l));
                    break;
                case 2:
                    disposition.setLast(state.getDecoder().readUnsignedInteger(stream, state, 0l));
                    break;
                case 3:
                    disposition.setSettled(state.getDecoder().readBoolean(stream, state, false));
                    break;
                case 4:
                    disposition.setState(state.getDecoder().readObject(stream, state, DeliveryState.class));
                    break;
                case 5:
                    disposition.setBatchable(state.getDecoder().readBoolean(stream, state, false));
                    break;
            }
        }

        return disposition;
    }
}
