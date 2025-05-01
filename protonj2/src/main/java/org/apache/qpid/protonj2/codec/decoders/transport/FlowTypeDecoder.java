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
import org.apache.qpid.protonj2.codec.Decoder;
import org.apache.qpid.protonj2.codec.DecoderState;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.StreamDecoder;
import org.apache.qpid.protonj2.codec.StreamDecoderState;
import org.apache.qpid.protonj2.codec.StreamTypeDecoder;
import org.apache.qpid.protonj2.codec.TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.AbstractDescribedListTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.ProtonStreamUtils;
import org.apache.qpid.protonj2.codec.decoders.primitives.ListTypeDecoder;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.transport.Flow;

/**
 * Decoder of AMQP Flow type values from a byte stream.
 */
public final class FlowTypeDecoder extends AbstractDescribedListTypeDecoder<Flow> {

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
    public Flow readValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        return readFlow(buffer, state.getDecoder(), state, checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder));
    }

    @Override
    public Flow[] readArrayElements(ProtonBuffer buffer, DecoderState state, int count) throws DecodeException {
        final TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        final Flow[] result = new Flow[count];
        for (int i = 0; i < count; ++i) {
            result[i] = readFlow(buffer, state.getDecoder(), state, checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder));
        }

        return result;
    }

    private Flow readFlow(ProtonBuffer buffer, Decoder decoder, DecoderState state, ListTypeDecoder listDecoder) throws DecodeException {
        final Flow flow = new Flow();

        @SuppressWarnings("unused")
        final int size = listDecoder.readSize(buffer, state);
        final int count = listDecoder.readCount(buffer, state);

        // Don't decode anything if things already look wrong.
        if (count < MIN_FLOW_LIST_ENTRIES) {
            throw new DecodeException(errorForMissingRequiredFields(count));
        }

        if (count > MAX_FLOW_LIST_ENTRIES) {
            throw new DecodeException("To many entries in Flow list encoding: " + count);
        }

        for (int index = 0; index < count; ++index) {
            // Peek ahead and see if there is a null in the next slot, if so we don't call
            // the setter for that entry to ensure the returned type reflects the encoded
            // state in the modification entry.
            if (buffer.peekByte() == EncodingCodes.NULL) {
                // Ensure mandatory fields are set
                if (index > 0 && index < MIN_FLOW_LIST_ENTRIES) {
                    throw new DecodeException(errorForMissingRequiredFields(index));
                }

                buffer.advanceReadOffset(1);
                continue;
            }

            switch (index) {
                case 0:
                    flow.setNextIncomingId(decoder.readUnsignedInteger(buffer, state, 0l));
                    break;
                case 1:
                    flow.setIncomingWindow(decoder.readUnsignedInteger(buffer, state, 0l));
                    break;
                case 2:
                    flow.setNextOutgoingId(decoder.readUnsignedInteger(buffer, state, 0l));
                    break;
                case 3:
                    flow.setOutgoingWindow(decoder.readUnsignedInteger(buffer, state, 0l));
                    break;
                case 4:
                    flow.setHandle(decoder.readUnsignedInteger(buffer, state, 0l));
                    break;
                case 5:
                    flow.setDeliveryCount(decoder.readUnsignedInteger(buffer, state, 0l));
                    break;
                case 6:
                    flow.setLinkCredit(decoder.readUnsignedInteger(buffer, state, 0l));
                    break;
                case 7:
                    flow.setAvailable(decoder.readUnsignedInteger(buffer, state, 0l));
                    break;
                case 8:
                    flow.setDrain(decoder.readBoolean(buffer, state, false));
                    break;
                case 9:
                    flow.setEcho(decoder.readBoolean(buffer, state, false));
                    break;
                case 10:
                    flow.setProperties(decoder.readMap(buffer, state));
                    break;
            }
        }

        return flow;
    }

    @Override
    public Flow readValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        final StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        return readFlow(stream, state.getDecoder(), state, checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder));
    }

    @Override
    public Flow[] readArrayElements(InputStream stream, StreamDecoderState state, int count) throws DecodeException {
        final StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        final Flow[] result = new Flow[count];
        for (int i = 0; i < count; ++i) {
            result[i] = readFlow(stream, state.getDecoder(), state, checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder));
        }

        return result;
    }

    private Flow readFlow(InputStream stream, StreamDecoder decoder, StreamDecoderState state, ListTypeDecoder listDecoder) throws DecodeException {
        final Flow flow = new Flow();

        @SuppressWarnings("unused")
        final int size = listDecoder.readSize(stream, state);
        final int count = listDecoder.readCount(stream, state);

        // Don't decode anything if things already look wrong.
        if (count < MIN_FLOW_LIST_ENTRIES) {
            throw new DecodeException(errorForMissingRequiredFields(count));
        }

        if (count > MAX_FLOW_LIST_ENTRIES) {
            throw new DecodeException("To many entries in Flow list encoding: " + count);
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
                    if (index > 0 && index < MIN_FLOW_LIST_ENTRIES) {
                        throw new DecodeException(errorForMissingRequiredFields(index));
                    }

                    continue;
                } else {
                    ProtonStreamUtils.reset(stream);
                }
            }

            switch (index) {
                case 0:
                    flow.setNextIncomingId(decoder.readUnsignedInteger(stream, state, 0l));
                    break;
                case 1:
                    flow.setIncomingWindow(decoder.readUnsignedInteger(stream, state, 0l));
                    break;
                case 2:
                    flow.setNextOutgoingId(decoder.readUnsignedInteger(stream, state, 0l));
                    break;
                case 3:
                    flow.setOutgoingWindow(decoder.readUnsignedInteger(stream, state, 0l));
                    break;
                case 4:
                    flow.setHandle(decoder.readUnsignedInteger(stream, state, 0l));
                    break;
                case 5:
                    flow.setDeliveryCount(decoder.readUnsignedInteger(stream, state, 0l));
                    break;
                case 6:
                    flow.setLinkCredit(decoder.readUnsignedInteger(stream, state, 0l));
                    break;
                case 7:
                    flow.setAvailable(decoder.readUnsignedInteger(stream, state, 0l));
                    break;
                case 8:
                    flow.setDrain(decoder.readBoolean(stream, state, false));
                    break;
                case 9:
                    flow.setEcho(decoder.readBoolean(stream, state, false));
                    break;
                case 10:
                    flow.setProperties(decoder.readMap(stream, state));
                    break;
            }
        }

        return flow;
    }

    private String errorForMissingRequiredFields(int present) {
        switch (present) {
            case 3:
                return "The outgoing-window field cannot be omitted from the Flow";
            case 2:
                return "The next-outgoing-id field cannot be omitted from the Flow";
            default:
                return "The incoming-window field cannot be omitted from the Flow";
        }
    }
}
