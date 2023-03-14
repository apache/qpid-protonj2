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
package org.apache.qpid.protonj2.codec.decoders.messaging;

import java.io.InputStream;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.DecoderState;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.StreamDecoderState;
import org.apache.qpid.protonj2.codec.StreamTypeDecoder;
import org.apache.qpid.protonj2.codec.TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.AbstractDescribedListTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.ProtonStreamUtils;
import org.apache.qpid.protonj2.codec.decoders.primitives.ListTypeDecoder;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.messaging.Properties;

/**
 * Decoder of AMQP Properties type values from a byte stream
 */
public final class PropertiesTypeDecoder extends AbstractDescribedListTypeDecoder<Properties> {

    private static final int MIN_PROPERTIES_LIST_ENTRIES = 0;
    private static final int MAX_PROPERTIES_LIST_ENTRIES = 13;

    @Override
    public UnsignedLong getDescriptorCode() {
        return Properties.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return Properties.DESCRIPTOR_SYMBOL;
    }

    @Override
    public Class<Properties> getTypeClass() {
        return Properties.class;
    }

    @Override
    public Properties readValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        return readProperties(buffer, state, checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder));
    }

    @Override
    public Properties[] readArrayElements(ProtonBuffer buffer, DecoderState state, int count) throws DecodeException {
        final TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);
        final ListTypeDecoder listDecoder = checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder);

        final Properties[] result = new Properties[count];
        for (int i = 0; i < count; ++i) {
            result[i] = readProperties(buffer, state, listDecoder);
        }

        return result;
    }

    private Properties readProperties(ProtonBuffer buffer, DecoderState state, ListTypeDecoder listDecoder) throws DecodeException {
        final Properties properties = new Properties();

        @SuppressWarnings("unused")
        final int size = listDecoder.readSize(buffer, state);
        final int count = listDecoder.readCount(buffer, state);

        // Don't decode anything if things already look wrong.
        if (count < MIN_PROPERTIES_LIST_ENTRIES) {
            throw new DecodeException("Not enough entries in Properties list encoding: " + count);
        }

        if (count > MAX_PROPERTIES_LIST_ENTRIES) {
            throw new DecodeException("To many entries in Properties list encoding: " + count);
        }

        for (int index = 0; index < count; ++index) {
            // Peek ahead and see if there is a null in the next slot, if so we don't call
            // the setter for that entry to ensure the returned type reflects the encoded
            // state in the modification entry.
            boolean nullValue = buffer.getByte(buffer.getReadOffset()) == EncodingCodes.NULL;
            if (nullValue) {
                buffer.readByte();
                continue;
            }

            switch (index) {
                case 0:
                    properties.setMessageId(state.getDecoder().readObject(buffer, state));
                    break;
                case 1:
                    properties.setUserId(state.getDecoder().readBinary(buffer, state));
                    break;
                case 2:
                    properties.setTo(state.getDecoder().readString(buffer, state));
                    break;
                case 3:
                    properties.setSubject(state.getDecoder().readString(buffer, state));
                    break;
                case 4:
                    properties.setReplyTo(state.getDecoder().readString(buffer, state));
                    break;
                case 5:
                    properties.setCorrelationId(state.getDecoder().readObject(buffer, state));
                    break;
                case 6:
                    properties.setContentType(state.getDecoder().readSymbol(buffer, state, null));
                    break;
                case 7:
                    properties.setContentEncoding(state.getDecoder().readSymbol(buffer, state, null));
                    break;
                case 8:
                    properties.setAbsoluteExpiryTime(state.getDecoder().readTimestamp(buffer, state, 0l));
                    break;
                case 9:
                    properties.setCreationTime(state.getDecoder().readTimestamp(buffer, state, 0l));
                    break;
                case 10:
                    properties.setGroupId(state.getDecoder().readString(buffer, state));
                    break;
                case 11:
                    properties.setGroupSequence(state.getDecoder().readUnsignedInteger(buffer, state, 0l));
                    break;
                case 12:
                    properties.setReplyToGroupId(state.getDecoder().readString(buffer, state));
                    break;
            }
        }

        return properties;
    }

    @Override
    public Properties readValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        final StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        return readProperties(stream, state, checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder));
    }

    @Override
    public Properties[] readArrayElements(InputStream stream, StreamDecoderState state, int count) throws DecodeException {
        final StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);
        final ListTypeDecoder listDecoder = checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder);

        final Properties[] result = new Properties[count];
        for (int i = 0; i < count; ++i) {
            result[i] = readProperties(stream, state, listDecoder);
        }

        return result;
    }

    private Properties readProperties(InputStream stream, StreamDecoderState state, ListTypeDecoder listDecoder) throws DecodeException {
        final Properties properties = new Properties();

        @SuppressWarnings("unused")
        final int size = listDecoder.readSize(stream, state);
        final int count = listDecoder.readCount(stream, state);

        // Don't decode anything if things already look wrong.
        if (count < MIN_PROPERTIES_LIST_ENTRIES) {
            throw new DecodeException("Not enough entries in Properties list encoding: " + count);
        }

        if (count > MAX_PROPERTIES_LIST_ENTRIES) {
            throw new DecodeException("To many entries in Properties list encoding: " + count);
        }

        for (int index = 0; index < count; ++index) {
            // If the stream allows we peek ahead and see if there is a null in the next slot,
            // if so we don't call the setter for that entry to ensure the returned type reflects
            // the encoded state in the modification entry.
            if (stream.markSupported()) {
                stream.mark(1);
                if (ProtonStreamUtils.readByte(stream) == EncodingCodes.NULL) {
                    continue;
                } else {
                    ProtonStreamUtils.reset(stream);
                }
            }

            switch (index) {
                case 0:
                    properties.setMessageId(state.getDecoder().readObject(stream, state));
                    break;
                case 1:
                    properties.setUserId(state.getDecoder().readBinary(stream, state));
                    break;
                case 2:
                    properties.setTo(state.getDecoder().readString(stream, state));
                    break;
                case 3:
                    properties.setSubject(state.getDecoder().readString(stream, state));
                    break;
                case 4:
                    properties.setReplyTo(state.getDecoder().readString(stream, state));
                    break;
                case 5:
                    properties.setCorrelationId(state.getDecoder().readObject(stream, state));
                    break;
                case 6:
                    properties.setContentType(state.getDecoder().readSymbol(stream, state, null));
                    break;
                case 7:
                    properties.setContentEncoding(state.getDecoder().readSymbol(stream, state, null));
                    break;
                case 8:
                    properties.setAbsoluteExpiryTime(state.getDecoder().readTimestamp(stream, state, 0l));
                    break;
                case 9:
                    properties.setCreationTime(state.getDecoder().readTimestamp(stream, state, 0l));
                    break;
                case 10:
                    properties.setGroupId(state.getDecoder().readString(stream, state));
                    break;
                case 11:
                    properties.setGroupSequence(state.getDecoder().readUnsignedInteger(stream, state, 0l));
                    break;
                case 12:
                    properties.setReplyToGroupId(state.getDecoder().readString(stream, state));
                    break;
            }
        }

        return properties;
    }
}
