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
package org.apache.qpid.proton4j.codec.decoders.messaging;

import java.io.IOException;
import java.util.Date;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedLong;
import org.apache.qpid.proton4j.amqp.messaging.Properties;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.codec.DecoderState;
import org.apache.qpid.proton4j.codec.TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.AbstractDescribedTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.ListTypeDecoder;

/**
 * Decoder of AMQP Properties type values from a byte stream
 */
public class PropertiesTypeDecoder extends AbstractDescribedTypeDecoder<Properties> {

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
    public Properties readValue(ProtonBuffer buffer, DecoderState state) throws IOException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        if (!(decoder instanceof ListTypeDecoder)) {
            throw new IOException("Expected List type indicator but got decoder for type: " + decoder.getTypeClass().getName());
        }

        return readProperties(buffer, state, (ListTypeDecoder) decoder);
    }

    @Override
    public Properties[] readArrayElements(ProtonBuffer buffer, DecoderState state, int count) throws IOException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        if (!(decoder instanceof ListTypeDecoder)) {
            throw new IOException("Expected List type indicator but got decoder for type: " + decoder.getTypeClass().getName());
        }

        Properties[] result = new Properties[count];
        for (int i = 0; i < count; ++i) {
            result[i] = readProperties(buffer, state, (ListTypeDecoder) decoder);
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

    private Properties readProperties(ProtonBuffer buffer, DecoderState state, ListTypeDecoder listDecoder) throws IOException {
        Properties properties = new Properties();

        @SuppressWarnings("unused")
        int size = listDecoder.readSize(buffer);
        int count = listDecoder.readCount(buffer);

        for (int index = 0; index < count; ++index) {
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
                    properties.setContentType(state.getDecoder().readSymbol(buffer, state));
                    break;
                case 7:
                    properties.setContentEncoding(state.getDecoder().readSymbol(buffer, state));
                    break;
                case 8:
                    Long expireyTime = state.getDecoder().readTimestamp(buffer, state);
                    properties.setAbsoluteExpiryTime(expireyTime == null ? null : new Date(expireyTime));
                    break;
                case 9:
                    Long createTime = state.getDecoder().readTimestamp(buffer, state);
                    properties.setCreationTime(createTime == null ? null : new Date(createTime));
                    break;
                case 10:
                    properties.setGroupId(state.getDecoder().readString(buffer, state));
                    break;
                case 11:
                    properties.setGroupSequence(state.getDecoder().readUnsignedInteger(buffer, state));
                    break;
                case 12:
                    properties.setReplyToGroupId(state.getDecoder().readString(buffer, state));
                    break;
                default:
                    throw new IllegalStateException("To many entries in Properties encoding");
            }
        }

        return properties;
    }
}
