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
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedLong;
import org.apache.qpid.proton4j.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.codec.Decoder;
import org.apache.qpid.proton4j.codec.DecoderState;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.apache.qpid.proton4j.codec.TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.AbstractDescribedTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.MapTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.NullTypeDecoder;

/**
 * Decoder of AMQP ApplicationProperties types from a byte stream
 */
public class ApplicationPropertiesTypeDecoder extends AbstractDescribedTypeDecoder<ApplicationProperties> {

    @Override
    public Class<ApplicationProperties> getTypeClass() {
        return ApplicationProperties.class;
    }

    @Override
    public UnsignedLong getDescriptorCode() {
        return ApplicationProperties.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return ApplicationProperties.DESCRIPTOR_SYMBOL;
    }

    @Override
    public ApplicationProperties readValue(ProtonBuffer buffer, DecoderState state) throws IOException {
        return new ApplicationProperties(readMap(buffer, state));
    }

    @Override
    public ApplicationProperties[] readArrayElements(ProtonBuffer buffer, DecoderState state, int count) throws IOException {
        ApplicationProperties[] result = new ApplicationProperties[count];

        for (int i = 0; i < count; ++i) {
            result[i] = new ApplicationProperties(readMap(buffer, state));
        }

        return result;
    }

    @Override
    public void skipValue(ProtonBuffer buffer, DecoderState state) throws IOException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        if (decoder instanceof NullTypeDecoder) {
            return;
        }

        if (!(decoder instanceof MapTypeDecoder)) {
            throw new IOException("Expected List type indicator but got decoder for type: " + decoder.getClass().getSimpleName());
        }

        decoder.skipValue(buffer, state);
    }

    private Map<String, Object> readMap(ProtonBuffer buffer, DecoderState state) throws IOException {
        final int size;
        final int count;

        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.MAP8:
                size = buffer.readByte() - 1;
                count = buffer.readByte();
                break;
            case EncodingCodes.MAP32:
                size = buffer.readInt() - 4;
                count = buffer.readInt();
                break;
            case EncodingCodes.NULL:
                return null;
            default:
                throw new IOException("Expected Map type but found encoding: " + encodingCode);
        }

        if (size > buffer.getReadableBytes()) {
            throw new IOException(String.format(
                    "Map encoded size %d is specified to be greater than the amount " +
                    "of data available (%d)", size, buffer.getReadableBytes()));
        }

        Decoder decoder = state.getDecoder();

        // Count include both key and value so we must include that in the loop
        Map<String, Object> map = new LinkedHashMap<>(count);
        for (int i = 0; i < count / 2; i++) {
            String key = decoder.readString(buffer, state);
            Object value = decoder.readObject(buffer, state);

            map.put(key, value);
        }

        return map;
    }
}
