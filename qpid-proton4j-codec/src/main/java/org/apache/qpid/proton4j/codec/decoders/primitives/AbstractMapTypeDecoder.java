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
package org.apache.qpid.proton4j.codec.decoders.primitives;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.qpid.proton4j.codec.DecoderState;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.apache.qpid.proton4j.codec.PrimitiveTypeDecoder;
import org.apache.qpid.proton4j.codec.TypeDecoder;

import io.netty.buffer.ByteBuf;

/**
 * Base for the various Map type decoders used to read AMQP Map values.
 */
public abstract class AbstractMapTypeDecoder implements MapTypeDecoder {

    @Override
    public Map<Object, Object> readValue(ByteBuf buffer, DecoderState state) throws IOException {
        int size = readSize(buffer);
        int count = readCount(buffer);

        // Ensure we do not allocate an array of size greater then the available data, otherwise there is a risk for an OOM error
        if (count > buffer.readableBytes()) {
            throw new IllegalArgumentException(String.format(
                    "Map encoded size %d is specified to be greater than the amount " +
                    "of data available (%d)", size, buffer.readableBytes()));
        }

        TypeDecoder<?> keyDecoder = null;
        TypeDecoder<?> valueDecoder = null;

        // Count include both key and value so we must include that in the loop
        Map<Object, Object> map = new LinkedHashMap<>(count);
        for (int i = 0; i < count / 2; i++) {
            keyDecoder = findNextDecoder(buffer, state, keyDecoder);
            Object key = keyDecoder.readValue(buffer, state);

            valueDecoder = findNextDecoder(buffer, state, valueDecoder);
            Object value = valueDecoder.readValue(buffer, state);

            map.put(key, value);
        }

        return map;
    }

    private TypeDecoder<?> findNextDecoder(ByteBuf buffer, DecoderState state, TypeDecoder<?> prevoudDecoder) throws IOException {
        if (prevoudDecoder == null) {
            return state.getDecoder().readNextTypeDecoder(buffer, state);
        } else {
            buffer.markReaderIndex();

            byte encodingCode = buffer.readByte();
            if (encodingCode == EncodingCodes.DESCRIBED_TYPE_INDICATOR || !(prevoudDecoder instanceof PrimitiveTypeDecoder<?>)) {
                buffer.resetReaderIndex();
                return state.getDecoder().readNextTypeDecoder(buffer, state);
            } else {
                PrimitiveTypeDecoder<?> primitiveTypeDecoder = (PrimitiveTypeDecoder<?>) prevoudDecoder;
                if (encodingCode != primitiveTypeDecoder.getTypeCode()) {
                    buffer.resetReaderIndex();
                    return state.getDecoder().readNextTypeDecoder(buffer, state);
                }
            }
        }

        return prevoudDecoder;
    }

    protected abstract int readSize(ByteBuf buffer);

    protected abstract int readCount(ByteBuf buffer);

}
