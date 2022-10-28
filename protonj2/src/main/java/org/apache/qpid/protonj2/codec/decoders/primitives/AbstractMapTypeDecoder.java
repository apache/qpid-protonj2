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
package org.apache.qpid.protonj2.codec.decoders.primitives;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.Decoder;
import org.apache.qpid.protonj2.codec.DecoderState;
import org.apache.qpid.protonj2.codec.StreamDecoder;
import org.apache.qpid.protonj2.codec.StreamDecoderState;
import org.apache.qpid.protonj2.codec.decoders.AbstractPrimitiveTypeDecoder;

/**
 * Base for the various Map type decoders used to read AMQP Map values.
 */
@SuppressWarnings("rawtypes")
public abstract class AbstractMapTypeDecoder extends AbstractPrimitiveTypeDecoder<Map> implements MapTypeDecoder {

    @Override
    public Map<Object, Object> readValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final int size = readSize(buffer);

        if (size > buffer.getReadableBytes()) {
            throw new DecodeException(String.format(
                    "Map encoded size %d is specified to be greater than the amount " +
                    "of data available (%d)", size, buffer.getReadableBytes()));
        }

        final int count = readCount(buffer);

        if (count % 2 != 0) {
            throw new DecodeException(String.format(
                "Map encoded number of elements %d is not an even number.", count));
        }

        final Decoder decoder = state.getDecoder();

        // Count include both key and value so we must include that in the loop
        final Map<Object, Object> map = new LinkedHashMap<>(count);
        for (int i = 0; i < count / 2; i++) {
            Object key = decoder.readObject(buffer, state);
            Object value = decoder.readObject(buffer, state);

            map.put(key, value);
        }

        return map;
    }

    @Override
    public void skipValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        buffer.skipBytes(readSize(buffer));
    }

    @Override
    public Map<Object, Object> readValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        readSize(stream);
        final int count = readCount(stream);

        if (count % 2 != 0) {
            throw new DecodeException(String.format(
                "Map encoded number of elements %d is not an even number.", count));
        }

        final StreamDecoder decoder = state.getDecoder();

        // Count include both key and value so we must include that in the loop
        final Map<Object, Object> map = new LinkedHashMap<>(count);
        for (int i = 0; i < count / 2; i++) {
            Object key = decoder.readObject(stream, state);
            Object value = decoder.readObject(stream, state);

            map.put(key, value);
        }

        return map;
    }

    @Override
    public void skipValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        try {
            stream.skip(readSize(stream));
        } catch (IOException ex) {
            throw new DecodeException("Error while reading Map payload bytes", ex);
        }
    }
}
