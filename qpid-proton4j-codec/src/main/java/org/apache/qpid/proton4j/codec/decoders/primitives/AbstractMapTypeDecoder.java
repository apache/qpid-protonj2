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

import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.codec.DecoderState;
import org.apache.qpid.proton4j.codec.decoders.AbstractPrimitiveTypeDecoder;

/**
 * Base for the various Map type decoders used to read AMQP Map values.
 */
@SuppressWarnings("rawtypes")
public abstract class AbstractMapTypeDecoder extends AbstractPrimitiveTypeDecoder<Map> implements MapTypeDecoder {

    @Override
    public Map<Object, Object> readValue(ProtonBuffer buffer, DecoderState state) throws IOException {
        int size = readSize(buffer);
        int count = readCount(buffer);

        if (count > buffer.getReadableBytes()) {
            throw new IllegalArgumentException(String.format(
                    "Map encoded size %d is specified to be greater than the amount " +
                    "of data available (%d)", size, buffer.getReadableBytes()));
        }

        // Count include both key and value so we must include that in the loop
        Map<Object, Object> map = new LinkedHashMap<>(count);
        for (int i = 0; i < count / 2; i++) {
            Object key = state.getDecoder().readObject(buffer, state);
            Object value = state.getDecoder().readObject(buffer, state);

            map.put(key, value);
        }

        return map;
    }

    @Override
    public void skipValue(ProtonBuffer buffer, DecoderState state) throws IOException {
        buffer.skipBytes(readSize(buffer));
    }
}
