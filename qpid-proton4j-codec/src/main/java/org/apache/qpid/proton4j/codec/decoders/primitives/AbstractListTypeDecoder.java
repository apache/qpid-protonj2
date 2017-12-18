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
import java.util.ArrayList;
import java.util.List;

import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.codec.DecoderState;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.apache.qpid.proton4j.codec.TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.AbstractPrimitiveTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.PrimitiveTypeDecoder;

/**
 * Base for the various List type decoders needed to read AMQP List values.
 */
@SuppressWarnings("rawtypes")
public abstract class AbstractListTypeDecoder extends AbstractPrimitiveTypeDecoder<List> implements ListTypeDecoder {

    @Override
    public List<Object> readValue(ProtonBuffer buffer, DecoderState state) throws IOException {
        int size = readSize(buffer);
        int count = readCount(buffer);

        // Ensure we do not allocate an array of size greater then the available data, otherwise there is a risk for an OOM error
        if (count > buffer.getReadableBytes()) {
            throw new IllegalArgumentException(String.format(
                    "List element size %d is specified to be greater than the amount " +
                    "of data available (%d)", size, buffer.getReadableBytes()));
        }

        TypeDecoder<?> typeDecoder = null;

        List<Object> list = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            // Whenever we can just reuse the previously used TypeDecoder instead
            // of spending time looking up the same one again.
            byte encodingCode = buffer.getByte(buffer.getReadIndex());
            if (encodingCode == EncodingCodes.DESCRIBED_TYPE_INDICATOR || !(typeDecoder instanceof PrimitiveTypeDecoder<?>)) {
                typeDecoder = state.getDecoder().readNextTypeDecoder(buffer, state);
            } else {
                PrimitiveTypeDecoder<?> primitiveTypeDecoder = (PrimitiveTypeDecoder<?>) typeDecoder;
                if (encodingCode != primitiveTypeDecoder.getTypeCode()) {
                    typeDecoder = state.getDecoder().readNextTypeDecoder(buffer, state);
                } else {
                    buffer.readByte();  // Reusing the previous decoder, consume the type code.
                }
            }

            list.add(typeDecoder.readValue(buffer, state));
        }

        return list;
    }

    @Override
    public void skipValue(ProtonBuffer buffer, DecoderState state) throws IOException {
        buffer.skipBytes(readSize(buffer));
    }
}
