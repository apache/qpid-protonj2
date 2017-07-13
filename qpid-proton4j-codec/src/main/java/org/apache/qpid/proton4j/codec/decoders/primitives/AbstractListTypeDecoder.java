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

import org.apache.qpid.proton4j.codec.DecoderState;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.apache.qpid.proton4j.codec.PrimitiveTypeDecoder;
import org.apache.qpid.proton4j.codec.TypeDecoder;

import io.netty.buffer.ByteBuf;

/**
 * Base for the various List type decoders needed to read AMQP List values.
 */
public abstract class AbstractListTypeDecoder implements ListTypeDecoder {

    @Override
    public List<Object> readValue(ByteBuf buffer, DecoderState state) throws IOException {
        int size = readSize(buffer);
        int count = readCount(buffer);

        // Ensure we do not allocate an array of size greater then the available data, otherwise there is a risk for an OOM error
        if (count > buffer.readableBytes()) {
            throw new IllegalArgumentException(String.format(
                    "List element size %d is specified to be greater than the amount " +
                    "of data available (%d)", size, buffer.readableBytes()));
        }

        TypeDecoder<?> typeDecoder = null;

        List<Object> list = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            // Whenever we can just reuse the previously used TypeDecoder instead
            // of spending time looking up the same one again.
            if (typeDecoder == null) {
                typeDecoder = state.getDecoder().readNextTypeDecoder(buffer, state);
            } else {
                buffer.markReaderIndex();

                byte encodingCode = buffer.readByte();
                if (encodingCode == EncodingCodes.DESCRIBED_TYPE_INDICATOR || !(typeDecoder instanceof PrimitiveTypeDecoder<?>)) {
                    buffer.resetReaderIndex();
                    typeDecoder = state.getDecoder().readNextTypeDecoder(buffer, state);
                } else {
                    PrimitiveTypeDecoder<?> primitiveTypeDecoder = (PrimitiveTypeDecoder<?>) typeDecoder;
                    if (encodingCode != primitiveTypeDecoder.getTypeCode()) {
                        buffer.resetReaderIndex();
                        typeDecoder = state.getDecoder().readNextTypeDecoder(buffer, state);
                    }
                }
            }

            list.add(typeDecoder.readValue(buffer, state));
        }

        return list;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public void readValue(ByteBuf buffer, DecoderState state, ListEntryHandler handler, Object target) throws IOException {
        int size = readSize(buffer);
        int count = readCount(buffer);

        // Ensure we do not allocate an array of size greater then the available data, otherwise there is a risk for an OOM error
        if (count > buffer.readableBytes()) {
            throw new IllegalArgumentException(String.format(
                    "List element size %d is specified to be greater than the amount " +
                    "of data available (%d)", size, buffer.readableBytes()));
        }

        for (int i = 0; i < count; i++) {
            handler.onListEntry(i, target, buffer, state);
        }
    }

    protected abstract int readSize(ByteBuf buffer);

    protected abstract int readCount(ByteBuf buffer);

}
