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
import java.util.ArrayList;
import java.util.List;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.DecoderState;
import org.apache.qpid.protonj2.codec.StreamDecoderState;
import org.apache.qpid.protonj2.codec.decoders.AbstractPrimitiveTypeDecoder;

/**
 * Base for the various List type decoders needed to read AMQP List values.
 */
@SuppressWarnings("rawtypes")
public abstract class AbstractListTypeDecoder extends AbstractPrimitiveTypeDecoder<List> implements ListTypeDecoder {

    @Override
    public List<Object> readValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final int size = readSize(buffer);

        // Ensure we do not allocate an array of size greater then the available data, otherwise there is a risk for an OOM error
        if (size > buffer.getReadableBytes()) {
            throw new DecodeException(String.format(
                    "List element size %d is specified to be greater than the amount " +
                    "of data available (%d)", size, buffer.getReadableBytes()));
        }

        final int count = readCount(buffer);

        if (count > buffer.getReadableBytes()) {
            throw new DecodeException(String.format(
                    "Symbol encoded element count %d is specified to be greater than the amount " +
                    "of data available (%d)", count, buffer.getReadableBytes()));
        }

        final List<Object> list = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            list.add(state.getDecoder().readObject(buffer, state));
        }

        return list;
    }

    @Override
    public void skipValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        buffer.skipBytes(readSize(buffer));
    }

    @Override
    public List<Object> readValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        readSize(stream);
        final int count = readCount(stream);

        final List<Object> list = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            list.add(state.getDecoder().readObject(stream, state));
        }

        return list;
    }

    @Override
    public void skipValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        try {
            stream.skip(readSize(stream));
        } catch (IOException ex) {
            throw new DecodeException("Error while reading List payload bytes", ex);
        }
    }
}
