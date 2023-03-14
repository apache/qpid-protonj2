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
import org.apache.qpid.protonj2.codec.StreamDecoderState;
import org.apache.qpid.protonj2.codec.StreamTypeDecoder;
import org.apache.qpid.protonj2.codec.TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.AbstractDescribedListTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.ListTypeDecoder;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.messaging.DeleteOnClose;

/**
 * Decoder of AMQP DeleteOnClose type values from a byte stream
 */
public final class DeleteOnCloseTypeDecoder extends AbstractDescribedListTypeDecoder<DeleteOnClose> {

    @Override
    public Class<DeleteOnClose> getTypeClass() {
        return DeleteOnClose.class;
    }

    @Override
    public UnsignedLong getDescriptorCode() {
        return DeleteOnClose.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return DeleteOnClose.DESCRIPTOR_SYMBOL;
    }

    @Override
    public DeleteOnClose readValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        decoder.skipValue(buffer, state);

        return DeleteOnClose.getInstance();
    }

    @Override
    public DeleteOnClose[] readArrayElements(ProtonBuffer buffer, DecoderState state, int count) throws DecodeException {
        final TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        final DeleteOnClose[] result = new DeleteOnClose[count];

        for (int i = 0; i < count; ++i) {
            decoder.skipValue(buffer, state);
            result[i] = DeleteOnClose.getInstance();
        }

        return result;
    }

    @Override
    public DeleteOnClose readValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        final StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        decoder.skipValue(stream, state);

        return DeleteOnClose.getInstance();
    }

    @Override
    public DeleteOnClose[] readArrayElements(InputStream stream, StreamDecoderState state, int count) throws DecodeException {
        final StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        final DeleteOnClose[] result = new DeleteOnClose[count];

        for (int i = 0; i < count; ++i) {
            decoder.skipValue(stream, state);
            result[i] = DeleteOnClose.getInstance();
        }

        return result;
    }
}
