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
package org.apache.qpid.protonj2.codec.decoders;

import java.io.InputStream;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.DecoderState;
import org.apache.qpid.protonj2.codec.StreamDecoderState;
import org.apache.qpid.protonj2.codec.StreamTypeDecoder;
import org.apache.qpid.protonj2.codec.TypeDecoder;
import org.apache.qpid.protonj2.types.DescribedType;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnknownDescribedType;
import org.apache.qpid.protonj2.types.UnsignedLong;

/**
 * Decoder of AMQP Described type values from a byte stream.
 */
public abstract class UnknownDescribedTypeDecoder extends AbstractDescribedTypeDecoder<DescribedType> {

    /**
     * @return the AMQP type descriptor for this {@link TypeDecoder}.
     */
    public abstract Object getDescriptor();

    @Override
    public final UnsignedLong getDescriptorCode() {
        return getDescriptor() instanceof UnsignedLong code ? code : null;
    }

    @Override
    public final Symbol getDescriptorSymbol() {
        return getDescriptor() instanceof Symbol symbol ? symbol : null;
    }

    @Override
    public final Class<DescribedType> getTypeClass() {
        return DescribedType.class;
    }

    @Override
    public final DescribedType readValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        if (!decoder.isPrimitive()) {
            throw new DecodeException("The described value must be an AMQP primitive type.");
        }

        return new UnknownDescribedType(getDescriptor(), decoder.readValue(buffer, state));
    }

    @Override
    public final DescribedType readValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        final StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        if (!decoder.isPrimitive()) {
            throw new DecodeException("The described value must be an AMQP primitive type.");
        }

        return new UnknownDescribedType(getDescriptor(), decoder.readValue(stream, state));
    }

    @Override
    public final DescribedType[] readArrayElements(ProtonBuffer buffer, DecoderState state, int count) throws DecodeException {
        final TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        if (!decoder.isPrimitive()) {
            throw new DecodeException("The described value must be an AMQP primitive type.");
        } else if (((PrimitiveTypeDecoder<?>) decoder).isZeroWidth()) {
            if (count > state.getMaxZeroWidthArrayElements()) {
                throw new DecodeException(String.format(
                    "Array element count %d is specified to be greater than limit for zero sized encoded array types (%d)",
                    count, state.getMaxZeroWidthArrayElements()));
            }
        }  else if (count > buffer.getReadableBytes()) {
            throw new DecodeException(String.format(
                "Array encoded element count %d is specified to be greater than the amount " +
                "of data available (%d)", count, buffer.getReadableBytes()));
        }

        final UnknownDescribedType[] result = new UnknownDescribedType[count];

        for (int i = 0; i < count; ++i) {
            result[i] = new UnknownDescribedType(getDescriptor(), decoder.readValue(buffer, state));
        }

        return result;
    }

    @Override
    public final DescribedType[] readArrayElements(InputStream stream, StreamDecoderState state, int count) throws DecodeException {
        final StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        if (!decoder.isPrimitive()) {
            throw new DecodeException("The described value must be an AMQP primitive type.");
        } else if (((PrimitiveTypeDecoder<?>) decoder).isZeroWidth()) {
            if (count > state.getMaxZeroWidthArrayElements()) {
                throw new DecodeException(String.format(
                    "Array element count %d is specified to be greater than limit for zero sized encoded array types (%d)",
                    count, state.getMaxZeroWidthArrayElements()));
            }
        } else if (count > state.getMaxArraySize()) {
            throw new DecodeException(String.format(
                "Array encoded length %d is specified to be greater than the amount " +
                "of the configured max array length (%d)", count, state.getMaxArraySize()));
        }

        final UnknownDescribedType[] result = new UnknownDescribedType[count];

        for (int i = 0; i < count; ++i) {
            result[i] = new UnknownDescribedType(getDescriptor(), decoder.readValue(stream, state));
        }

        return result;
    }

    @Override
    public final void skipValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        state.increaseDepth();

        try {
            state.getDecoder().readNextTypeDecoder(buffer, state).skipValue(buffer, state);
        } finally {
            state.decreaseDepth();
        }
    }

    @Override
    public final void skipValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        state.increaseDepth();

        try {
            state.getDecoder().readNextTypeDecoder(stream, state).skipValue(stream, state);
        } finally {
            state.decreaseDepth();
        }
    }
}
