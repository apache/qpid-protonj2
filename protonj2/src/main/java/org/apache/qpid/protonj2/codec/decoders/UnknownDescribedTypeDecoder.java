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
        return getDescriptor() instanceof UnsignedLong ? (UnsignedLong) getDescriptor() : null;
    }

    @Override
    public final Symbol getDescriptorSymbol() {
        return getDescriptor() instanceof Symbol ? (Symbol) getDescriptor() : null;
    }

    @Override
    public final Class<DescribedType> getTypeClass() {
        return DescribedType.class;
    }

    @Override
    public final DescribedType readValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);
        Object described = decoder.readValue(buffer, state);

        return new UnknownDescribedType(getDescriptor(), described);
    }

    @Override
    public final DescribedType readValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);
        Object described = decoder.readValue(stream, state);

        return new UnknownDescribedType(getDescriptor(), described);
    }

    @Override
    public final DescribedType[] readArrayElements(ProtonBuffer buffer, DecoderState state, int count) throws DecodeException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        UnknownDescribedType[] result = new UnknownDescribedType[count];

        for (int i = 0; i < count; ++i) {
            Object described = decoder.readValue(buffer, state);
            result[i] = new UnknownDescribedType(getDescriptor(), described);
        }

        return result;
    }

    @Override
    public final DescribedType[] readArrayElements(InputStream stream, StreamDecoderState state, int count) throws DecodeException {
        StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        UnknownDescribedType[] result = new UnknownDescribedType[count];

        for (int i = 0; i < count; ++i) {
            Object described = decoder.readValue(stream, state);
            result[i] = new UnknownDescribedType(getDescriptor(), described);
        }

        return result;
    }

    @Override
    public final void skipValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        state.getDecoder().readNextTypeDecoder(buffer, state).skipValue(buffer, state);
    }

    @Override
    public final void skipValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        state.getDecoder().readNextTypeDecoder(stream, state).skipValue(stream, state);
    }
}
