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
import java.util.List;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.DecoderState;
import org.apache.qpid.protonj2.codec.StreamDecoderState;
import org.apache.qpid.protonj2.codec.StreamTypeDecoder;
import org.apache.qpid.protonj2.codec.TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.AbstractDescribedTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.ListTypeDecoder;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.messaging.AmqpSequence;

/**
 * Decoder of AMQP Data type values from a byte stream.
 */
@SuppressWarnings("rawtypes")
public final class AmqpSequenceTypeDecoder extends AbstractDescribedTypeDecoder<AmqpSequence> {

    @Override
    public Class<AmqpSequence> getTypeClass() {
        return AmqpSequence.class;
    }

    @Override
    public UnsignedLong getDescriptorCode() {
        return AmqpSequence.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return AmqpSequence.DESCRIPTOR_SYMBOL;
    }

    @SuppressWarnings("unchecked")
    @Override
    public AmqpSequence<?> readValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);
        final ListTypeDecoder valueDecoder = checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder);
        final List<Object> result = valueDecoder.readValue(buffer, state);

        return new AmqpSequence<>(result);
    }

    @SuppressWarnings({ "unchecked" })
    @Override
    public AmqpSequence[] readArrayElements(ProtonBuffer buffer, DecoderState state, int count) throws DecodeException {
        final TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);
        final ListTypeDecoder valueDecoder = checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder);
        final List<Object>[] elements = valueDecoder.readArrayElements(buffer, state, count);

        AmqpSequence[] array = new AmqpSequence[count];
        for (int i = 0; i < count; ++i) {
            array[i] = new AmqpSequence(elements[i]);
        }

        return array;
    }

    @Override
    public void skipValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        decoder.skipValue(buffer, state);
    }

    @SuppressWarnings("unchecked")
    @Override
    public AmqpSequence readValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        final StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);
        final ListTypeDecoder valueDecoder = checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder);
        final List<Object> result = valueDecoder.readValue(stream, state);

        return new AmqpSequence<>(result);
    }

    @SuppressWarnings("unchecked")
    @Override
    public AmqpSequence[] readArrayElements(InputStream stream, StreamDecoderState state, int count) throws DecodeException {
        final StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);
        final ListTypeDecoder valueDecoder = checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder);
        final List<Object>[] elements = valueDecoder.readArrayElements(stream, state, count);

        AmqpSequence[] array = new AmqpSequence[count];
        for (int i = 0; i < count; ++i) {
            array[i] = new AmqpSequence(elements[i]);
        }

        return array;
    }

    @Override
    public void skipValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        final StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        decoder.skipValue(stream, state);
    }
}
