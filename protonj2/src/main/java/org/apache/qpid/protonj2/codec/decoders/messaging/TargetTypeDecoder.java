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
import org.apache.qpid.protonj2.codec.decoders.AbstractDescribedTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.ListTypeDecoder;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedInteger;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.messaging.Target;
import org.apache.qpid.protonj2.types.messaging.TerminusDurability;
import org.apache.qpid.protonj2.types.messaging.TerminusExpiryPolicy;

/**
 * Decoder of AMQP Target type values from a byte stream
 */
public final class TargetTypeDecoder extends AbstractDescribedTypeDecoder<Target> {

    private static final int MIN_TARGET_LIST_ENTRIES = 0;
    private static final int MAX_TARGET_LIST_ENTRIES = 7;

    @Override
    public Class<Target> getTypeClass() {
        return Target.class;
    }

    @Override
    public UnsignedLong getDescriptorCode() {
        return Target.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return Target.DESCRIPTOR_SYMBOL;
    }

    @Override
    public Target readValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        return readTarget(buffer, state, checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder));
    }

    @Override
    public Target[] readArrayElements(ProtonBuffer buffer, DecoderState state, int count) throws DecodeException {
        final TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        final Target[] result = new Target[count];
        for (int i = 0; i < count; ++i) {
            result[i] = readTarget(buffer, state, checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder));
        }

        return result;
    }

    @Override
    public void skipValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        decoder.skipValue(buffer, state);
    }

    private Target readTarget(ProtonBuffer buffer, DecoderState state, ListTypeDecoder listDecoder) throws DecodeException {
        final Target target = new Target();

        @SuppressWarnings("unused")
        final int size = listDecoder.readSize(buffer);
        final int count = listDecoder.readCount(buffer);

        if (count < MIN_TARGET_LIST_ENTRIES) {
            throw new DecodeException("Not enough entries in Target list encoding: " + count);
        }

        if (count > MAX_TARGET_LIST_ENTRIES) {
            throw new DecodeException("To many entries in Target list encoding: " + count);
        }

        for (int index = 0; index < count; ++index) {
            switch (index) {
                case 0:
                    target.setAddress(state.getDecoder().readString(buffer, state));
                    break;
                case 1:
                    final long durability = state.getDecoder().readUnsignedInteger(buffer, state, 0);
                    target.setDurable(TerminusDurability.valueOf(durability));
                    break;
                case 2:
                    final Symbol expiryPolicy = state.getDecoder().readSymbol(buffer, state);
                    target.setExpiryPolicy(expiryPolicy == null ? TerminusExpiryPolicy.SESSION_END : TerminusExpiryPolicy.valueOf(expiryPolicy));
                    break;
                case 3:
                    final UnsignedInteger timeout = state.getDecoder().readUnsignedInteger(buffer, state);
                    target.setTimeout(timeout == null ? UnsignedInteger.ZERO : timeout);
                    break;
                case 4:
                    target.setDynamic(state.getDecoder().readBoolean(buffer, state, false));
                    break;
                case 5:
                    target.setDynamicNodeProperties(state.getDecoder().readMap(buffer, state));
                    break;
                case 6:
                    target.setCapabilities(state.getDecoder().readMultiple(buffer, state, Symbol.class));
                    break;
            }
        }

        return target;
    }

    @Override
    public Target readValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        final StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        return readTarget(stream, state, checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder));
    }

    @Override
    public Target[] readArrayElements(InputStream stream, StreamDecoderState state, int count) throws DecodeException {
        final StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        final Target[] result = new Target[count];
        for (int i = 0; i < count; ++i) {
            result[i] = readTarget(stream, state, checkIsExpectedTypeAndCast(ListTypeDecoder.class, decoder));
        }

        return result;
    }

    @Override
    public void skipValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        final StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        decoder.skipValue(stream, state);
    }

    private Target readTarget(InputStream stream, StreamDecoderState state, ListTypeDecoder listDecoder) throws DecodeException {
        final Target target = new Target();

        @SuppressWarnings("unused")
        final int size = listDecoder.readSize(stream);
        final int count = listDecoder.readCount(stream);

        if (count < MIN_TARGET_LIST_ENTRIES) {
            throw new DecodeException("Not enough entries in Target list encoding: " + count);
        }

        if (count > MAX_TARGET_LIST_ENTRIES) {
            throw new DecodeException("To many entries in Target list encoding: " + count);
        }

        for (int index = 0; index < count; ++index) {
            switch (index) {
                case 0:
                    target.setAddress(state.getDecoder().readString(stream, state));
                    break;
                case 1:
                    final long durability = state.getDecoder().readUnsignedInteger(stream, state, 0);
                    target.setDurable(TerminusDurability.valueOf(durability));
                    break;
                case 2:
                    final Symbol expiryPolicy = state.getDecoder().readSymbol(stream, state);
                    target.setExpiryPolicy(expiryPolicy == null ? TerminusExpiryPolicy.SESSION_END : TerminusExpiryPolicy.valueOf(expiryPolicy));
                    break;
                case 3:
                    final UnsignedInteger timeout = state.getDecoder().readUnsignedInteger(stream, state);
                    target.setTimeout(timeout == null ? UnsignedInteger.ZERO : timeout);
                    break;
                case 4:
                    target.setDynamic(state.getDecoder().readBoolean(stream, state, false));
                    break;
                case 5:
                    target.setDynamicNodeProperties(state.getDecoder().readMap(stream, state));
                    break;
                case 6:
                    target.setCapabilities(state.getDecoder().readMultiple(stream, state, Symbol.class));
                    break;
            }
        }

        return target;
    }
}
