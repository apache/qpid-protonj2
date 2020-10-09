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
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.messaging.DeleteOnNoLinksOrMessages;

/**
 * Decoder of AMQP DeleteOnNoLinksOrMessages type values from a byte stream
 */
public final class DeleteOnNoLinksOrMessagesTypeDecoder extends AbstractDescribedTypeDecoder<DeleteOnNoLinksOrMessages> {

    @Override
    public Class<DeleteOnNoLinksOrMessages> getTypeClass() {
        return DeleteOnNoLinksOrMessages.class;
    }

    @Override
    public UnsignedLong getDescriptorCode() {
        return DeleteOnNoLinksOrMessages.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return DeleteOnNoLinksOrMessages.DESCRIPTOR_SYMBOL;
    }

    @Override
    public DeleteOnNoLinksOrMessages readValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        decoder.skipValue(buffer, state);

        return DeleteOnNoLinksOrMessages.getInstance();
    }

    @Override
    public DeleteOnNoLinksOrMessages[] readArrayElements(ProtonBuffer buffer, DecoderState state, int count) throws DecodeException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        DeleteOnNoLinksOrMessages[] result = new DeleteOnNoLinksOrMessages[count];

        for (int i = 0; i < count; ++i) {
            decoder.skipValue(buffer, state);
            result[i] = DeleteOnNoLinksOrMessages.getInstance();
        }

        return result;
    }

    @Override
    public void skipValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        decoder.skipValue(buffer, state);
    }

    @Override
    public DeleteOnNoLinksOrMessages readValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        decoder.skipValue(stream, state);

        return DeleteOnNoLinksOrMessages.getInstance();
    }

    @Override
    public DeleteOnNoLinksOrMessages[] readArrayElements(InputStream stream, StreamDecoderState state, int count) throws DecodeException {
        StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        DeleteOnNoLinksOrMessages[] result = new DeleteOnNoLinksOrMessages[count];

        for (int i = 0; i < count; ++i) {
            decoder.skipValue(stream, state);
            result[i] = DeleteOnNoLinksOrMessages.getInstance();
        }

        return result;
    }

    @Override
    public void skipValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        checkIsExpectedType(ListTypeDecoder.class, decoder);

        decoder.skipValue(stream, state);
    }
}
