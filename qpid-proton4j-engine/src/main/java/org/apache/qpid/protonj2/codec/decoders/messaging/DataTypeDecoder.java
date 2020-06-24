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

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.DecoderState;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.AbstractDescribedTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.BinaryTypeDecoder;
import org.apache.qpid.protonj2.types.Binary;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.messaging.Data;

/**
 * Decoder of AMQP Data type values from a byte stream.
 */
public final class DataTypeDecoder extends AbstractDescribedTypeDecoder<Data> {

    private static final Data EMPTY_DATA = new Data(null);

    @Override
    public Class<Data> getTypeClass() {
        return Data.class;
    }

    @Override
    public UnsignedLong getDescriptorCode() {
        return Data.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return Data.DESCRIPTOR_SYMBOL;
    }

    @Override
    public Data readValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final byte encodingCode = buffer.readByte();
        final int size;

        switch (encodingCode) {
            case EncodingCodes.VBIN8:
                size = buffer.readByte() & 0xFF;
                break;
            case EncodingCodes.VBIN32:
                size = buffer.readInt();
                break;
            case EncodingCodes.NULL:
                return EMPTY_DATA;
            default:
                throw new DecodeException("Expected Binary type but found encoding: " + encodingCode);
        }

        if (size > buffer.getReadableBytes()) {
            throw new DecodeException("Binary data size " + size + " is specified to be greater than the " +
                                      "amount of data available ("+ buffer.getReadableBytes()+")");
        }

        final int position = buffer.getReadIndex();
        final ProtonBuffer data = ProtonByteBufferAllocator.DEFAULT.allocate(size, size);

        if (data.hasArray()) {
            buffer.getBytes(position, data.getArray(), data.getArrayOffset(), size);
        } else {
            buffer.getBytes(position, data, 0, size);
        }

        data.setWriteIndex(size);
        buffer.setReadIndex(position + size);

        return new Data(new Binary(data));
    }

    @Override
    public Data[] readArrayElements(ProtonBuffer buffer, DecoderState state, int count) throws DecodeException {
        final TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        checkIsExpectedType(BinaryTypeDecoder.class, decoder);

        final BinaryTypeDecoder valueDecoder = (BinaryTypeDecoder) decoder;
        final Binary[] binaryArray = valueDecoder.readArrayElements(buffer, state, count);

        final Data[] dataArray = new Data[count];
        for (int i = 0; i < count; ++i) {
            dataArray[i] = new Data(binaryArray[i]);
        }

        return dataArray;
    }

    @Override
    public void skipValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        checkIsExpectedType(BinaryTypeDecoder.class, decoder);

        decoder.skipValue(buffer, state);
    }
}
