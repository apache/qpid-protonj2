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

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.codec.DecoderState;
import org.apache.qpid.proton4j.codec.decoders.AbstractPrimitiveTypeDecoder;

/**
 * Base class for the various Binary type decoders used to read AMQP Binary values.
 */
public abstract class AbstractBinaryTypeDecoder extends AbstractPrimitiveTypeDecoder<Binary> implements BinaryTypeDecoder {

    @Override
    public Binary readValue(ProtonBuffer buffer, DecoderState state) {
        return new Binary(readValueAsBuffer(buffer, state));
    }

    public ProtonBuffer readValueAsBuffer(ProtonBuffer buffer, DecoderState state) {
        int length = readSize(buffer);

        if (length > buffer.getReadableBytes()) {
            throw new IllegalArgumentException(
                String.format("Binary data size %d is specified to be greater than the amount " +
                              "of data available (%d)", length, buffer.getReadableBytes()));
        }

        ProtonBuffer payload = ProtonByteBufferAllocator.DEFAULT.allocate(length, length);

        buffer.readBytes(payload);

        return payload;
    }

    public byte[] readValueAsArray(ProtonBuffer buffer, DecoderState state) {
        int length = readSize(buffer);

        if (length > buffer.getReadableBytes()) {
            throw new IllegalArgumentException(
                String.format("Binary data size %d is specified to be greater than the amount " +
                              "of data available (%d)", length, buffer.getReadableBytes()));
        }

        byte[] payload = new byte[length];

        buffer.readBytes(payload);

        return payload;
    }

    @Override
    public void skipValue(ProtonBuffer buffer, DecoderState state) throws IOException {
        int length = readSize(buffer);

        if (length > buffer.getReadableBytes()) {
            throw new IllegalArgumentException(
                String.format("Binary data size %d is specified to be greater than the amount " +
                              "of data available (%d)", length, buffer.getReadableBytes()));
        }

        buffer.skipBytes(length);
    }

    protected abstract int readSize(ProtonBuffer buffer);

}
