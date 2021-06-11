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
package org.apache.qpid.protonj2.codec.encoders.primitives;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.codec.EncoderState;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.encoders.AbstractPrimitiveTypeEncoder;
import org.apache.qpid.protonj2.types.Binary;

/**
 * Encoder of AMQP Binary type values to a byte stream.
 */
public final class BinaryTypeEncoder extends AbstractPrimitiveTypeEncoder<Binary> {

    @Override
    public Class<Binary> getTypeClass() {
        return Binary.class;
    }

    @Override
    public void writeType(ProtonBuffer buffer, EncoderState state, Binary value) {
        writeType(buffer, state, value.asProtonBuffer());
    }

    /**
     * Shortcut API that allows a {@link ProtonBuffer} to be directly encoded as an AMQP Binary
     * type without the need to create a {@link Binary} instance.  The encoder will attempt
     * to write the smallest encoding possible based on the buffer size.
     *
     * @param buffer
     * 		The {@link ProtonBuffer} instance to write the encoding to.
     * @param state
     * 		The {@link EncoderState} for use in encoding operations.
     * @param value
     * 		The {@link ProtonBuffer} instance that is to be encoded.
     */
    public void writeType(ProtonBuffer buffer, EncoderState state, ProtonBuffer value) {
        if (value.getReadableBytes() > 255) {
            buffer.writeByte(EncodingCodes.VBIN32);
            buffer.writeInt(value.getReadableBytes());
        } else {
            buffer.writeByte(EncodingCodes.VBIN8);
            buffer.writeByte((byte) value.getReadableBytes());
        }

        if (value.hasArray()) {
            buffer.writeBytes(value.getArray(), value.getArrayOffset() + value.getReadIndex(), value.getReadableBytes());
        } else {
            buffer.writeBytes(value, value.getReadIndex(), value.getReadableBytes());
        }
    }

    /**
     * Shortcut API that allows a <code>byte[]</code> to be directly encoded as an AMQP Binary
     * type without the need to create a {@link Binary} instance.  The encoder will attempt
     * to write the smallest encoding possible based on the buffer size.
     *
     * @param buffer
     * 		The {@link ProtonBuffer} instance to write the encoding to.
     * @param state
     * 		The {@link EncoderState} for use in encoding operations.
     * @param value
     * 		The <code>byte[]</code> instance that is to be encoded.
     */
    public void writeType(ProtonBuffer buffer, EncoderState state, byte[] value) {
        if (value.length > 255) {
            buffer.writeByte(EncodingCodes.VBIN32);
            buffer.writeInt(value.length);
            buffer.writeBytes(value, 0, value.length);
        } else {
            buffer.writeByte(EncodingCodes.VBIN8);
            buffer.writeByte((byte) value.length);
            buffer.writeBytes(value, 0, value.length);
        }
    }

    @Override
    public void writeRawArray(ProtonBuffer buffer, EncoderState state, Object[] values) {
        buffer.writeByte(EncodingCodes.VBIN32);
        for (Object value : values) {
            Binary binary = (Binary) value;
            ProtonBuffer binaryBuffer = binary.asProtonBuffer();

            buffer.writeInt(binaryBuffer.getReadableBytes());
            binaryBuffer.markReadIndex();
            try {
                buffer.writeBytes(binaryBuffer);
            } finally {
                binaryBuffer.resetReadIndex();
            }
        }
    }
}
